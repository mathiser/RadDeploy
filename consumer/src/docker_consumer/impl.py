import json
import os
import shutil
import tarfile
import tempfile
import time
from io import BytesIO
from typing import List, Iterable

import docker
from docker import types, errors

from DicomFlowLib.data_structures.contexts import FlowContext, PublishContext
from DicomFlowLib.data_structures.flow import Model
from DicomFlowLib.fs import FileStorageClient
from DicomFlowLib.log import CollectiveLogger


class DockerConsumer:
    def __init__(self,
                 logger: CollectiveLogger,
                 file_storage: FileStorageClient,
                 static_storage: FileStorageClient | None,
                 gpus: List | str,
                 pub_routing_key_success: str,
                 pub_routing_key_fail: str):
        self.pub_declared = False
        self.logger = logger
        self.fs = file_storage
        self.ss = static_storage
        self.pub_routing_key_success = pub_routing_key_success
        self.pub_routing_key_fail = pub_routing_key_fail

        if isinstance(gpus, str):
            self.gpus = gpus.split()
        else:
            self.gpus = gpus

        self.cli = docker.from_env()

    def __del__(self):
        self.cli.close()

    def mq_entrypoint(self, basic_deliver, body) -> Iterable[PublishContext]:
        context = FlowContext(**json.loads(body.decode()))
        self.uid = context.flow_instance_uid

        self.logger.info(f"RUNNING FLOW", uid=self.uid, finished=False)
        tar = self.fs.get(context.input_file_uid)
        for model in context.flow.models:
            tar = self.exec_model(model, tar)

        context.output_file_uid = self.fs.post(tar)
        self.logger.info(f"RUNNING FLOW", uid=self.uid, finished=True)
        return [PublishContext(body=context.model_dump_json().encode(), routing_key=self.pub_routing_key_success)]


    def exec_model(self,
                   model: Model,
                   input_tar: BytesIO) -> BytesIO:

        if model.pull_before_exec:
            try:
                self.cli.images.pull(*model.docker_kwargs["image"].split(":"))
            except Exception as e:
                self.logger.error("Could not pull container - does it exist on docker hub?")
                raise e
        self.logger.info(f"RUNNING CONTAINER TAG: {model.docker_kwargs["image"]}", uid=self.uid, finished=False)

        kwargs = model.docker_kwargs
        # Mount point of input/output to container. These are dummy binds, to make sure the container has /input and /output
        # container.

        kwargs["volumes"] = [
            f"{tempfile.mkdtemp()}:{model.input_dir}:rw",  # is there a better way
            f"{tempfile.mkdtemp()}:{model.output_dir}:rw"
        ]
        # Create mountpoints for static dirs:
        for sm in model.static_mounts:
            _, dst = sm.split(":")
            kwargs["volumes"].append(f"{tempfile.mkdtemp()}:{dst}:rw")

        # Allow GPU usage. If int, use as count, if str use as uuid
        if len(self.gpus) > 0:
            kwargs["ipc_mode"] = "host"
            kwargs["device_requests"] = [types.DeviceRequest(device_ids=self.gpus, capabilities=[['gpu']])]

        try:
            # Create container
            container = self.cli.containers.create(**kwargs)

            # Reload new params
            container.reload()

            # Add the input tar to provided input
            if model.input_dir:
                container.put_archive(model.input_dir, input_tar)

            # Mount static_uid to static_folder if they are set
            for sm in model.static_mounts:
                src_uid, dst = sm.split(":")
                static_tar = self.ss.get(src_uid)
                container.put_archive(dst, static_tar)

            container.start()
            result = container.wait(timeout=model.timeout)  # Blocks...

            self.logger.info(f"####### CONTAINER LOG ########## STATUS CODE: {result['StatusCode']}")
            self.logger.info(container.logs().decode())

            if result["StatusCode"] != 0:
                self.logger.error("Flow execution failed")
                raise Exception("Flow did not exit with code 0.")
            else:
                self.logger.info("Flow execution succeeded")

            if model.output_dir:
                output, stats = container.get_archive(path=model.output_dir)

                # Write to a format I understand
                output_tar = BytesIO()
                for chunk in output:
                    output_tar.write(chunk)
                output_tar.seek(0)
                self.logger.info(f"RUNNING CONTAINER TAG: {model.docker_kwargs["image"]}", uid=self.uid,
                                 finished=True)

                output_tar = self.postprocess_output_tar(model.output_dir, tarf=output_tar)
                return output_tar

        except Exception as e:
            self.logger.error(e)
            raise e
        finally:
            # Delete dummy directories from mounts. These are empty, so no loss if this fails for some reason.
            for src_dst_perm in kwargs["volumes"]:
                src, dst, perm = src_dst_perm.split(":")
                if os.path.exists(src):
                    shutil.rmtree(src)

            counter = 0
            while counter < 5:
                self.logger.debug(f"Attempting to remove container {container.short_id}", uid=self.uid, finished=False)
                try:
                    c = self.cli.containers.get(container.short_id)
                    c.remove(force=True)
                    break
                except errors.NotFound:
                    break
                except Exception as e:
                    self.logger.debug(f"Failed to remove {container.short_id}. Trying again in 5 sec...", uid=self.uid, finished=True)
                    counter += 1
                    time.sleep(5)

    def postprocess_output_tar(self, output_dir: str, tarf: BytesIO):
        """
        Removes one layer from the output_tar - i.e. the /output/
        """
        """
        Removes one layer from the output_tar - i.e. the /output/
        If dump_logs==True, container logs are dumped to container.log
        """

        # Unwrap container tar to temp dir
        with tempfile.TemporaryDirectory() as tmpd:
            with tarfile.TarFile.open(fileobj=tarf, mode="r|*") as container_tar_obj:
                container_tar_obj.extractall(tmpd)

            # Make a new temp tar.gz
            new_tar_file = BytesIO()
            with tarfile.TarFile.open(fileobj=new_tar_file, mode="w") as new_tar_obj:
                # Walk directory from output to strip it away the base dir
                to_del = "/" + tmpd.strip("/") + "/" + output_dir.strip("/") + "/"
                for fol, subs, files in os.walk(os.path.join(tmpd)):
                    for file in files:
                        path = os.path.join(fol, file)
                        new_path = path.replace(to_del, "")
                        new_tar_obj.add(path, arcname=new_path)

            new_tar_file.seek(0)  # Reset pointer
            return new_tar_file  # Ready to ship directly to DB

