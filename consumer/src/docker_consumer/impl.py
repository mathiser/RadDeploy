import json
import logging
import os
import shutil
import tarfile
import tempfile
import threading
import time
from io import BytesIO
from typing import List, Iterable, Dict

import docker
from docker import types, errors

from DicomFlowLib.data_structures.contexts import PublishContext, FlowContext
from DicomFlowLib.data_structures.flow import Model
from DicomFlowLib.fs import FileStorageClient


def postprocess_output_tar(output_dir: str, tarf: BytesIO):
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


class DockerConsumer:
    def __init__(self,
                 file_storage: FileStorageClient,
                 static_storage: FileStorageClient | None,
                 gpus: List | str,
                 pub_routing_key_success: str,
                 pub_routing_key_fail: str):
        self.pub_declared = False
        self.logger = logging.getLogger(__name__)
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
        fc = FlowContext(**json.loads(body.decode()))
        model = fc.active_model

        self.logger.info(f"RUNNING FLOW")

        fc.mount_mapping = self.exec_model(model, fc.mount_mapping)

        self.logger.info(f"RUNNING FLOW")
        return [PublishContext(body=fc.model_dump_json().encode(), routing_key=self.pub_routing_key_success)]

    def exec_model(self,
                   model: Model,
                   mount_mapping: Dict) -> Dict:

        if model.pull_before_exec:
            try:
                self.cli.images.pull(*model.docker_kwargs["image"].split(":"))
            except Exception as e:
                self.logger.error("Could not pull container - does it exist on docker hub?")
                raise e
        self.logger.info(f"RUNNING CONTAINER TAG: {model.docker_kwargs["image"]}")

        kwargs = model.docker_kwargs

        # generate dummy binds for all mounts
        kwargs["volumes"] = []
        for src, dst in {**model.input_mounts, **model.output_mounts, **model.static_mounts}.items():
            kwargs["volumes"].append(f'{tempfile.mkdtemp()}:{dst}')

        # Allow GPU usage. If int, use as count, if str use as uuid
        if len(self.gpus) > 0:
            kwargs["ipc_mode"] = "host"
            kwargs["device_requests"] = [types.DeviceRequest(device_ids=self.gpus, capabilities=[['gpu']])]

        # Create container
        container = self.cli.containers.create(**kwargs)

        try:
            # Reload new params
            container.reload()

            # Mount inputs
            for uid, dst in model.remapped_input_mount_keys(mount_mapping).items():  # remap to use actual uid from the file_storage
                container.put_archive(dst, self.fs.get(uid))

            # Mount statics
            for uid, dst in model.static_mounts.items():
                container.put_archive(dst, self.ss.get(uid))  # Get static from static storage

            # Run the container
            container.start()
            result = container.wait(timeout=model.timeout)  # Blocks...

            self.assert_container_status(container=container, result=result)

            for src, dst in model.output_mounts.items():
                tar = self.get_archive(container=container, path=dst)
                uid = self.fs.post(tar)
                mount_mapping[src] = uid

            return mount_mapping

        except Exception as e:
            self.logger.error(str(e))
            raise e
        finally:
            self.clean_up(container, kwargs)

    def assert_container_status(self, container, result):
        self.logger.info(f"####### CONTAINER LOG ########## STATUS CODE: {result['StatusCode']}")
        self.logger.info(container.logs().decode())

        if result["StatusCode"] != 0:
            self.logger.error("Flow execution failed")
            raise Exception("Flow did not exit with code 0.")
        else:
            self.logger.info("Flow execution succeeded")

    def clean_up(self, container, kwargs):
        threading.Thread(target=self.delete_container, args=(container.short_id, )).start()
        threading.Thread(target=self.remove_temp_dirs, args=(kwargs["volumes"], )).start()

    def delete_container(self, container_id):
        counter = 0
        while counter < 5:
            self.logger.debug(f"Attempting to remove container {container_id}")
            try:
                c = self.cli.containers.get(container_id)
                c.remove(force=True)
                self.logger.debug(f"Attempting to remove container {container_id}")
                return
            except errors.NotFound:
                return
            except Exception as e:
                self.logger.debug(f"Failed to remove {container_id}. Trying again in 5 sec...")
                counter += 1
                time.sleep(5)


    def remove_temp_dirs(self, volumes):
        # Delete dummy directories from mounts. These are empty, so no loss if this fails for some reason.
        for src_dst in volumes:
            src, dst = src_dst.split(":")
            if os.path.exists(src):
                shutil.rmtree(src)

    @staticmethod
    def get_archive(container, path):
        output, stats = container.get_archive(path=path)

        output_tar = BytesIO()  # Write to a format I understand
        for chunk in output:
            output_tar.write(chunk)
        output_tar.seek(0)

        return postprocess_output_tar(path, tarf=output_tar)
