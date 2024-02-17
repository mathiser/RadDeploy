import json
import logging
import os
import shutil
import sys
import tarfile
import tempfile
import threading
import time
from io import BytesIO
from typing import Iterable, Dict

import docker
import yaml
from docker import types, errors

from DicomFlowLib.data_structures.contexts import PublishContext, FlowContext
from DicomFlowLib.fs import FileStorageClient
from .worker import  Worker


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
                 worker: Worker,
                 pub_routing_key_success: str,
                 pub_routing_key_fail: str,
                 log_level: int = 20):
        self.pub_declared = False
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(log_level)
        self.fs = file_storage
        self.ss = static_storage
        self.pub_routing_key_success = pub_routing_key_success
        self.pub_routing_key_fail = pub_routing_key_fail

        self.worker = worker

        self.cli = docker.from_env()

    def __del__(self):
        self.cli.close()

    def mq_entrypoint(self, basic_deliver, body) -> Iterable[PublishContext]:
        fc = FlowContext(**json.loads(body.decode()))

        self.logger.info(f"Spinning up flow")
        fc = self.exec_model(flow_context=fc)
        self.logger.info(f"Finished flow")

        return [PublishContext(body=fc.model_dump_json().encode(), routing_key=self.pub_routing_key_success)]

    def exec_model(self,
                   flow_context: FlowContext) -> FlowContext:
        uid = flow_context.uid
        model = flow_context.active_model
        mount_mapping = flow_context.mount_mapping

        if model.pull_before_exec:
            try:
                self.cli.images.pull(*model.docker_kwargs["image"].split(":"))
            except Exception as e:
                self.logger.error("Could not pull container - does it exist on docker hub?")
                raise e
        self.logger.info(f"UID={uid} ; RUNNING CONTAINER TAG: {model.docker_kwargs["image"]}")

        kwargs = model.docker_kwargs

        # generate dummy binds for all mounts
        kwargs["volumes"] = []
        kwargs["volumes"].append(f"{tempfile.mkdtemp()}:{model.config_dump_folder}")  # Folder for config. Default is /config.yaml
        for src, dst in {**model.input_mounts, **model.output_mounts, **model.static_mounts}.items():
            kwargs["volumes"].append(f'{tempfile.mkdtemp()}:{dst}')

        # Allow GPU usage
        if self.worker.is_gpu_worker():
            kwargs["ipc_mode"] = "host"
            self.logger.debug(f"Uses GPU device {self.worker.device_id}")
            if "DEBUG_DISABLE_GPU" not in flow_context.flow.extra.keys():
                kwargs["device_requests"] = [types.DeviceRequest(device_ids=[self.worker.device_id], capabilities=[['gpu']])]

        # So logs can be pulled
        kwargs["detach"] = True

        # Create container
        container = self.cli.containers.create(**kwargs)

        try:
            # Reload new params
            container.reload()

            # Dumps config.yaml in folder
            if model.config_dump_folder:
                config_file = self.config_to_tar(model.config)
                container.put_archive(model.config_dump_folder, config_file.read())
                config_file.close()

            # Mount inputs
            for uid, dst in model.remapped_input_mount_keys(mount_mapping).items():  # remap to use actual uid from the file_storage
                container.put_archive(dst, self.fs.get(uid))

            # Mount statics
            for uid, dst in model.static_mounts.items():
                container.put_archive(dst, self.ss.get(uid))  # Get static from static storage

            # Run the container
            container.start()
            for line in container.logs(stream=True):
                self.logger.debug(f"UID={uid} ; CONTAINER_ID={container.short_id} ; {line.decode()}")

            result = container.wait(timeout=model.timeout)  # Blocks...

            self.assert_container_status(uid=uid, container=container, result=result)

            for src, dst in model.output_mounts.items():
                tar = self.get_archive(container=container, path=dst)
                uid = self.fs.post(tar)
                mount_mapping[src] = uid

            return flow_context

        except Exception as e:
            self.logger.error(str(e))
            raise e
        finally:
            self.clean_up(container, kwargs)

    def assert_container_status(self, uid, container, result):
        self.logger.info(f"UID={uid} ; CONTAINER_ID={container.short_id} ; EXITCODE={result['StatusCode']}")
        if result["StatusCode"] != 0:
            self.logger.error(f"UID={uid} ; CONTAINER_ID={container.short_id} ; Flow execution failed")
            raise Exception(f"UID={uid} ; CONTAINER_ID={container.short_id} ; Flow did not exit with code 0.")
        else:
            self.logger.info(f"UID={uid} ; CONTAINER_ID={container.short_id} ; Flow execution succeeded")

    def clean_up(self, container, kwargs):
        threading.Thread(target=self.delete_container, args=(container.short_id, )).start()
        threading.Thread(target=self.remove_temp_dirs, args=(kwargs["volumes"], )).start()

    def delete_container(self, container_id):
        counter = 0
        while counter < 5:
            try:
                self.logger.debug(f"Attempting to remove container {container_id}")
                c = self.cli.containers.get(container_id)
                c.remove(force=True)
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

    def config_to_tar(self, config_dict: Dict):
        info = tarfile.TarInfo(name="config.yaml")
        dump = BytesIO(yaml.dump(config_dict).encode())
        dump.seek(0, 2)
        info.size = dump.tell()
        dump.seek(0)

        file = BytesIO()
        with tarfile.TarFile.open(fileobj=file, mode="w") as tf:
            tf.addfile(info, dump)
            dump.close()

        file.seek(0)
        return file