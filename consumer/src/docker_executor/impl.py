import enum
import json
import logging
import os
import shutil
import tarfile
import tempfile
import threading
import time
from io import BytesIO
from typing import Iterable, Dict

import docker
import yaml
from docker import types, errors

from DicomFlowLib.data_structures.flow import Model
from DicomFlowLib.data_structures.service_contexts.models import JobContext
from DicomFlowLib.fs.client.interface import FileStorageClientInterface
from DicomFlowLib.log import init_logger
from DicomFlowLib.mq import PublishContext


class DockerExecutor(threading.Thread):
    def __init__(self,
                 file_storage: FileStorageClientInterface,
                 static_storage: FileStorageClientInterface,
                 worker_device_id: str,
                 job_log_dir: str,
                 worker_type: str = "CPU",
                 log_level: int = 20):
        super().__init__()
        self.pub_declared = False
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(log_level)
        self.job_log_dir = job_log_dir
        self.fs = file_storage
        self.ss = static_storage

        self.worker_type = worker_type
        self.worker_device_id = worker_device_id

        self.cli = docker.from_env()

    def __del__(self):
        self.cli.close()

    def mq_entrypoint(self, basic_deliver, body) -> Iterable[PublishContext]:
        pending_job_context = JobContext(**json.loads(body.decode()))

        self.logger.info(f"Spinning up flow")
        output_mount_mapping = self.exec_model(pending_job_context.model,
                                               pending_job_context.input_mount_mapping,
                                               pending_job_context.uid)
        self.logger.info(f"Finished flow")

        # Overwrite with output mount mapping
        finished_job_context = JobContext(**pending_job_context.model_dump(),
                                              output_mount_mapping=output_mount_mapping)

        # Return new
        return [PublishContext(body=finished_job_context.model_dump_json().encode(),
                               pub_model_routing_key="SUCCESS")]

    def exec_model(self,
                   model: Model,
                   input_mount_mapping: Dict,
                   correlation_id: int,
                   flow_uid: str,
                   ) -> Dict:

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
        kwargs["volumes"].append(
            f"{tempfile.mkdtemp()}:{os.path.dirname(model.config_path)}"  # Folder for config.
        )  # Default is /config/config.yaml

        for src, dst in {**model.input_mounts, **model.output_mounts, **model.static_mounts}.items():
            kwargs["volumes"].append(f'{tempfile.mkdtemp()}:{dst}')

        # Allow GPU usage
        if self.worker_type == "GPU":
            kwargs["ipc_mode"] = "host"
            self.logger.debug(f"Uses GPU device {self.worker_device_id}")
            if "DEBUG_DISABLE_GPU" not in model.config.keys():
                kwargs["device_requests"] = [
                    types.DeviceRequest(device_ids=[self.worker_device_id], capabilities=[['gpu']])]

        # So logs can be pulled
        kwargs["detach"] = True

        # Add docker tag to environment to allow traceability
        if "environment" not in kwargs.keys():
            kwargs["environment"] = {}

        # kwargs["environment"]["FLOW_UID"] = model.uid
        kwargs["environment"]["CONTAINER_TAG"] = model.docker_kwargs["image"]
        # To do: Insert hash of flow definition

        # Create container
        container = self.cli.containers.create(**kwargs)

        try:
            # Reload new envionment
            # container.reload()

            # Dumps config.yaml in folder
            with self.config_to_tar(model.config, model.config_path) as config_file:
                container.put_archive("/", config_file.read())  # Dump in root. Should end up the right place

            # Mount inputs
            # Remap to use actual uid from the file_storage
            for mappings in model.remap_input_mount_keys(input_mount_mapping):
                container.put_archive(mappings["dst"], self.fs.get(mappings["uid"]))

            # Mount statics
            for uid, dst in model.static_mounts.items():
                container.put_archive(dst, self.ss.get(uid))  # Get static from static storage

            # Run the container
            container.start()

            # Grab the output and log to job specific logger
            job_logger_name = f"{flow_uid}_{correlation_id}_{kwargs["image"].replace("/", "-")}"
            threading.Thread(target=self.catch_logs_from_container, args=(job_logger_name, container)).start()

            # Wait for job to execute - with timeout. If not finished, it will be killed.
            result = container.wait(timeout=model.timeout)  # Blocks...

            # If status code != 0,
            self.assert_container_status(uid=flow_uid, container=container, result=result)

            output_mapping = {}
            for src, dst in model.output_mounts.items():
                tar = self.get_archive(container=container, path=dst)
                uid = self.fs.post(tar)
                output_mapping[src] = uid

            return output_mapping

        except Exception as e:
            self.logger.error(str(e))
            raise e
        finally:
            self.clean_up(container, kwargs)

    def catch_logs_from_container(self, logger_name, container, job_log_dir):

        container_logger = init_logger(name=logger_name,
                                       log_dir=job_log_dir)
        container_logger.setLevel(10)

        for line in container.logs(stream=True):
            container_logger.info(line.decode().strip("\n"))

    def assert_container_status(self, uid, container, result):
        if result["StatusCode"] != 0:
            self.logger.error(f"UID={uid} ; CONTAINER_ID={container.short_id} ; Flow execution failed")
            raise Exception(
                f"UID={uid} ; CONTAINER_ID={container.short_id} ; Flow exited with status code: {str(result["StatusCode"])}.")
        else:
            self.logger.info(f"UID={uid} ; CONTAINER_ID={container.short_id} ; Flow execution succeeded")

    def clean_up(self, container, kwargs):
        threading.Thread(target=self.delete_container, args=(container.short_id,)).start()
        threading.Thread(target=self.remove_temp_dirs, args=(kwargs["volumes"],)).start()

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

    @staticmethod
    def remove_temp_dirs(volumes):
        # Delete dummy directories from mounts. These are empty, so no loss if this fails for some reason.
        for src_dst in volumes:
            src, dst = src_dst.split(":")
            if os.path.exists(src):
                shutil.rmtree(src)

    def get_archive(self, container, path):
        output, stats = container.get_archive(path=path)

        output_tar = BytesIO()  # Write to a format I understand
        for chunk in output:
            output_tar.write(chunk)
        output_tar.seek(0)

        return self.postprocess_output_tar(path, tarf=output_tar)

    @staticmethod
    def config_to_tar(config_dict: Dict, config_path: str):
        info = tarfile.TarInfo(name=config_path)
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

    @staticmethod
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
                container_tar_obj.extractall(tmpd, filter="data")

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
