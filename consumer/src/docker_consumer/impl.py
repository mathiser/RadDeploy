import json
import tempfile
import time
from io import BytesIO
from typing import List, Iterable

import docker
from docker import types

from DicomFlowLib.data_structures.contexts import FlowContext
from DicomFlowLib.data_structures.flow import Model
from DicomFlowLib.data_structures.mq import MQEntrypointResult
from DicomFlowLib.fs import FileStorage
from DicomFlowLib.log import CollectiveLogger


class DockerConsumer:
    def __init__(self,
                 logger: CollectiveLogger,
                 file_storage: FileStorage,
                 gpus: str | List | None):
        self.pub_declared = False
        self.logger = logger
        self.fs = file_storage

        if gpus and isinstance(gpus, str):
            self.gpus = gpus.split()
        else:
            self.gpus = gpus
        self.cli = docker.from_env()

    def __del__(self):
        self.cli.close()

    def mq_entrypoint(self, basic_deliver, body) -> Iterable[MQEntrypointResult]:
        context = FlowContext(**json.loads(body.decode()))
        self.uid = context.flow_instance_uid

        self.logger.info(f"RUNNING FLOW", uid=self.uid, finished=False)
        tar = self.fs.get(context.input_file_uid)
        for model in context.flow.models:
            tar = self.exec_model(model, tar)

        context.output_file_uid = self.fs.put(tar)
        self.logger.info(f"RUNNING FLOW", uid=self.uid, finished=True)

        self.uid = None

        return [MQEntrypointResult(body=context.model_dump_json().encode())]

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
        # Mount static_uid to static_folder if they are set
        if model.static_mount:
            kwargs["volumes"].append(f"{model.static_mount}")

        # Allow GPU usage. If int, use as count, if str use as uuid
        if self.gpus:
            kwargs["ipc_mode"] = "host"
            kwargs["device_requests"] = [types.DeviceRequest(device_ids=self.gpus, capabilities=[['gpu']])]

        try:
            # Create container
            container = self.cli.containers.create(**kwargs)
            # Reload new params
            container.reload()

            if model.input_dir:
                # Add the input tar to provided input
                container.put_archive(model.input_dir, input_tar)

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
                return output_tar

        except Exception as e:
            self.logger.error(e)
            raise e
        finally:
            counter = 0
            while counter < 5:
                try:
                    self.logger.debug(f"Attempting to remove container {container.short_id}",
                                      uid=self.uid,
                                      finished=False)
                    container.remove(force=True)
                    self.logger.debug(f"Attempting to remove container {container.short_id}",
                                      uid=self.uid,
                                      finished=True)
                    break
                except:
                    self.logger.debug(f"Failed to remove {container.short_id}. Trying again in 5 sec...",
                                      uid=self.uid,
                                      finished=True)
                    counter += 1
                    time.sleep(5)
