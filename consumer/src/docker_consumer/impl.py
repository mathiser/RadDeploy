import json
import tempfile
import time
from io import BytesIO
from typing import List

import docker
from docker import types

from DicomFlowLib.data_structures.contexts import PubModel
from DicomFlowLib.data_structures.contexts.data_context import FlowContext
from DicomFlowLib.fs import FileStorage
from DicomFlowLib.log import CollectiveLogger
from DicomFlowLib.mq import MQBase, MQSubEntrypoint


class DockerConsumer(MQSubEntrypoint):
    def __init__(self,
                 logger: CollectiveLogger,
                 file_storage: FileStorage,
                 static_storage: FileStorage,
                 pub_models: List[PubModel],
                 gpus: str | List | None):
        super().__init__(logger, pub_models)
        self.pub_declared = False
        self.logger = logger
        self.fs = file_storage
        self.ss = static_storage
        self.pub_models = pub_models

        if gpus and isinstance(gpus, str):
            self.gpus = gpus.split()
        else:
            self.gpus = gpus
        self.cli = docker.from_env()

    def __del__(self):
        self.cli.close()

    def mq_entrypoint(self, connection, channel, basic_deliver, properties, body):
        mq = MQBase(logger=self.logger, close_conn_on_exit=False).connect_with(connection=connection, channel=channel)

        context = FlowContext(**json.loads(body.decode()))

        self.logger.info(f"PROCESSING", uid=context.uid, finished=False)

        input_tar = self.fs.get(context.input_file_uid)

        output_tar = self.exec_model(context, input_tar)

        self.logger.info(f"RUNNING FLOW", uid=context.uid, finished=False)
        context.output_file_uid = self.fs.put(output_tar)
        self.logger.info(f"RUNNING FLOW", uid=context.uid, finished=True)

        self.publish(mq, context)

        self.logger.info(f"PROCESSING", uid=context.uid, finished=True)

    def exec_model(self,
                   context: FlowContext,
                   input_tar: BytesIO) -> BytesIO:

        model = context.flow.model
        if model.pull_before_exec:
            try:
                self.cli.images.pull(*model.docker_kwargs["image"].split(":"))
            except Exception as e:
                self.logger.error("Could not pull container - does it exist on docker hub?")
                raise e
        self.logger.info(f"RUNNING CONTAINER TAG: {model.docker_kwargs["image"]}", uid=context.uid, finished=False)

        kwargs = model.docker_kwargs
        # Mount point of input/output to container. These are dummy binds, to make sure the container has /input and /output
        # container.
        kwargs["volumes"] = [
            f"{tempfile.mkdtemp()}:{model.input_dir}:rw",  # is there a better way
            f"{tempfile.mkdtemp()}:{model.output_dir}:rw"
        ]

        # Mount static_uid to static_folder if they are set
        # if model.static_uid and model.static_dir:
        #     print(f"{self.ss.get_file_path(model.static_uid)}:{model.static_dir}")
        #     kwargs["volumes"].append(f"{self.ss.get_file_path(model.static_uid)}:{model.static_dir}:ro")
        # print(kwargs["volumes"])
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
            container.wait()  # Blocks...
            self.logger.info("####### CONTAINER LOG ##########")
            self.logger.info(container.logs().decode())

            if model.output_dir:
                output, stats = container.get_archive(path=model.output_dir)

                # Write to a format I understand
                output_tar = BytesIO()
                for chunk in output:
                    output_tar.write(chunk)
                output_tar.seek(0)
                self.logger.info(f"RUNNING CONTAINER TAG: {model.docker_kwargs["image"]}", uid=context.uid,
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
                                      uid=context.uid,
                                      finished=False)
                    container.remove()
                    self.logger.debug(f"Attempting to remove container {container.short_id}",
                                      uid=context.uid,
                                      finished=True)
                    break
                except:
                    self.logger.debug(f"Failed to remove {container.short_id}. Trying again in 5 sec...",
                                      uid=context.uid,
                                      finished=True)
                    counter += 1
                    time.sleep(5)
