import json
import logging
import tarfile
import tempfile
import time
import traceback
from io import BytesIO

import docker
from docker import types
from python_logging_rabbitmq import RabbitMQHandler

from DicomFlowLib.data_structures.contexts.data_context import FlowContext
from DicomFlowLib.data_structures.flow import Model
from DicomFlowLib.default_config import LOG_FORMAT
from DicomFlowLib.fs import FileStorage
from DicomFlowLib.mq import MQBase


class DockerConsumer:
    def __init__(self,
                 logger: RabbitMQHandler,
                 file_storage: FileStorage,
                 pub_exchange: str,
                 pub_routing_key: str,
                 pub_routing_key_as_queue: bool,
                 pub_exchange_type: str,
                 gpu: str | bool = False,
                 log_level: int = 10):
        self.fs = file_storage
        self.pub_exchange_type = pub_exchange_type
        self.pub_routing_key = pub_routing_key
        self.pub_routing_key_as_queue = pub_routing_key_as_queue
        self.pub_exchange = pub_exchange
        logging.basicConfig(level=log_level, format=LOG_FORMAT)
        self.LOGGER = logging.getLogger(__name__)
        self.LOGGER.addHandler(logger)
        self.gpu = gpu
        self.cli = docker.from_env()

    def __del__(self):
        self.cli.close()

    def mq_entrypoint(self, connection, channel, basic_deliver, properties, body):
        mq = MQBase().connect_like(connection=connection)

        context = FlowContext(**json.loads(body.decode()))

        input_tar = self.fs.get(context.input_file_uid)
        output_tar = self.exec_model(context.flow.model, input_tar)

        context.output_file_uid = self.fs.put(output_tar)

        mq.setup_exchange(exchange=self.pub_exchange, exchange_type=self.pub_exchange_type)
        mq.setup_queue_and_bind(exchange=self.pub_exchange,
                                routing_key=self.pub_routing_key,
                                routing_key_as_queue=self.pub_routing_key_as_queue)
        mq.basic_publish(exchange=self.pub_exchange,
                         routing_key=self.pub_routing_key,
                         body=context.model_dump_json().encode())
        self.LOGGER.debug(context)

    def exec_model(self,
                   model: Model,
                   input_tar: BytesIO | None = None) -> BytesIO:
        kwargs = model.docker_kwargs
        self.LOGGER.info(str(kwargs))
        # Mount point of input/output to container. These are dummy binds, to make sure the container has /input and /output
        # container.
        kwargs["volumes"] = [
            f"{tempfile.mkdtemp()}:{model.input_dir}:rw",  # is there a better way
            f"{tempfile.mkdtemp()}:{model.output_dir}:rw"
        ]

        # Allow GPU usage. If int, use as count, if str use as uuid
        if self.gpu:
            if isinstance(self.gpu, str):
                kwargs["device_requests"] = [types.DeviceRequest(device_ids=[self.gpu], capabilities=[['gpu']])]
            else:
                kwargs["device_requests"] = [types.DeviceRequest(count=-1, capabilities=[['gpu']])]

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
            self.LOGGER.info("####### CONTAINER LOG ##########")
            self.LOGGER.info(container.logs().decode())

            if model.output_dir:
                output, stats = container.get_archive(path=model.output_dir)

                # Write to a format I understand
                output_tar = BytesIO()
                for chunk in output:
                    output_tar.write(chunk)
                output_tar.seek(0)
                return output_tar
                # # Remove one dir level
                # # output_tar = self.postprocess_output_tar(tarf=output_tar)
                #
                # return output_tar

        except Exception as e:
            self.LOGGER.error(str(traceback.format_exc()))
            raise e
        finally:
            counter = 0
            while counter < 5:
                try:
                    self.LOGGER.info("Attempting to remove %s", container.short_id)
                    container.remove()
                    break
                except:
                    self.LOGGER.info("Failed to remove %s. Trying again in 5 sec...", container.short_id)
                    counter += 1
                    time.sleep(5)

    # def postprocess_output_tar(self, tarf: BytesIO):
    #     """
    #     Removes one layer from the output_tar - i.e. the /output/
    #     If dump_logs==True, container logs are dumped to container.log
    #     """
    #     container_tar_obj = tarfile.TarFile.open(fileobj=tarf, mode="r|*")
    #
    #     # Unwrap container tar to temp dir
    #     with tempfile.TemporaryDirectory() as tmpd:
    #         container_tar_obj.extractall(tmpd)
    #         container_tar_obj.close()
    #
    #         # Make a new temp tar.gz
    #         new_tar_file = BytesIO()
    #         new_tar_obj = tarfile.TarFile.open(fileobj=new_tar_file, mode="w")
    #
    #         # Walk directory from output to strip it away
    #         for fol, subs, files in os.walk(os.path.join(tmpd, "output")):
    #             for file in files:
    #                 new_tar_obj.add(os.path.join(fol, file), arcname=file)
    #
    #     new_tar_obj.close()  # Now safe to close tar obj
    #     new_tar_file.seek(0)  # Reset pointer
    #     return new_tar_file  # Ready to ship directly to DB


if __name__ == "__main__":
    model = Model(dockerKwargs={"image": "busybox",
                                "command": "cp -r /input/* /output/"})
    try:
        input_tar = BytesIO()
        tf = tarfile.TarFile.open(fileobj=input_tar, mode="w")
        tf.add(".")
        tf.close()
        input_tar.seek(0)

        consumer = DockerConsumer()
        output_tar = consumer.exec_model(model=model, input_tar=input_tar)
    finally:
        input_tar.close()
        output_tar.close()
