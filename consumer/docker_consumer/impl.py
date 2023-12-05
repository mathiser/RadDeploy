import logging
import os
import tarfile
import tempfile
import traceback
from io import BytesIO

import docker
from docker import types

from DicomFlowLib.contexts import Node


class DockerConsumer:
    def __init__(self,
                 gpu: str | int | None = None,
                 log_level: int = 10):
        LOG_FORMAT = '%(levelname)s:%(asctime)s:%(message)s'

        logging.basicConfig(level=log_level, format=LOG_FORMAT)
        self.logger = logging.getLogger(__name__)

        self.gpu = gpu
        self.cli = docker.from_env()

    def exec_node(self,
                  node: Node,
                  input_tar: BytesIO):
        kwargs = node.docker_kwargs

        # Mount point of input/output to container. These are dummy binds, to make sure the container has /input and /output
        # container.
        kwargs["volumes"] = [f"{tempfile.mkdtemp(dir='/tmp/')}:{node.input_dir}:rw",  # is there a better way
                             f"{tempfile.mkdtemp(dir='/tmp/')}:{node.output_dir}:rw"]

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

            # Add the input tar to provided input
            container.put_archive(node.input_dir, input_tar)

            container.start()
            container.wait()  # Blocks...

            # Grab /output
            output, stats = container.get_archive(path=node.output_dir)

            # Write to a format I understand
            output_tar = BytesIO()
            for chunk in output:
                output_tar.write(chunk)
            output_tar.seek(0)

            # Remove one dir level
            output_tar = self.postprocess_output_tar(tarf=output_tar)

            return output_tar

        except Exception as e:
            self.logger.error(str(e))
            self.logger.error(str(traceback.format_exc()))
            raise e
        finally:
            container.remove(force=True)
            container.client.tear_down_connection_and_channel()

    def postprocess_output_tar(self, tarf: BytesIO):
        """
        Removes one layer from the output_tar - i.e. the /output/
        If dump_logs==True, container logs are dumped to container.log
        """
        container_tar_obj = tarfile.TarFile.open(fileobj=tarf, mode="r|*")

        # Unwrap container tar to temp dir
        with tempfile.TemporaryDirectory() as tmpd:
            container_tar_obj.extractall(tmpd)
            container_tar_obj.close()

            # Make a new temp tar.gz
            new_tar_file = BytesIO()
            new_tar_obj = tarfile.TarFile.open(fileobj=new_tar_file, mode="w")

            # Walk directory from output to strip it away
            for fol, subs, files in os.walk(os.path.join(tmpd, "output")):
                for file in files:
                    new_tar_obj.add(os.path.join(fol, file), arcname=file)

        new_tar_obj.close()  # Now safe to close tar obj
        new_tar_file.seek(0)  # Reset pointer
        return new_tar_file  # Ready to ship directly to DB


if __name__ == "__main__":
    node = Node(docker_kwargs={"image": "busybox",
                               "command": "cp -r /input/* /output/"})
    try:
        input_tar = BytesIO()
        tf = tarfile.TarFile.open(fileobj=input_tar, mode="w")
        tf.add(".")
        tf.close()
        input_tar.seek(0)

        consumer = DockerConsumer()
        output_tar = consumer.exec_node(node=node, input_tar=input_tar)
    finally:
        input_tar.close()
        output_tar.close()
