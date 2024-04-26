import tarfile
import uuid

import pytest

from DicomFlowLib.data_structures.flow import Model
from DicomFlowLib.test_utils.fixtures import scp_tar, scp_tar_path, fs
from DicomFlowLib.test_utils.mock_classes import MockFileStorageClient
from consumer.src.docker_executor import DockerExecutor



@pytest.fixture
def docker_executor(fs, tmpdir):
    return DockerExecutor(log_level=10,
                          file_storage=fs,
                          static_storage=MockFileStorageClient(),
                          job_log_dir=tmpdir,
                          worker_type="CPU",
                          worker_device_id="0"
    )

def test_docker_executor_hello_world(tmpdir, scp_tar, docker_executor):
    model = Model(
        docker_kwargs={"image": "hello-world"},
    )
    input_mount_mapping = {
        "src": docker_executor.fs.post(scp_tar)
    }

    output_mount_mapping = docker_executor.exec_model(
        model=model,
        input_mount_mapping=input_mount_mapping,
        correlation_id=0,
        flow_uid=str(uuid.uuid4())
    )
    assert "dst" in output_mount_mapping

def test_container_tag_exists(tmpdir, scp_tar, docker_executor):
    model = Model(
        docker_kwargs={"image": "busybox",
                       "command": "sh -c 'if [[ -z $CONTAINER_TAG ]]; then exit 0; else exit 1; fi'"},
    )
    input_mount_mapping = {
        "src": docker_executor.fs.post(scp_tar)
    }

    output_mount_mapping = docker_executor.exec_model(
        model=model,
        input_mount_mapping=input_mount_mapping,
        correlation_id=0,
        flow_uid=str(uuid.uuid4())
    )
    assert "dst" in output_mount_mapping

def test_docker_executor_multiple_outputs(scp_tar, docker_executor):
    model = Model(
        docker_kwargs={"image": "busybox",
                       "command": "sh -c 'cp -r /input/* /output_0; cp -r /input/* /output_1; cp -r /input/* /output_2'"},
        output_mounts={
            "output_0": "/output_0",
            "output_1": "/output_1",
            "output_2": "/output_2"
        }
    )
    input_mount_mapping = {
        "src": docker_executor.fs.post(scp_tar)
    }

    output_mount_mapping = docker_executor.exec_model(
        model=model,
        input_mount_mapping=input_mount_mapping,
        correlation_id=0,
        flow_uid=str(uuid.uuid4())
    )
    assert "output_0" in output_mount_mapping
    assert "output_1" in output_mount_mapping
    assert "output_2" in output_mount_mapping

    src = docker_executor.fs.get(input_mount_mapping["src"])
    with tarfile.TarFile.open(fileobj=src) as src_tf:
        src_files = [member.name for member in src_tf.getmembers()]

    for output in ["output_0", "output_1", "output_2"]:
        output = docker_executor.fs.get(output_mount_mapping[output])
        with tarfile.TarFile.open(fileobj=output) as dst_tf:
            dst_files = [member.name for member in dst_tf.getmembers()]
        assert src_files == dst_files


def test_docker_executor_default_config_loc(tmpdir, scp_tar, docker_executor):
    model = Model(
        docker_kwargs={
            "image": "busybox",
            "command": "sh -c 'cp /config/config.yaml /output/'"
        },
        config={
            "HEST": 123
        },
    )
    input_mount_mapping = {
        "src": docker_executor.fs.post(scp_tar)
    }
    output_mount_mapping = docker_executor.exec_model(model=model,
                                                      input_mount_mapping=input_mount_mapping,
                                                      correlation_id=0,
                                                      flow_uid=str(uuid.uuid4()))
    output = docker_executor.fs.get(output_mount_mapping["dst"])
    with tarfile.TarFile.open(fileobj=output) as dst_tf:
        assert "HEST: 123" in str(dst_tf.extractfile("config.yaml").read())


def test_docker_executor_new_config_loc(tmpdir, scp_tar, docker_executor):
    model = Model(
        docker_kwargs={
            "image": "busybox",
            "command": "sh -c 'cp /opt/fidus/config/config.yaml /output/'"
        },
        config={
            "HEST": 123
        },
        config_path="/opt/fidus/config/config.yaml"
    )
    input_mount_mapping = {
        "src": docker_executor.fs.post(scp_tar)
    }
    output_mount_mapping = docker_executor.exec_model(model=model,
                                                      input_mount_mapping=input_mount_mapping,
                                                      correlation_id=0,
                                                      flow_uid=str(uuid.uuid4()))
    output = docker_executor.fs.get(output_mount_mapping["dst"])
    with tarfile.TarFile.open(fileobj=output) as dst_tf:
        assert "HEST: 123" in str(dst_tf.extractfile("config.yaml").read())
