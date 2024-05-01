import tarfile
import uuid

from docker_executor import DockerExecutor

from RadDeployLib.test_utils.fixtures import *
from RadDeployLib.test_utils.mock_classes import MockFileStorageClient


@pytest.fixture
def docker_executor(fs, tmpdir):
    return DockerExecutor(log_level=10,
                          file_storage=fs,
                          static_storage=MockFileStorageClient(),
                          job_log_dir=tmpdir,
                          worker_type="CPU",
                          worker_device_id="0"
    )

def test_docker_executor_hello_world(tmpdir, scp_tar, docker_executor, job_context):
    assert "src" in job_context.input_mount_mapping.keys()
    assert docker_executor.fs.exists(job_context.input_mount_mapping["src"])
    finished_job_context = docker_executor.exec_model(job_context)

    assert "dst" in finished_job_context.output_mount_mapping

def test_container_tag_exists(tmpdir, scp_tar, docker_executor, job_context):
    job_context.model = Model(
        docker_kwargs={"image": "busybox",
                       "command": "sh -c 'echo $CONTAINER_TAG; if [[ -z $CONTAINER_TAG ]]; then exit 1; else exit 0; fi'"},
    )

    finished_job_context = docker_executor.exec_model(job_context)
    assert "dst" in finished_job_context.output_mount_mapping

def test_docker_executor_multiple_outputs(scp_tar, docker_executor, job_context):
    job_context.model = Model(
        docker_kwargs={"image": "busybox",
                       "command": "sh -c 'cp -r /input/* /output_0; cp -r /input/* /output_1; cp -r /input/* /output_2'"},
        output_mounts={
            "output_0": "/output_0",
            "output_1": "/output_1",
            "output_2": "/output_2"
        }
    )

    finished_job_context = docker_executor.exec_model(job_context)

    assert "output_0" in finished_job_context.output_mount_mapping
    assert "output_1" in finished_job_context.output_mount_mapping
    assert "output_2" in finished_job_context.output_mount_mapping

    src = docker_executor.fs.get(finished_job_context.input_mount_mapping["src"])
    with tarfile.TarFile.open(fileobj=src) as src_tf:
        src_files = [member.name for member in src_tf.getmembers()]

    for output in ["output_0", "output_1", "output_2"]:
        output = docker_executor.fs.get(finished_job_context.output_mount_mapping[output])
        with tarfile.TarFile.open(fileobj=output) as dst_tf:
            dst_files = [member.name for member in dst_tf.getmembers()]
        assert src_files == dst_files


def test_docker_executor_default_config_loc(tmpdir, scp_tar, docker_executor, job_context):
    job_context.model = Model(
        docker_kwargs={
            "image": "busybox",
            "command": "sh -c 'cp /config/config.yaml /output/'"
        },
        config={
            "HEST": 123
        },
    )

    finished_job_context = docker_executor.exec_model(job_context)
    output = docker_executor.fs.get(finished_job_context.output_mount_mapping["dst"])
    with tarfile.TarFile.open(fileobj=output) as dst_tf:
        assert "HEST: 123" in str(dst_tf.extractfile("config.yaml").read())


def test_docker_executor_new_config_loc(tmpdir, scp_tar, docker_executor, job_context):
    job_context.model = Model(
        docker_kwargs={
            "image": "busybox",
            "command": "sh -c 'cp /opt/fidus/config/config.yaml /output/'"
        },
        config={
            "HEST": 123
        },
        config_path="/opt/fidus/config/config.yaml"
    )

    finished_job_context = docker_executor.exec_model(job_context)
    output = docker_executor.fs.get(finished_job_context.output_mount_mapping["dst"])
    with tarfile.TarFile.open(fileobj=output) as dst_tf:
        assert "HEST: 123" in str(dst_tf.extractfile("config.yaml").read())
