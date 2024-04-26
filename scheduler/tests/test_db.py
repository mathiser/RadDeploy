from DicomFlowLib.test_utils.fixtures import *

from db.db_models import DBFlow, DBJob
from .fixtures import db

def test_add_db_flow(db, dag_flow_context):
    echo = db.add_db_flow(dag_flow_context)
    assert echo.id
    assert echo.flow_context.flow.is_valid_dag()
    assert len(echo.db_jobs) == 0


def test_add_job(db_flow, db):
    echo_db_flow = db.get_by_kwargs(DBFlow, {"id": db_flow.id})[0]
    assert len(echo_db_flow.db_jobs) == 4
    db_job = echo_db_flow.db_jobs[0]
    assert db_job.db_flow_id == db_flow.id
    assert "src" in db_job.input_mount_keys
    assert not db_job.input_mounts_available()

def test_add_mount_mapping(db, dag_flow_context):
    flow = db.add_db_flow(dag_flow_context)
    assert len(flow.mount_mapping.keys()) == 0

    db.add_mount_mapping(flow.id, "src", "VeryUID-UID")

    echo_flow = db.get_by_kwargs(DBFlow, {"id": flow.id}, first=True)
    assert len(echo_flow.mount_mapping.keys()) == 1

    db.add_mount_mapping(flow.id, "src", "VeryUID-UID")

    echo_flow = db.get_by_kwargs(DBFlow, {"id": flow.id}, first=True)
    assert len(echo_flow.mount_mapping.keys()) == 1

    db.add_mount_mapping(flow.id, "src", "VeryUID-UID2")

    echo_flow = db.get_by_kwargs(DBFlow, {"id": flow.id}, first=True)
    assert len(echo_flow.mount_mapping.keys()) == 1


def test_runnable(db_flow, db):
    # Get and check before inserting mount mapping
    echo_db_flow = db.get_by_kwargs(DBFlow, {"id": db_flow.id}, first=True)
    db_job = echo_db_flow.db_jobs[0]
    assert not db_job.input_mounts_available()

    # Now insert and check that "are_required_input_mounts_available" returns True
    db.add_mount_mapping(db_flow_id=db_flow.id,
                         src="src", uid="TestFestHest")
    echo_db_flow = db.get_by_kwargs(DBFlow, {"id": db_flow.id}, first=True)
    db_job = echo_db_flow.db_jobs[0]
    assert "src" in db_job.input_mount_keys
    assert db_job.input_mounts_available()


def test_update_by_kwargs(db_flow, db):
    # Get and check before inserting mount mapping
    job = db.get_by_kwargs(DBJob, {"id": 2}, first=True)
    assert job.retries == 0
    updated_job = db.update_by_kwargs(DBJob,
                                      {"id": 2},
                                      {"retries": 99},
                                      first=True)
    assert updated_job.retries == 99

    # Update all:
    updated_jobs = db.update_by_kwargs(DBJob,
                                       {},
                                       {"retries": 100},
                                       first=False)
    for j in updated_jobs:
        assert j.retries == 100
