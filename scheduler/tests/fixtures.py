import os

import pytest

from db import Database

from scheduler.src.orchestration import Scheduler


@pytest.fixture
def db(tmpdir):
    return Database(database_path=os.path.join(tmpdir, "db.db"))


@pytest.fixture
def db_flow(db, dag_flow_context):
    db_flow = db.add_db_flow(dag_flow_context)
    for model in db_flow.flow_context.flow.models:
        j = db.add_job(model=model,
                       db_flow_id=db_flow.id,
                       pub_model_routing_key="CPU",
                       priority=db_flow.flow_context.flow.priority)
        assert j
    return db_flow


@pytest.fixture
def scheduler(db):
    return Scheduler(database=db,
                     log_level=10,
                     reschedule_priority=5,
                     flow_incoming_exchanges=["FLOW_INCOMING"],
                     model_incoming_exchange=["MODEL_INCOMING"])
