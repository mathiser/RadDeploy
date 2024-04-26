from DicomFlowLib.test_utils.fixtures import *
from .fixtures import *


def test_initiate_flow_in_db(scheduler, dag_flow_context, db, fs):
    f = scheduler.initiate_flow_in_db(dag_flow_context)
    assert len(f.db_jobs) == 4
    assert "src" in f.mount_mapping.keys()
    assert fs.exists(f.mount_mapping["src"])


def test_get_and_update_runnable_jobs_by_kwargs_full_dag(scheduler, dag_flow_context, db, fs):
    f = scheduler.initiate_flow_in_db(dag_flow_context)
    print(f.__dict__)
    jobs = [j for j in scheduler.get_and_update_runnable_jobs_by_kwargs(where_kwargs={}, update_kwargs={},
                                                                        increment_retries=False)]
    assert len(jobs) == 1
    db.add_mount_mapping(db_flow_id=f.id,
                         src="STRUCT",
                         uid="ImportanteUID")
    db.add_mount_mapping(db_flow_id=f.id,
                         src="CT",
                         uid="ImportanteCTUID")
    jobs = [j for j in scheduler.get_and_update_runnable_jobs_by_kwargs(where_kwargs={}, update_kwargs={},
                                                                        increment_retries=False)]
    assert len(jobs) == 2

    db.add_mount_mapping(db_flow_id=f.id,
                         src="STRUCT_POSTPROCESS",
                         uid="ImportanteUID")
    db.add_mount_mapping(db_flow_id=f.id,
                         src="STRUCT_POSTPROCESS1",
                         uid="ImportanteCTUID")

    jobs = [j for j in scheduler.get_and_update_runnable_jobs_by_kwargs(where_kwargs={}, update_kwargs={},
                                                                        increment_retries=False)]
    assert len(jobs) == 1

    db.add_mount_mapping(db_flow_id=f.id,
                         src="dst",
                         uid="ImportanteCTUID")

    jobs = [j for j in scheduler.get_and_update_runnable_jobs_by_kwargs(where_kwargs={}, update_kwargs={},
                                                                        increment_retries=False)]
    assert len(jobs) == 0
