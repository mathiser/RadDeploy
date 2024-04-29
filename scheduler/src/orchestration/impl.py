import json
import logging
from typing import Iterable, List, Dict

import sqlalchemy

from RadDeployLib.data_structures.service_contexts import FlowContext
from RadDeployLib.data_structures.service_contexts.models import JobContext, FinishedFlowContext
from RadDeployLib.mq.mq_models import PublishContext
from db import Database, DBFlow, DBJob


class Scheduler:
    def __init__(self,
                 database: Database,
                 flow_incoming_exchanges: List[str],
                 job_incoming_exchanges: List[str],
                 reschedule_priority: int = 5,
                 log_level: int = 20):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(log_level)
        self.db = database

        self.flow_incoming_exchanges = flow_incoming_exchanges
        self.job_incoming_exchanges = job_incoming_exchanges

        self.reschedule_priority = reschedule_priority

    def mq_entrypoint(self, basic_deliver, body) -> Iterable[PublishContext]:
        # Coming from fingerprinter (first time seen)
        self.logger.debug(f"Scheduler received message on {basic_deliver}")
        if basic_deliver.exchange in self.flow_incoming_exchanges:
            flow_context = FlowContext(**json.loads(body.decode()))
            self.logger.debug("Received new flow {}".format(flow_context.model_dump_json()))
            # Add Flow to DB when orchestration sees it for the first time
            # Also adds DBJobs
            self.initiate_flow_in_db(flow_context)

        # Coming from Consumers
        elif basic_deliver.exchange in self.job_incoming_exchanges:
            job_context = JobContext(**json.loads(body.decode()))
            self.logger.debug("Received job from consumers {}".format(job_context.model_dump_json()))

            # In case model executed successfully
            if basic_deliver.routing_key == "success":
                self.db.update_by_kwargs(DBJob,
                                         {"id": job_context.correlation_id},
                                         {"status": 200}),

                # Update DB with output mappings from this model
                self.update_db_mount_mapping_from_job_context_output(job_context=job_context)

            # In case model executed failed
            elif basic_deliver.routing_key in ["fail", "error"]:
                self.db.update_by_kwargs(DBJob,
                                         {"id": job_context.correlation_id},
                                         {"status": 400}),

            # In case model received from unknown lands
            else:
                self.logger.warning("Dunno how to handle things with this routing key {}".format(basic_deliver))

        # If Exchange is undefined for orchestration
        else:
            self.logger.info("Received message from unknown exchange {}".format(basic_deliver))

        jobs = self.main_loop()
        self.logger.info(f"Yielding {jobs}")
        yield from jobs

    def initiate_flow_in_db(self, flow_context: FlowContext) -> DBFlow:
        # Instantiate the DBFlow
        db_flow = self.db.add_db_flow(flow_context=flow_context)

        # Will only have one src to be set
        self.db.add_mount_mapping(db_flow_id=db_flow.id,
                                  src="src",
                                  uid=flow_context.src_uid)

        # Instantiate all models in the DAG of the flow as DBJobs
        for model in db_flow.flow_context.flow.models:
            if "src" in model.input_mount_keys:
                priority = flow_context.flow.priority
            else:
                priority = self.reschedule_priority

            self.db.add_job(
                model=model,
                db_flow_id=db_flow.id,
                priority=priority,
                pub_model_routing_key="GPU" if model.gpu else "CPU"
            )

        # Return updated DBFlow
        return self.db.get_by_kwargs(DBFlow, {"id": db_flow.id}, first=True)

    def update_db_mount_mapping_from_job_context_output(self, job_context: JobContext):
        db_flow_id = self.db.get_by_kwargs(DBJob, {"id": job_context.correlation_id}, first=True).db_flow_id
        for src, uid in job_context.output_mount_mapping.items():
            print(f"Adding mount mapping to flow {job_context.correlation_id}, src: {src}, uid: {uid}")

            try:
                self.db.add_mount_mapping(
                    db_flow_id=db_flow_id,
                    src=src,
                    uid=uid
                )
            except sqlalchemy.exc.IntegrityError:
                self.logger.debug("Mapping exists - passing")
            except Exception as e:
                self.logger.error(str(e))
                raise e

    def get_and_update_runnable_jobs_by_kwargs(self,
                                               where_kwargs: Dict,
                                               update_kwargs: Dict,
                                               increment_retries: bool = True):
        job: DBJob
        for job in self.db.get_by_kwargs(DBJob, where_kwargs=where_kwargs):
            if job.is_runnable():
                if increment_retries:
                    update_kwargs["retries"] = job.retries + 1

                self.db.update_by_kwargs(DBJob,
                                         where_kwargs={"id": job.id},
                                         update_kwargs=update_kwargs)
                job_model = JobContext(
                    correlation_id=job.id,
                    model=job.model,
                    input_mount_mapping=job.db_flow.mount_mapping,
                    output_mount_mapping={}
                )
                yield PublishContext(body=job_model.model_dump_json().encode(),
                                     pub_model_routing_key=job.pub_model_routing_key,
                                     priority=job.priority)

    def get_and_update_unsent_flows_by_kwargs(self,
                                              where_kwargs: Dict,
                                              update_kwargs: Dict):
        for db_flow in self.db.get_by_kwargs(DBFlow, where_kwargs=where_kwargs):
            if db_flow.is_finished() and db_flow.status not in [200, 400]:
                self.db.update_by_kwargs(DBFlow,
                                         where_kwargs={"id": db_flow.id},
                                         update_kwargs=update_kwargs)

                finished_flow_context = FinishedFlowContext(**db_flow.flow_context.model_dump(),
                                                            mount_mapping=db_flow.mount_mapping)

                yield PublishContext(body=finished_flow_context.model_dump_json().encode(),
                                     pub_model_routing_key="SUCCESS",
                                     priority=finished_flow_context.flow.priority)

    def main_loop(self):
        self.logger.info("Yield eligible jobs")
        yield from self.get_and_update_runnable_jobs_by_kwargs(
            {"retries": 0},
            {},
            increment_retries=True,
        )

        self.logger.info("Yield finished flows")
        yield from self.get_and_update_unsent_flows_by_kwargs(
            {},
            {"status": 200},
        )

        # To do: Resubmit dbjobs which are timed_out
