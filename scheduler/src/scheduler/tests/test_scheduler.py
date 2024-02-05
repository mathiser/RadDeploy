import logging
import queue
import unittest
import uuid

import yaml
from scheduler.impl import Scheduler

from DicomFlowLib.data_structures.contexts import FlowContext
from DicomFlowLib.data_structures.flow import Flow


class MyTestCase(unittest.TestCase):
    def setUp(self):
        file = "scheduler/tests/test_flows/dag_flow.yaml"
        with open(file) as r:
            fp = yaml.safe_load(r)
        flow = Flow(**fp)
        logger = logging.getLogger("Scheduler")
        self.scheduler = Scheduler(logger=logger,
                                   pub_routing_key_success="success",
                                   pub_routing_key_fail="fail",
                                   pub_routing_key_gpu="gpu",
                                   pub_routing_key_cpu="cpu")
        flow.is_valid_dag()
        self.fc = FlowContext(src_uid="asdfasdfasdf", flow=flow)

    def test_check_flow_finished_false(self):
        self.assertFalse(self.scheduler.check_flow_finished(self.fc))

    def test_update_mount_mapping(self):
        self.scheduler.update_mount_mapping(self.fc)
        self.assertIn(self.fc.uid, self.scheduler.mount_mapping)

    def test_yield_eligible_models_as_publish_contexts(self):
        self.scheduler.update_mount_mapping(self.fc)
        elig_models = list(self.scheduler.yield_eligible_models_as_publish_contexts(self.fc))
        self.assertEqual(1, len(elig_models))

        # Simulate first model run
        self.fc.mount_mapping["STRUCT"] = "new fancy uid"
        self.fc.mount_mapping["CT"] = "new bla"
        self.scheduler.update_mount_mapping(self.fc)
        elig_models = list(self.scheduler.yield_eligible_models_as_publish_contexts(self.fc))
        self.assertEqual(2, len(elig_models))
        for m in elig_models:
            self.assertIn(m.active_model_idx, [1, 2])

    def test_schedule_from_flow_context_success(self):
        q = queue.Queue()
        for fc, rk in self.scheduler.schedule_from_flow_context(self.fc):
            q.put(fc)

        while not q.empty():
            executed_fc = self.mock_model_run(q.get())
            new_fcs = self.scheduler.schedule_from_flow_context(executed_fc)
            for new_fc, routing_key in new_fcs:
                if routing_key != "success":
                    q.put(new_fc)

    def test_schedule_from_flow_context_fail(self):
        q = queue.Queue()
        for fc, rk in self.scheduler.schedule_from_flow_context(self.fc):
            q.put(fc)

        while not q.empty():
            executed_fc = self.mock_model_run(q.get())
            new_fcs = self.scheduler.schedule_from_flow_context(executed_fc)
            for new_fc, routing_key in new_fcs:
                print(routing_key)
                if routing_key != "success":
                    q.put(new_fc)

    @staticmethod
    def mock_model_run(fc: FlowContext):
        print(f"Executing model: {fc.active_model_idx}")
        model = fc.active_model
        for k in model.output_mount_keys:
            fc.mount_mapping[k] = str(uuid.uuid4())
        return fc


if __name__ == '__main__':
    unittest.main()
