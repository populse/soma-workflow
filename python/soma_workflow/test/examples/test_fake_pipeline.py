from __future__ import with_statement
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 25 13:51:00 2013

@author: laure.hugo@cea.fr

Workflow test of a fake pipeline of operations:
* Workflow constitued of 100 groups of 7 jobs each :
    Brain extraction(1), test1(11), test2(12), test3(13),
    Gray/white segmentation(2), Left hemisphere sulci recognition(3),
    Right hemisphere sulci recognition(4)
* Dependencies : job11 depends on job1
                 job12 depends on job11
                 job13 depends on job12
                 job2 depends on job13
                 job3, job4 depend on job2
* Allowed configurations : Light mode - Local path
                           Local mode - Local path
                           Remote mode - File Transfer
                           Remote mode - Shared Resource Path (SRP)
                           Remote mode - File Transfer and SRP
* Expected comportment : All jobs succeed
* Outcome independant of the configuration
"""

from soma_workflow.client import Helper
from soma_workflow.configuration import LIGHT_MODE
from soma_workflow.configuration import REMOTE_MODE
from soma_workflow.configuration import LOCAL_MODE
import soma_workflow.constants as constants
from soma_workflow.test.examples.workflow_test import WorkflowTest


class FakePipelineT1(WorkflowTest):

    allowed_config = [(LIGHT_MODE, WorkflowTest.LOCAL_PATH),
                      (LOCAL_MODE, WorkflowTest.LOCAL_PATH),
                      (REMOTE_MODE, WorkflowTest.FILE_TRANSFER),
                      (REMOTE_MODE, WorkflowTest.SHARED_RESOURCE_PATH),
                      (REMOTE_MODE, WorkflowTest.SHARED_TRANSFER)]

    def test_result(self):
        workflow = FakePipelineT1.wf_examples.example_fake_pipelineT1()
        self.wf_id = FakePipelineT1.wf_ctrl.submit_workflow(
            workflow=workflow,
            name=self.__class__.__name__)
        # Transfer input files if file transfer
        if self.path_management == FakePipelineT1.FILE_TRANSFER or \
                self.path_management == FakePipelineT1.SHARED_TRANSFER:
            Helper.transfer_input_files(self.wf_id,
                                        FakePipelineT1.wf_ctrl)
        # Wait for the workflow to finish
        Helper.wait_workflow(self.wf_id, FakePipelineT1.wf_ctrl)
        # Transfer output files if file transfer
        if self.path_management == FakePipelineT1.FILE_TRANSFER or \
                self.path_management == FakePipelineT1.SHARED_TRANSFER:
            Helper.transfer_output_files(self.wf_id,
                                         FakePipelineT1.wf_ctrl)

        status = self.wf_ctrl.workflow_status(self.wf_id)
        self.assertTrue(status == constants.WORKFLOW_DONE)
        self.assertTrue(len(Helper.list_failed_jobs(
                        self.wf_id,
                        FakePipelineT1.wf_ctrl)) == 0)
        self.assertTrue(len(Helper.list_failed_jobs(
                        self.wf_id,
                        FakePipelineT1.wf_ctrl,
                        include_aborted_jobs=True)) == 0)


if __name__ == '__main__':
    FakePipelineT1.run_test(debug=False)
