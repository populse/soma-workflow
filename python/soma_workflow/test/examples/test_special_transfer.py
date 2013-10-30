from __future__ import with_statement
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 24 16:23:43 2013

@author: laure.hugo@cea.fr

Workflow test of file transfer:
* Workflow constitued of 2 jobs : A job testing the contents of a directory
                                  A job testing multi file format
* Dependencies : no dependencies
* Allowed configurations : Remote mode - File Transfer
                           Remote mode - File Transfer and SRP
* Expected comportment : All jobs succeed
* As this test concerns file transfer, it only works with a file transfer
    path management
"""

from soma_workflow.client import Helper
from soma_workflow.configuration import REMOTE_MODE
import soma_workflow.constants as constants
from soma_workflow.test.examples.workflow_test import WorkflowTest


class SpecialTransferTest(WorkflowTest):

    allowed_config = [(REMOTE_MODE, WorkflowTest.FILE_TRANSFER),
                      (REMOTE_MODE, WorkflowTest.SHARED_TRANSFER)]

    def test_result(self):
        workflow = SpecialTransferTest.wf_examples.example_special_transfer()
        self.wf_id = SpecialTransferTest.wf_ctrl.submit_workflow(
            workflow=workflow,
            name=self.__class__.__name__)

        # Transfer input files
        Helper.transfer_input_files(self.wf_id, SpecialTransferTest.wf_ctrl)
        # Wait for the worklow to finish
        Helper.wait_workflow(self.wf_id, SpecialTransferTest.wf_ctrl)
        status = self.wf_ctrl.workflow_status(self.wf_id)
        # Transfer output files
        Helper.transfer_output_files(self.wf_id, SpecialTransferTest.wf_ctrl)

        self.assertTrue(status == constants.WORKFLOW_DONE)
        self.assertTrue(len(Helper.list_failed_jobs(
            self.wf_id,
            SpecialTransferTest.wf_ctrl)) == 0)
        self.assertTrue(len(Helper.list_failed_jobs(
            self.wf_id,
            SpecialTransferTest.wf_ctrl,
            include_aborted_jobs=True)) == 0)
        # TODO: check the stdout


if __name__ == '__main__':
    SpecialTransferTest.run_test(debug=False)
