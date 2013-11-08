from __future__ import with_statement
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 25 13:51:00 2013

@author: laure.hugo@cea.fr

Workflow test of a pipeline of jobs:
* Workflow constitued of (n+1) jobs : You can change the value n with nb
* Dependencies : The (i+1)th job depends on the (i)th job
* Allowed configurations : Light mode - Local path
                           Local mode - Local path
                           Remote mode - File Transfer
                           Remote mode - Shared Resource Path (SRP)
                           Remote mode - File Transfer and SRP
* Expected comportment : All jobs succeed
* Outcome independant of the configuration
* Tests : final status of the workflow
          number of failed jobs (excluding aborted)
          number of failed jobs (including aborted)
"""

from soma_workflow.client import Helper
from soma_workflow.configuration import LIGHT_MODE
from soma_workflow.configuration import REMOTE_MODE
from soma_workflow.configuration import LOCAL_MODE
import soma_workflow.constants as constants
from soma_workflow.test.examples.workflow_test import WorkflowTest


class SerialJobs(WorkflowTest):

    allowed_config = [(LIGHT_MODE, WorkflowTest.LOCAL_PATH),
                      (LOCAL_MODE, WorkflowTest.LOCAL_PATH),
                      (REMOTE_MODE, WorkflowTest.FILE_TRANSFER),
                      (REMOTE_MODE, WorkflowTest.SHARED_RESOURCE_PATH),
                      (REMOTE_MODE, WorkflowTest.SHARED_TRANSFER)]

    def test_result(self):
        nb = 5

        workflow = SerialJobs.wf_examples.example_serial_jobs(nb=nb)
        self.wf_id = SerialJobs.wf_ctrl.submit_workflow(
            workflow=workflow,
            name=self.__class__.__name__)
        # Transfer input files if file transfer
        if self.path_management == SerialJobs.FILE_TRANSFER or \
                self.path_management == SerialJobs.SHARED_TRANSFER:
            Helper.transfer_input_files(self.wf_id,
                                        SerialJobs.wf_ctrl)
        # Wait for the workflow to finish
        Helper.wait_workflow(self.wf_id, SerialJobs.wf_ctrl)
        # Transfer output files if file transfer
        if self.path_management == SerialJobs.FILE_TRANSFER or \
                self.path_management == SerialJobs.SHARED_TRANSFER:
            Helper.transfer_output_files(self.wf_id,
                                         SerialJobs.wf_ctrl)

        status = self.wf_ctrl.workflow_status(self.wf_id)
        self.assertTrue(status == constants.WORKFLOW_DONE,
                        "workflow status : %s. Expected : %s" %
                        (status, constants.WORKFLOW_DONE))
        nb_failed_jobs = len(Helper.list_failed_jobs(
            self.wf_id,
            SerialJobs.wf_ctrl))
        self.assertTrue(nb_failed_jobs == 0,
                        "nb failed jobs : %i. Expected : %i" %
                        (nb_failed_jobs, 0))
        nb_failed_aborted_jobs = len(Helper.list_failed_jobs(
            self.wf_id,
            SerialJobs.wf_ctrl,
            include_aborted_jobs=True))
        self.assertTrue(nb_failed_aborted_jobs == 0,
                        "nb failed jobs including aborted : %i. Expected : %i"
                        % (nb_failed_aborted_jobs, 0))


if __name__ == '__main__':
    SerialJobs.run_test(debug=False)
