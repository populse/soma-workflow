from __future__ import with_statement
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 24 17:15:01 2013

@author: laure.hugo@cea.fr

Workflow test of one job with a command:
* Workflow constitued of 1 job
* Dependencies : no dependencies
* Allowed configurations : Light mode - Local path
                           Local mode - Local path
                           Remote mode - File Transfer
                           Remote mode - Shared Resource Path (SRP)
                           Remote mode - File Transfer and SRP
* Expected comportment : The job succeeds
* Outcome independant of the configuration
"""
import tempfile
import sys

from soma_workflow.client import Helper
from soma_workflow.configuration import LIGHT_MODE
from soma_workflow.configuration import REMOTE_MODE
from soma_workflow.configuration import LOCAL_MODE
import soma_workflow.constants as constants
from soma_workflow.utils import identicalFiles

from soma_workflow.test.examples.workflow_test import WorkflowTest


class SpecialCommandTest(WorkflowTest):

    allowed_config = [(LIGHT_MODE, WorkflowTest.LOCAL_PATH),
                      (LOCAL_MODE, WorkflowTest.LOCAL_PATH),
                      (REMOTE_MODE, WorkflowTest.FILE_TRANSFER),
#                      (REMOTE_MODE, WorkflowTest.SHARED_RESOURCE_PATH),
#                      (REMOTE_MODE, WorkflowTest.SHARED_TRANSFER)
                      ]

    def test_result(self):
        workflow = SpecialCommandTest.wf_examples.example_special_command()
        self.wf_id = SpecialCommandTest.wf_ctrl.submit_workflow(
            workflow=workflow,
            name=self.__class__.__name__)

        # Transfer input files if file transfer
        if self.path_management == SpecialCommandTest.FILE_TRANSFER or \
                self.path_management == SpecialCommandTest.SHARED_TRANSFER:
            Helper.transfer_input_files(self.wf_id, SpecialCommandTest.wf_ctrl)
        # Wait for the worklow to finish
        Helper.wait_workflow(self.wf_id, SpecialCommandTest.wf_ctrl)

        status = self.wf_ctrl.workflow_status(self.wf_id)
        self.assertTrue(status == constants.WORKFLOW_DONE,
                        "workflow status : %s. Expected : %s" %
                        (status, constants.WORKFLOW_DONE))
        # TODO : sometimes raises an error
        # because status = "workflow_in_progress"

        nb_failed_jobs = len(Helper.list_failed_jobs(
            self.wf_id,
            SpecialCommandTest.wf_ctrl))
        self.assertTrue(nb_failed_jobs == 0,
                        "nb failed jobs : %i. Expected : %i" %
                        (nb_failed_jobs, 0))

        nb_failed_aborted_jobs = len(Helper.list_failed_jobs(
            self.wf_id,
            SpecialCommandTest.wf_ctrl,
            include_aborted_jobs=True))
        self.assertTrue(nb_failed_aborted_jobs == 0,
                        "nb failed jobs including aborted : %i. Expected : %i"
                        % (nb_failed_aborted_jobs, 0))
        # TODO: check the stdout
        (jobs_info, transfers_info, workflow_status, workflow_queue) = \
            SpecialCommandTest.wf_ctrl.workflow_elements_status(self.wf_id)

        for (job_id, tmp_status, queue, exit_info, dates) in jobs_info:
            job_list = self.wf_ctrl.jobs([job_id])
            job_name, job_command, job_submission_date = job_list[job_id]

            if exit_info[0] == constants.FINISHED_REGULARLY:
                # To check job standard out and standard err
                job_stdout_file = tempfile.NamedTemporaryFile(
                    prefix="job_soma_out_log_",
                    suffix=repr(job_id))
                job_stdout_file = job_stdout_file.name
                job_stderr_file = tempfile.NamedTemporaryFile(
                    prefix="job_soma_outerr_log_",
                    suffix=repr(job_id))
                job_stderr_file = job_stderr_file.name
                self.wf_ctrl.retrieve_job_stdouterr(job_id,
                                                    job_stdout_file,
                                                    job_stderr_file)
                if job_name == 'test_command_1':
                    # Test job stdout
                    isSame, msg = identicalFiles(
                        job_stdout_file,
                        SpecialCommandTest.wf_examples.lo_stdout_command_model)
                    self.assertTrue(isSame, msg)


if __name__ == '__main__':
    SpecialCommandTest.run_test(debug=True)
