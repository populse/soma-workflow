from __future__ import with_statement
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 25 09:28:55 2013

@author: laure.hugo@cea.fr

Workflow test of job exception:
* Workflow constitued of 4 jobs : job1 with exception,
                                  job2, job3, job4
* Dependencies : job2, job3 depend on job1
                 job4 depends on job2, job3
* Allowed configurations : Light mode - Local path
                           Local mode - Local path
                           Remote mode - File Transfer
                           Remote mode - Shared Resource Path (SRP)
                           Remote mode - File Transfer and SRP
* Expected comportment : job1 fails
                         job2, job3, job4 are aborted
* Outcome independant of the configuration
"""

from soma_workflow.client import Helper
from soma_workflow.configuration import LIGHT_MODE
from soma_workflow.configuration import REMOTE_MODE
from soma_workflow.configuration import LOCAL_MODE
import soma_workflow.constants as constants
from soma_workflow.test.utils import identicalFiles

from soma_workflow.test.examples.workflow_test import WorkflowTest


class Exception1Test(WorkflowTest):
    allowed_config = [(LIGHT_MODE, WorkflowTest.LOCAL_PATH),
                      (LOCAL_MODE, WorkflowTest.LOCAL_PATH),
                      (REMOTE_MODE, WorkflowTest.FILE_TRANSFER),
                      (REMOTE_MODE, WorkflowTest.SHARED_RESOURCE_PATH),
                      (REMOTE_MODE, WorkflowTest.SHARED_TRANSFER)]

    def test_result(self):
        workflow = Exception1Test.wf_examples.example_simple_exception1()
        self.wf_id = Exception1Test.wf_ctrl.submit_workflow(
            workflow=workflow,
            name=self.__class__.__name__)
        # Transfer input files if file transfer
        if self.path_management == Exception1Test.FILE_TRANSFER or \
                self.path_management == Exception1Test.SHARED_TRANSFER:
            Helper.transfer_input_files(self.wf_id, Exception1Test.wf_ctrl)
        # Wait for the workflow to finish
        Helper.wait_workflow(self.wf_id, Exception1Test.wf_ctrl)
        # Transfer output files if file transfer
        if self.path_management == Exception1Test.FILE_TRANSFER or \
                self.path_management == Exception1Test.SHARED_TRANSFER:
            Helper.transfer_output_files(self.wf_id, Exception1Test.wf_ctrl)

        status = self.wf_ctrl.workflow_status(self.wf_id)
        self.assertTrue(status == constants.WORKFLOW_DONE)
        self.assertTrue(len(Helper.list_failed_jobs(
                        self.wf_id,
                        Exception1Test.wf_ctrl)) == 1)
        self.assertTrue(len(Helper.list_failed_jobs(
                        self.wf_id,
                        Exception1Test.wf_ctrl,
                        include_aborted_jobs=True)) == 4)

        (jobs_info, transfers_info, workflow_status, workflow_queue) = \
            Exception1Test.wf_ctrl.workflow_elements_status(self.wf_id)

        for (job_id, tmp_status, queue, exit_info, dates) in jobs_info:
            job_list = self.wf_ctrl.jobs([job_id])
            job_name, job_command, job_submission_date = job_list[job_id]

            if exit_info[0] == constants.FINISHED_REGULARLY:
                # To check job standard out
                job_stdout_file = "/tmp/job_soma_out_log_" + repr(job_id)
                job_stderr_file = "/tmp/job_soma_outerr_log_" + repr(job_id)
                self.wf_ctrl.retrieve_job_stdouterr(job_id,
                                                    job_stdout_file,
                                                    job_stderr_file)

                if job_name in ['job2', 'job3', 'job4']:
                    job_nb = int(job_name[3])
                    isSame, msg = identicalFiles(
                        job_stdout_file,
                        Exception1Test.wf_examples.lo_stdout[job_nb])
                    self.assertTrue(isSame)
                    if self.path_management == Exception1Test.LOCAL_PATH:
                        isSame, msg = identicalFiles(
                            Exception1Test.wf_examples.lo_out_model_file[job_nb],
                            Exception1Test.wf_examples.lo_file[job_nb])
                        self.assertTrue(isSame)
                        isSame, msg = identicalFiles(
                            job_stderr_file,
                            Exception1Test.wf_examples.lo_stderr[job_nb])
                        self.assertTrue(isSame)
                    if self.path_management == Exception1Test.FILE_TRANSFER:
                        isSame, msg = identicalFiles(
                            Exception1Test.wf_examples.lo_out_model_file[job_nb],
                            Exception1Test.wf_examples.tr_file[job_nb].client_path)
                        self.assertTrue(isSame)

                if job_name == 'job1 with exception':
                    isSame, msg = identicalFiles(
                        job_stdout_file,
                        Exception1Test.wf_examples.lo_stdout1_exception_model)
                    self.assertTrue(isSame)


if __name__ == '__main__':
    Exception1Test.run_test(debug=False)
