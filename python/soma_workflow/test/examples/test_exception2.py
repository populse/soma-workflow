from __future__ import with_statement
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 25 09:33:40 2013

@author: laure.hugo@cea.fr

Workflow test of job exception:
* Workflow constitued of 4 jobs : job1, job2, job4,
                                  job3 with exception
* Dependencies : job2, job3 depend on job1
                 job4 depends on job2, job3
* Allowed configurations : Light mode - Local path
                           Local mode - Local path
                           Remote mode - File Transfer
                           Remote mode - Shared Resource Path (SRP)
                           Remote mode - File Transfer and SRP
* Expected comportment : job1, job2 succeed
                         job3 fails
                         job4 is aborted
* Outcome independant of the configuration
"""

from soma_workflow.client import Helper
from soma_workflow.configuration import LIGHT_MODE
from soma_workflow.configuration import REMOTE_MODE
from soma_workflow.configuration import LOCAL_MODE
import soma_workflow.constants as constants
from soma_workflow.test.utils import identicalFiles
from soma_workflow.test.examples.workflow_test import WorkflowTest


class Exception2Test(WorkflowTest):

    allowed_config = [(LIGHT_MODE, WorkflowTest.LOCAL_PATH),
                      (LOCAL_MODE, WorkflowTest.LOCAL_PATH),
                      (REMOTE_MODE, WorkflowTest.FILE_TRANSFER),
                      (REMOTE_MODE, WorkflowTest.SHARED_RESOURCE_PATH),
                      (REMOTE_MODE, WorkflowTest.SHARED_TRANSFER)]

    def test_result(self):
        workflow = Exception2Test.wf_examples.example_simple_exception2()
        self.wf_id = Exception2Test.wf_ctrl.submit_workflow(
            workflow=workflow,
            name=self.__class__.__name__)
        # Transfer input files if file transfer
        if self.path_management == Exception2Test.FILE_TRANSFER or \
                self.path_management == Exception2Test.SHARED_TRANSFER:
            Helper.transfer_input_files(self.wf_id, Exception2Test.wf_ctrl)
        # Wait for the workflow to finish
        Helper.wait_workflow(self.wf_id, Exception2Test.wf_ctrl)
        # Transfer output files if file transfer
        if self.path_management == Exception2Test.FILE_TRANSFER or \
                self.path_management == Exception2Test.SHARED_TRANSFER:
            Helper.transfer_output_files(self.wf_id, Exception2Test.wf_ctrl)

        status = self.wf_ctrl.workflow_status(self.wf_id)
        self.assertTrue(status == constants.WORKFLOW_DONE)
        self.assertTrue(len(Helper.list_failed_jobs(
                        self.wf_id,
                        Exception2Test.wf_ctrl)) == 1)
        self.assertTrue(len(Helper.list_failed_jobs(
                        self.wf_id,
                        Exception2Test.wf_ctrl,
                        include_aborted_jobs=True)) == 2)

        (jobs_info, transfers_info, workflow_status, workflow_queue) = \
            Exception2Test.wf_ctrl.workflow_elements_status(self.wf_id)

        # TODO: check the stdout and stderrr
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
                if job_name == 'job1':
                    isSame, msg = identicalFiles(
                        job_stdout_file,
                        Exception2Test.wf_examples.lo_stdout[1])
                    self.assertTrue(isSame)
                    if self.path_management == Exception2Test.LOCAL_PATH:
                        isSame, msg = identicalFiles(
                            Exception2Test.wf_examples.lo_out_model_file[11],
                            Exception2Test.wf_examples.lo_file[11])
                        self.assertTrue(isSame)
                        isSame, msg = identicalFiles(
                            Exception2Test.wf_examples.lo_out_model_file[12],
                            Exception2Test.wf_examples.lo_file[12])
                        self.assertTrue(isSame)
                        isSame, msg = identicalFiles(
                            job_stderr_file,
                            Exception2Test.wf_examples.lo_stderr[1])
                        self.assertTrue(isSame)
                    if self.path_management == Exception2Test.FILE_TRANSFER:
                        isSame, msg = identicalFiles(
                            Exception2Test.wf_examples.lo_out_model_file[11],
                            Exception2Test.wf_examples.tr_file[11].client_path)
                        self.assertTrue(isSame)
                        isSame, msg = identicalFiles(
                            Exception2Test.wf_examples.lo_out_model_file[12],
                            Exception2Test.wf_examples.tr_file[12].client_path)
                        self.assertTrue(isSame)

                if job_name in ['job2', 'job4']:
                    job_nb = int(job_name[3])
                    isSame, msg = identicalFiles(
                        job_stdout_file,
                        Exception2Test.wf_examples.lo_stdout[job_nb])
                    self.assertTrue(isSame)
                    if self.path_management == Exception2Test.LOCAL_PATH:
                        isSame, msg = identicalFiles(
                            Exception2Test.wf_examples.lo_out_model_file[job_nb],
                            Exception2Test.wf_examples.lo_file[job_nb])
                        self.assertTrue(isSame)
                        isSame, msg = identicalFiles(
                            job_stderr_file,
                            Exception2Test.wf_examples.lo_stderr[job_nb])
                        self.assertTrue(isSame)
                    if self.path_management == Exception2Test.FILE_TRANSFER:
                        isSame, msg = identicalFiles(
                            Exception2Test.wf_examples.lo_out_model_file[job_nb],
                            Exception2Test.wf_examples.tr_file[job_nb].client_path)
                        self.assertTrue(isSame)

                if job_name == 'job3 with exception':
                    isSame, msg = identicalFiles(
                        job_stdout_file,
                        Exception2Test.wf_examples.lo_stdout1_exception_model)
                    self.assertTrue(isSame)


if __name__ == '__main__':
    Exception2Test.run_test(debug=False)
