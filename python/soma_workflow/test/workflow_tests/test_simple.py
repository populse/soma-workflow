from __future__ import with_statement
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 24 17:34:55 2013

@author: laure.hugo@cea.fr
@author: Soizic Laguitton
@organization: U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}

Workflow test of simple jobs:
* Workflow constitued of 4 jobs : job1, job2, job3, job4
* Dependencies : job2, job3 depend on job1
                 job4 depends on job2, job3
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
          job stdout and stderr
          job output
"""
import tempfile
import os
import sys

from soma_workflow.client import Helper
from soma_workflow.configuration import LIGHT_MODE
from soma_workflow.configuration import REMOTE_MODE
from soma_workflow.configuration import LOCAL_MODE
import soma_workflow.constants as constants
from soma_workflow.test.utils import identical_files

from soma_workflow.test.workflow_tests import WorkflowTest


class SimpleTest(WorkflowTest):

    allowed_config = [(LIGHT_MODE, WorkflowTest.LOCAL_PATH),
                      (LOCAL_MODE, WorkflowTest.LOCAL_PATH),
                      (REMOTE_MODE, WorkflowTest.FILE_TRANSFER),
                      (REMOTE_MODE, WorkflowTest.SHARED_RESOURCE_PATH),
                      (REMOTE_MODE, WorkflowTest.SHARED_TRANSFER)]

    def test_result(self):
        workflow = self.wf_examples.example_simple()
        self.wf_id = self.wf_ctrl.submit_workflow(
            workflow=workflow,
            name=self.__class__.__name__)
        # Transfer input files if file transfer
        if self.path_management == self.FILE_TRANSFER or \
                self.path_management == self.SHARED_TRANSFER:
            Helper.transfer_input_files(self.wf_id, self.wf_ctrl)
        # Wait for the workflow to finish
        Helper.wait_workflow(self.wf_id, self.wf_ctrl)
        # Transfer output files if file transfer
        if self.path_management == self.FILE_TRANSFER or \
                self.path_management == self.SHARED_TRANSFER:
            Helper.transfer_output_files(self.wf_id, self.wf_ctrl)

        status = self.wf_ctrl.workflow_status(self.wf_id)
        self.assertTrue(status == constants.WORKFLOW_DONE,
                        "workflow status : %s. Expected : %s" %
                        (status, constants.WORKFLOW_DONE))

        nb_failed_jobs = len(Helper.list_failed_jobs(self.wf_id,
                                                     self.wf_ctrl))
        self.assertTrue(nb_failed_jobs == 0,
                        "nb failed jobs : %i. Expected : %i" %
                        (nb_failed_jobs, 0))
        nb_failed_aborted_jobs = len(Helper.list_failed_jobs(
            self.wf_id,
            self.wf_ctrl,
            include_aborted_jobs=True))
        self.assertTrue(nb_failed_aborted_jobs == 0,
                        "nb failed jobs including aborted : %i. Expected : %i"
                        % (nb_failed_aborted_jobs, 0))

        (jobs_info, transfers_info, workflow_status, workflow_queue,
            tmp_files) = self.wf_ctrl.workflow_elements_status(self.wf_id)

        for (job_id, tmp_status, queue, exit_info, dates) in jobs_info:
            job_list = self.wf_ctrl.jobs([job_id])
            job_name, job_command, job_submission_date = job_list[job_id]

            self.tested_job = job_id

            if exit_info[0] == constants.FINISHED_REGULARLY:
                # To check job standard out and standard err
                job_stdout_file = tempfile.NamedTemporaryFile(
                    prefix="job_soma_out_log_",
                    suffix=repr(job_id),
                    delete=False)
                job_stdout_file = job_stdout_file.name
                job_stderr_file = tempfile.NamedTemporaryFile(
                    prefix="job_soma_outerr_log_",
                    suffix=repr(job_id),
                    delete=False)
                job_stderr_file = job_stderr_file.name

                try:
                    self.wf_ctrl.retrieve_job_stdouterr(job_id,
                                                        job_stdout_file,
                                                        job_stderr_file)
                    if job_name == 'job1':
                        # Test stdout
                        isSame, msg = identical_files(
                            job_stdout_file,
                            self.wf_examples.lo_stdout[1])
                        self.assertTrue(isSame, msg)
                        # Test no stderr
                        msg = "job stderr not empty : cf %s\n" \
                            "stderr:\n---\n%s\n---" \
                            % (job_stderr_file, open(job_stderr_file).read())
                        self.assertTrue(os.stat(job_stderr_file).st_size == 0,
                                        msg)
                        # Test output files
                        if self.path_management == self.LOCAL_PATH:
                            isSame, msg = identical_files(
                                self.wf_examples.lo_out_model_file[11],
                                self.wf_examples.lo_file[11])
                            self.assertTrue(isSame, msg)
                            isSame, msg = identical_files(
                                self.wf_examples.lo_out_model_file[12],
                                self.wf_examples.lo_file[12])
                            self.assertTrue(isSame, msg)
                        if self.path_management == self.FILE_TRANSFER or \
                                self.path_management == self.SHARED_TRANSFER:
                            isSame, msg = identical_files(
                                self.wf_examples.lo_out_model_file[11],
                                self.wf_examples.tr_file[11].client_path)
                            self.assertTrue(isSame, msg)
                            isSame, msg = identical_files(
                                self.wf_examples.lo_out_model_file[12],
                                self.wf_examples.tr_file[12].client_path)
                            self.assertTrue(isSame, msg)
                            # For unknown reason, it raises some errors
                            # http://stackoverflow.com/questions/10496758/unexpected-end-of-file-and-error-importing-function-definition-error-running
                            # isSame,	msg	= identical_files(job_stderr_file,self.wf_examples.lo_stderr[1])
                            # self.failUnless(isSame == True)

                    if job_name in ['job2', 'job3', 'job4']:
                        job_nb = int(job_name[3])
                        # Test stdout
                        isSame, msg = identical_files(
                            job_stdout_file,
                            self.wf_examples.lo_stdout[job_nb])
                        self.assertTrue(isSame, msg)
                        # Test no stderr
                        self.assertTrue(os.stat(job_stderr_file).st_size == 0,
                                        "job stderr not empty : cf %s" %
                                        job_stderr_file)
                        # Test output files
                        if self.path_management == self.LOCAL_PATH:
                            isSame, msg = identical_files(
                                self.wf_examples.lo_out_model_file[job_nb],
                                self.wf_examples.lo_file[job_nb])
                            self.assertTrue(isSame, msg)
                        if self.path_management == self.FILE_TRANSFER or \
                                self.path_management == self.SHARED_TRANSFER:
                            isSame, msg = identical_files(
                                self.wf_examples.lo_out_model_file[job_nb],
                                self.wf_examples.tr_file[job_nb].client_path)
                            self.assertTrue(isSame, msg)
                finally:
                    os.unlink(job_stdout_file)
                    os.unlink(job_stderr_file)

        del self.tested_job


def test():
    return SimpleTest.run_test_function(**WorkflowTest.parse_args(sys.argv))

if __name__ == '__main__':
    SimpleTest.run_test(**WorkflowTest.parse_args(sys.argv))
