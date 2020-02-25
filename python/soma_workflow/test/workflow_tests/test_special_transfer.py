# -*- coding: utf-8 -*-
"""
Created on Thu Oct 24 16:23:43 2013

@author: laure.hugo@cea.fr
@author: Soizic Laguitton
@organization: U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}

Workflow test of file transfer:
* Workflow constitued of 2 jobs : A job testing the contents of a directory
                                  A job testing multi file format
* Dependencies : no dependencies
* Allowed configurations : Remote mode - File Transfer
                           Remote mode - File Transfer and SRP
* Expected comportment : All jobs succeed
* As this test concerns file transfer, it only works with a file transfer
    path management
* Tests : final status of the workflow
          number of failed jobs (excluding aborted)
          number of failed jobs (including aborted)
          job stdout and stderr
"""
from __future__ import with_statement, print_function
from __future__ import absolute_import
import tempfile
import os
import sys

from soma_workflow.client import Helper
from soma_workflow.configuration import REMOTE_MODE
import soma_workflow.constants as constants
from soma_workflow.test.workflow_tests import WorkflowTest
from soma_workflow.test.utils import list_contents
from soma_workflow.test.utils import identical_files


class SpecialTransferTest(WorkflowTest):

    allowed_config = [(REMOTE_MODE, WorkflowTest.FILE_TRANSFER),
                      (REMOTE_MODE, WorkflowTest.SHARED_TRANSFER)]

    def test_result(self):
        workflow = self.wf_examples.example_special_transfer()
        self.wf_id = self.wf_ctrl.submit_workflow(
            workflow=workflow,
            name=self.__class__.__name__)

        # Transfer input files
        Helper.transfer_input_files(self.wf_id, self.wf_ctrl)
        # Wait for the worklow to finish
        Helper.wait_workflow(self.wf_id, self.wf_ctrl)
        status = self.wf_ctrl.workflow_status(self.wf_id)
        # Transfer output files
        Helper.transfer_output_files(self.wf_id, self.wf_ctrl)

        status = self.wf_ctrl.workflow_status(self.wf_id)
        self.assertTrue(status == constants.WORKFLOW_DONE,
                        "workflow status : %s. Expected : %s" %
                        (status, constants.WORKFLOW_DONE))
        nb_failed_jobs = len(Helper.list_failed_jobs(
            self.wf_id,
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

        for (job_id, tmp_status, queue, exit_info, dates, drmaa_id) \
                in jobs_info:
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
                    if job_name == 'dir_contents':
                        # Test job standard out
                        with open(job_stdout_file, 'r+') as f:
                            dir_contents = f.readlines()
                        dir_path_in = self.wf_examples.lo_in_dir
                        full_path_list = []
                        for element in os.listdir(dir_path_in):
                            full_path_list.append(os.path.join(dir_path_in,
                                                               element))
                        dir_contents_model = list_contents(full_path_list, [])
                        self.assertTrue(
                            sorted(dir_contents) == sorted(dir_contents_model))
                        # Test no stderr
                        self.assertTrue(os.stat(job_stderr_file).st_size == 0,
                                        "job stderr not empty : cf %s" %
                                        job_stderr_file)

                    if job_name == 'multi file format test':
                        # Test job standard out
                        isSame, msg = identical_files(
                            job_stdout_file,
                            self.wf_examples.lo_mff_stdout)
                        self.assertTrue(isSame, msg)
                        # Test no stderr
                        self.assertTrue(os.stat(job_stderr_file).st_size == 0,
                                        "job stderr not empty : cf %s" %
                                        job_stderr_file)
                finally:
                    os.unlink(job_stdout_file)
                    os.unlink(job_stderr_file)

        del self.tested_job


def test():
    return SpecialTransferTest.run_test_function(**WorkflowTest.parse_args(sys.argv))

if __name__ == '__main__':
    SpecialTransferTest.run_test(**WorkflowTest.parse_args(sys.argv))
