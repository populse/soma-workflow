# -*- coding: utf-8 -*-
"""
Created on Thu Oct 24 17:15:01 2013

@author: laure.hugo@cea.fr
@author: Soizic Laguitton
@organization: U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}

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
* Tests : final status of the workflow
          number of failed jobs (excluding aborted)
          number of failed jobs (including aborted)
          job stdout and stderr
          submission warning when single quote comand
"""
from __future__ import with_statement, print_function
from __future__ import absolute_import
import tempfile
import os
import sys
import warnings

from soma_workflow.client import Helper
from soma_workflow.configuration import LIGHT_MODE
from soma_workflow.configuration import REMOTE_MODE
from soma_workflow.configuration import LOCAL_MODE
import soma_workflow.constants as constants
from soma_workflow.test.utils import identical_files

from soma_workflow.test.workflow_tests import WorkflowTest


class SpecialCommandTest(WorkflowTest):

    allowed_config = [(LIGHT_MODE, WorkflowTest.LOCAL_PATH),
                      (LOCAL_MODE, WorkflowTest.LOCAL_PATH),
                      (REMOTE_MODE, WorkflowTest.FILE_TRANSFER),
                      (REMOTE_MODE, WorkflowTest.SHARED_RESOURCE_PATH),
                      (REMOTE_MODE, WorkflowTest.SHARED_TRANSFER)]

    def test_result(self):
        # Cause all warnings to always be triggered.
        warnings.resetwarnings()
        warnings.simplefilter("always")
        with warnings.catch_warnings(record=True) as w:
            # Trigger a warning.
            workflow = self.wf_examples.example_special_command()
            # Verify some things
            self.assertTrue(len(w) == 1)
            self.assertTrue(issubclass(w[-1].category, UserWarning))
            self.assertTrue("This workflow prints a warning."
                            in str(w[-1].message))

        self.wf_id = self.wf_ctrl.submit_workflow(
            workflow=workflow,
            name=self.__class__.__name__)
        # Transfer input files if file transfer
        if self.path_management == self.FILE_TRANSFER or \
                self.path_management == self.SHARED_TRANSFER:
            Helper.transfer_input_files(self.wf_id, self.wf_ctrl)
        # Wait for the worklow to finish
        Helper.wait_workflow(self.wf_id, self.wf_ctrl)

        status = self.wf_ctrl.workflow_status(self.wf_id)
        self.assertTrue(status == constants.WORKFLOW_DONE,
                        "workflow status : %s. Expected : %s" %
                        (status, constants.WORKFLOW_DONE))
        # TODO : sometimes raises an error
        # because status = "workflow_in_progress"

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
                job_stdout_file = tempfile.mkstemp(
                    prefix="job_soma_out_log_",
                    suffix=repr(job_id))
                os.close(job_stdout_file[0])
                job_stdout_file = job_stdout_file[1]
                job_stderr_file = tempfile.mkstemp(
                    prefix="job_soma_outerr_log_",
                    suffix=repr(job_id))
                os.close(job_stderr_file[0])
                job_stderr_file = job_stderr_file[1]

                try:
                    self.wf_ctrl.retrieve_job_stdouterr(job_id,
                                                        job_stdout_file,
                                                        job_stderr_file)
                    # Test job stdout
                    if self.path_management == self.LOCAL_PATH:
                        isSame, msg = identical_files(
                            job_stdout_file,
                            self.wf_examples.lo_stdout_command_local)
                        self.assertTrue(isSame, msg)
                    else:
                        isSame, msg = identical_files(
                            job_stdout_file,
                            self.wf_examples.lo_stdout_command_remote)
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
    return SpecialCommandTest.run_test_function(
        **WorkflowTest.parse_args(sys.argv))

if __name__ == '__main__':
    SpecialCommandTest.run_test(**WorkflowTest.parse_args(sys.argv))
