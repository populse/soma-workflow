# -*- coding: utf-8 -*-
"""
Created on Fri Oct 25 13:51:00 2013

@author: laure.hugo@cea.fr
@author: Soizic Laguitton
@organization: U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}

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
* Tests : final status of the workflow
          number of failed jobs (excluding aborted)
          number of failed jobs (including aborted)
          job stdout and stderr
"""
from __future__ import with_statement
from __future__ import absolute_import
import os
import tempfile
import sys

from soma_workflow.client import Helper
from soma_workflow.configuration import LIGHT_MODE
from soma_workflow.configuration import REMOTE_MODE
from soma_workflow.configuration import LOCAL_MODE
import soma_workflow.constants as constants
from soma_workflow.test.workflow_tests import WorkflowTest


class FakePipelineTest(WorkflowTest):

    allowed_config = [(LIGHT_MODE, WorkflowTest.LOCAL_PATH),
                      (LOCAL_MODE, WorkflowTest.LOCAL_PATH),
                      (REMOTE_MODE, WorkflowTest.FILE_TRANSFER),
                      (REMOTE_MODE, WorkflowTest.SHARED_RESOURCE_PATH),
                      (REMOTE_MODE, WorkflowTest.SHARED_TRANSFER)]

    def test_result(self):
        if hasattr(self.wf_ctrl.scheduler_config, 'get_proc_nb'):
            n_iter = 5 * self.wf_ctrl.scheduler_config.get_proc_nb()
        else:
            n_iter = 5
        workflow = self.wf_examples.example_fake_pipelineT1(n_iter)
        self.wf_id = self.wf_ctrl.submit_workflow(
            workflow=workflow,
            name=self.__class__.__name__)
        # Transfer input files if file transfer
        if self.path_management == self.FILE_TRANSFER or \
                self.path_management == self.SHARED_TRANSFER:
            Helper.transfer_input_files(self.wf_id,
                                        self.wf_ctrl)
        # Wait for the workflow to finish
        Helper.wait_workflow(self.wf_id, self.wf_ctrl)
        # Transfer output files if file transfer
        if self.path_management == self.FILE_TRANSFER or \
                self.path_management == self.SHARED_TRANSFER:
            Helper.transfer_output_files(self.wf_id,
                                         self.wf_ctrl)

        status = self.wf_ctrl.workflow_status(self.wf_id)
        self.assertTrue(status == constants.WORKFLOW_DONE)
        self.assertTrue(len(Helper.list_failed_jobs(
                        self.wf_id,
                        self.wf_ctrl)) == 0)
        self.assertTrue(len(Helper.list_failed_jobs(
                        self.wf_id,
                        self.wf_ctrl,
                        include_aborted_jobs=True)) == 0)

        (jobs_info, transfers_info, workflow_status, workflow_queue,
            tmp_files) = self.wf_ctrl.workflow_elements_status(self.wf_id)

        for (job_id, tmp_status, queue, exit_info, dates, dmraa_id) \
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
                    # Test stdout
                    self.assertTrue(os.stat(job_stdout_file).st_size == 0,
                                    "job stdout not empty : cf %s" %
                                    job_stdout_file)
                    # Test no stderr
                    self.assertTrue(os.stat(job_stderr_file).st_size == 0,
                                    "job stderr not empty : cf %s" %
                                    job_stderr_file)
                finally:
                    if os.path.exists(job_stdout_file):
                        os.unlink(job_stdout_file)
                    if os.path.exists(job_stderr_file):
                        os.unlink(job_stderr_file)

            del self.tested_job


def test():
    return FakePipelineTest.run_test_function(
        **WorkflowTest.parse_args(sys.argv))

if __name__ == '__main__':
    FakePipelineTest.run_test(**WorkflowTest.parse_args(sys.argv))
