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
from __future__ import with_statement
from __future__ import print_function

from __future__ import absolute_import
import tempfile
import os
import sys
import six

from soma_workflow.client import Helper
from soma_workflow.configuration import LIGHT_MODE
from soma_workflow.configuration import REMOTE_MODE
from soma_workflow.configuration import LOCAL_MODE
import soma_workflow.constants as constants
from soma_workflow.test.utils import identical_files

from soma_workflow.test.workflow_tests import WorkflowTest


class SimpleTest(WorkflowTest):

    allowed_config = [(LIGHT_MODE, WorkflowTest.LOCAL_PATH),
                      (LIGHT_MODE, WorkflowTest.FILE_TRANSFER),
                      #(LIGHT_MODE, WorkflowTest.SHARED_RESOURCE_PATH),
                      (LOCAL_MODE, WorkflowTest.LOCAL_PATH),
                      (REMOTE_MODE, WorkflowTest.FILE_TRANSFER),
                      (REMOTE_MODE, WorkflowTest.SHARED_RESOURCE_PATH),
                      (REMOTE_MODE, WorkflowTest.SHARED_TRANSFER)]

    def run_workflow(self, workflow, test_files=[], test_dyn_files={}):
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

        failed_jobs = Helper.list_failed_jobs(self.wf_id, self.wf_ctrl)
        nb_failed_jobs = len(failed_jobs)
        if nb_failed_jobs != 0:
            self.print_jobs(failed_jobs, 'Failed jobs')

        self.assertTrue(nb_failed_jobs == 0,
                        "nb failed jobs : %i. Expected : %i" %
                        (nb_failed_jobs, 0))
        failed_aborted_jobs = Helper.list_failed_jobs(
            self.wf_id,
            self.wf_ctrl,
            include_aborted_jobs=True)
        nb_failed_aborted_jobs = len(failed_aborted_jobs)
        if nb_failed_aborted_jobs != 0:
            self.print_jobs(failed_aborted_jobs, 'Aborted jobs')
        self.assertTrue(nb_failed_aborted_jobs == 0,
                        "nb failed jobs including aborted : %i. Expected : %i"
                        % (nb_failed_aborted_jobs, 0))

        (jobs_info, transfers_info, workflow_status, workflow_queue,
            tmp_files) = self.wf_ctrl.workflow_elements_status(self.wf_id)

        dyn_out_params = {1: 'filePathOut2',
                          2: 'filePathOut'}
        dyn_out_params = {}

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
                    if job_name.startswith('job1'):
                        # Test stdout
                        isSame, msg = identical_files(
                            job_stdout_file,
                            self.wf_examples.lo_stdout[1])
                        self.assertTrue(isSame, msg)
                        # Test no stderr
                        with open(job_stderr_file) as f:
                            msg = "job stderr not empty : cf %s\n" \
                                "stderr:\n---\n%s\n---" \
                                % (job_stderr_file, f.read())
                        self.assertTrue(os.stat(job_stderr_file).st_size == 0,
                                        msg)

                    if job_name in test_dyn_files:
                        out_params = self.wf_ctrl.get_job_output_params(job_id)
                        dyn_out_params[job_name] = out_params

                    # For unknown reason, it raises some errors
                    # http://stackoverflow.com/questions/10496758/unexpected-end-of-file-and-error-importing-function-definition-error-running
                    # isSame,	msg	= identical_files(job_stderr_file,self.wf_examples.lo_stderr[1])
                    # self.failUnless(isSame == True)

                finally:
                    os.unlink(job_stdout_file)
                    os.unlink(job_stderr_file)

        for out_file_num in test_files:
            # Test output files
            if self.path_management == self.LOCAL_PATH:
                out_file = self.wf_examples.lo_file[out_file_num]
            elif self.path_management == self.FILE_TRANSFER or \
                    self.path_management == self.SHARED_TRANSFER:
                out_file = self.wf_examples.tr_file[out_file_num].client_path

            isSame, msg = identical_files(
                self.wf_examples.lo_out_model_file[out_file_num], out_file)
            self.assertTrue(isSame, msg)

        for job_name, ref_out_params in six.iteritems(test_dyn_files):
            out_params = dyn_out_params[job_name]
            for param, file_num in six.iteritems(ref_out_params):
                isSame, msg = identical_files(
                    self.wf_examples.lo_out_model_file[file_num],
                    out_params[param])
                self.assertTrue(isSame, msg)


        del self.tested_job

    def test_simple_workflow(self):
        workflow = self.wf_examples.example_simple()
        return self.run_workflow(workflow, test_files=[11, 12, 2, 3, 4])

    def test_workflow_with_outputs(self):
        workflow = self.wf_examples.example_dynamic_outputs()
        return self.run_workflow(
            workflow, test_files=[11, 3, 4],
            test_dyn_files={'job1_with_outputs': {'filePathOut2': 12},
                            'job2_with_outputs': {'filePathOut': 2}
                            })

    def test_workflow_mapreduce_with_outputs(self):
        workflow = self.wf_examples.example_dynamic_outputs_with_mapreduce()
        return self.run_workflow(workflow, test_files=[13])

    def test_workflow_loo(self):
        workflow = self.wf_examples.example_dynamic_outputs_with_loo()
        return self.run_workflow(
            workflow, test_files=[14],
            test_dyn_files={'test': {'output': 15}})

    def test_workflow_cv(self):
        workflow = self.wf_examples.example_dynamic_outputs_with_cv()
        return self.run_workflow(workflow, test_files=[16, 17])

    def test_workflow_mapreduce_with_jobs(self):
        workflow \
            = self.wf_examples.example_dynamic_outputs_with_mapreduce_jobs()
        #Helper.serialize('/tmp/workflow.workflow', workflow)
        return self.run_workflow(workflow, test_files=[13])

    def test_workflow_loo_with_jobs(self):
        workflow = self.wf_examples.example_dynamic_outputs_with_loo_jobs()
        return self.run_workflow(
            workflow, test_files=[14],
            test_dyn_files={'test': {'output': 15}})

    def test_workflow_cv_with_jobs(self):
        workflow = self.wf_examples.example_dynamic_outputs_with_cv_jobs()
        return self.run_workflow(workflow, test_files=[16, 17])


def test():
    return SimpleTest.run_test_function(**WorkflowTest.parse_args(sys.argv))

if __name__ == '__main__':
    SimpleTest.run_test(**WorkflowTest.parse_args(sys.argv))
