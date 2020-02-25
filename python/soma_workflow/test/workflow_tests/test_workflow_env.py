# -*- coding: utf-8 -*-
"""
Created on Thu Oct 24 17:34:55 2013

license: CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html

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
from __future__ import with_statement, print_function
from __future__ import absolute_import
import tempfile
import os
import sys

from soma_workflow.client import Helper, Job, Workflow
from soma_workflow.configuration import LIGHT_MODE
from soma_workflow.configuration import REMOTE_MODE
from soma_workflow.configuration import LOCAL_MODE
import soma_workflow.constants as constants
from soma_workflow.test.utils import identical_files

from soma_workflow.test.workflow_tests import WorkflowTest


class WorkflowEnvTest(WorkflowTest):

    allowed_config = [(LIGHT_MODE, WorkflowTest.LOCAL_PATH),
                      (LOCAL_MODE, WorkflowTest.LOCAL_PATH),
                      (REMOTE_MODE, WorkflowTest.LOCAL_PATH)]

    def test_result(self):
        env_cmd = ['env']
        if sys.platform.startswith('win'):
            env_cmd = ['cmd', '/C', 'set']
        #env_cmd = [sys.executable, '-c', "print('\n'.join(['%s=%s' % (k, v) for k, v in os.environ.items()]))"]
        job1 = Job(name='job1', command=env_cmd, env={'JOB_ENV1': 'bidule'})
        job2 = Job(name='job2', command=env_cmd)
        expected_outputs = {
            'workflow_without_env': {
                'job1': 'JOB_ENV1=bidule',
            },
            'workflow_with_env': {
                'job1': 'JOB_ENV1=bidule\nWF_ENV1=truc\nWF_ENV2=caillou',
                'job2': 'JOB_ENV1=ciseaux\nWF_ENV1=truc\nWF_ENV2=caillou',
            },
            'workflow_with_env2': {
                'job1': 'JOB_ENV1=bidule\nWF_ENV1=truc\nWF_ENV2=caillou',
                'job2': 'JOB_ENV1=ciseaux\nWF_ENV1=truc\nWF_ENV2=caillou',
            },
        }
        workflow1 = Workflow(name='workflow_without_env', jobs=[job1, job2])
        workflow2 = Workflow(
            name='workflow_with_env', jobs=[job1, job2],
            env={'WF_ENV1': 'truc'},
            env_builder_code='from __future__ import print_function\n'
                'print(\'{\\n'
                '    "JOB_ENV1": "ciseaux",\\n'
                '    "WF_ENV2": "caillou"\\n'
                '}\\n'
                '\')')
        workflow3 = Workflow(
            name='workflow_with_env2', jobs=[job1, job2],
            env={'WF_ENV1': 'truc'},
            env_builder_code='from __future__ import print_function\n'
                'print(\'{\\n'
                '    "JOB_ENV1": "ciseaux",\\n'
                '    "WF_ENV2": "caillou",\\n'
                '    "WF_ENV1": "feuille"\\n'
                '}\\n'
                '\')')
        for workflow in (workflow1, workflow2, workflow3):
            self.wf_id = self.wf_ctrl.submit_workflow(
                workflow=workflow,
                name=self.__class__.__name__)
            # Transfer input files if file transfer
            if self.path_management == self.FILE_TRANSFER or \
                    self.path_management == self.SHARED_TRANSFER:
                Helper.transfer_input_files(self.wf_id, self.wf_ctrl)
            # Wait for the workflow to finish
            Helper.wait_workflow(self.wf_id, self.wf_ctrl)
            status = self.wf_ctrl.workflow_status(self.wf_id)
            self.assertTrue(status == constants.WORKFLOW_DONE,
                            "workflow status : %s. Expected : %s" %
                            (status, constants.WORKFLOW_DONE))

            nb_failed_jobs = len(Helper.list_failed_jobs(self.wf_id,
                                                         self.wf_ctrl))
            try:
                self.assertTrue(nb_failed_jobs == 0,
                                "nb failed jobs : %i. Expected : %i" %
                                (nb_failed_jobs, 0))
            except:  # noqa: E722
                print('jobs failed:', file=sys.stderr)
                print(Helper.list_failed_jobs(self.wf_id, self.wf_ctrl), file=sys.stderr)
                raise
            nb_failed_aborted_jobs = len(Helper.list_failed_jobs(
                self.wf_id,
                self.wf_ctrl,
                include_aborted_jobs=True))
            try:
                self.assertTrue(nb_failed_aborted_jobs == 0,
                                "nb failed jobs including aborted : %i. Expected : %i"
                                % (nb_failed_aborted_jobs, 0))
            except:  # noqa: E722
                print('aborted jobs:', file=sys.stderr)
                print(Helper.list_failed_jobs(self.wf_id, self.wf_ctrl, include_aborted_jobs=True), file=sys.stderr)
                raise

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
                        output \
                            = open(job_stdout_file).read().strip().split('\n')
                        exp_wf_outputs = expected_outputs[workflow.name]
                        if job_name in exp_wf_outputs:
                            exp = exp_wf_outputs[job_name].split('\n')
                            #print('### job', job_name, 'output:', output, file=sys.stderr)
                            #print('### expected:', exp, file=sys.stderr)
                            #print('### res:', [o in output for o in exp], file=sys.stderr)
                            self.assertTrue(all([o in output for o in exp]))
                    finally:
                        os.unlink(job_stdout_file)
                        os.unlink(job_stderr_file)

            del self.tested_job


def test():
    return WorkflowEnvTest.run_test_function(
        **WorkflowTest.parse_args(sys.argv))

if __name__ == '__main__':
    WorkflowEnvTest.run_test(**WorkflowTest.parse_args(sys.argv))
