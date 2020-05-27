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
import json

from soma_workflow.client import Helper, Job, Workflow
from soma_workflow.configuration import LIGHT_MODE
from soma_workflow.configuration import REMOTE_MODE
from soma_workflow.configuration import LOCAL_MODE
import soma_workflow.constants as constants
from soma_workflow.test.utils import identical_files

from soma_workflow.test.workflow_tests import WorkflowTest


def print_cmdline(argv):
    print('args:', argv)
    pfile = os.environ.get('SOMAWF_INPUT_PARAMS')
    conf = None
    if len(argv) != 0 and argv[0].startswith('conf={'):
        print('conf param')
        conf = json.loads(argv[0][5:])
    if pfile:
        print('with input file')
        with open(pfile) as f:
            params = json.load(f)
        print('params:')
        print(params)
        if 'configuration_dict' in params:
            conf = params['configuration_dict']
    print('config:')
    print(conf)


class WorkflowConfTest(WorkflowTest):

    allowed_config = [(LIGHT_MODE, WorkflowTest.LOCAL_PATH),
                      (LOCAL_MODE, WorkflowTest.LOCAL_PATH),
                      (REMOTE_MODE, WorkflowTest.LOCAL_PATH)]

    def test_result(self):
        cmd = [sys.executable, '-c',
               'from __future__ import print_function; import sys;'
               'from soma_workflow.test.workflow_tests.test_workflow_config import print_cmdline; '
               'print_cmdline(sys.argv[1:])', 'conf=%(configuration_dict)s']
        configuration = {'config1': 'value1', 'config2': 'value2'}
        print(cmd)
        job1 = Job(name='job1', command=cmd)
        job2 = Job(name='job2', command=cmd, configuration=configuration,
                   param_dict={})
        job3 = Job(name='job3', command=cmd, configuration=configuration,
                   use_input_params_file=True)
        expected_outputs = {
            'workflow': {
                'job1': 'args: [\'conf=%(configuration_dict)s\']\n'
                    'config:\n'
                    'None',
                'job2': '''args: ['conf={"config1": "value1", "config2": "value2"}']
conf param
config:
{'config1': 'value1', 'config2': 'value2'}''',
                'job3': '''args: [\'conf=%(configuration_dict)s\']
with input file
params:
{'parameters': {}, 'configuration_dict': {'config1': 'value1', 'config2': 'value2'}}
config:
{'config1': 'value1', 'config2': 'value2'}''',
            },
        }
        workflow1 = Workflow(name='workflow', jobs=[job1, job2, job3])
        for workflow in (workflow1, ):
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
                print(Helper.list_failed_jobs(self.wf_id, self.wf_ctrl),
                      file=sys.stderr)
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
                print(Helper.list_failed_jobs(
                    self.wf_id, self.wf_ctrl, include_aborted_jobs=True),
                    file=sys.stderr)
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
    return WorkflowConfTest.run_test_function(
        **WorkflowTest.parse_args(sys.argv))

if __name__ == '__main__':
    WorkflowConfTest.run_test(**WorkflowTest.parse_args(sys.argv))
