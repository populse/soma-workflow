# -*- coding: utf-8 -*-
"""
Created on Mon Oct 28 15:09:02 2013

@author: laure
"""
import os
import sys

import soma_workflow.constants as constants
from soma_workflow.test.utils import check_files
from soma_workflow.test.job_examples.jobs_test import JobsTest
from soma_workflow.configuration import LIGHT_MODE
from soma_workflow.configuration import LOCAL_MODE
from soma_workflow.configuration import REMOTE_MODE


class ExceptionJobTest(JobsTest):
    '''
    Submission of a job raising an exception
    '''
    allowed_resources = [LIGHT_MODE, LOCAL_MODE, REMOTE_MODE]

    def setUp(self):
        self.my_jobs = []
        self.my_transfers = []
        info = JobsTest.job_examples.submit_exception_job()
        self.my_jobs.append(info[0])
        self.client_files = []

    def tearDown(self):
        JobsTest.tearDown(self)
        for file in self.client_files:
            if os.path.isfile(file):
                os.remove(file)

    def test_aresult(self):
        jobid = self.my_jobs[0]
        JobsTest.wf_ctrl.wait_job(self.my_jobs)
        status = JobsTest.wf_ctrl.job_status(jobid)
        self.failUnless(status == constants.DONE or constants.FAILED,
                        'Job %s status after wait: %s' % (jobid, status))
        job_termination_status = JobsTest.wf_ctrl.job_termination_status(jobid)
        exit_status = job_termination_status[0]
        self.failUnless(exit_status == constants.FINISHED_REGULARLY,
                        'Job %s exit status: %s' % (jobid, exit_status))
        exit_value = job_termination_status[1]
        self.failUnless(exit_value == 1,
                        'Job exit value: %d' % exit_value)
        # checking stdout and stderr
        client_stdout = os.path.join(JobsTest.job_examples.output_dir,
                                     "stdout_exception_job")
        client_stderr = os.path.join(JobsTest.job_examples.output_dir,
                                     "stderr_exception_job")
        JobsTest.wf_ctrl.retrieve_job_stdouterr(jobid, client_stdout,
                                                client_stderr)
        self.client_files.append(client_stdout)
        self.client_files.append(client_stderr)

        (identical, msg) = check_files(
            self.client_files,
            JobsTest.job_examples.exceptionjobstdouterr, 1)
        self.failUnless(identical, msg)


if __name__ == '__main__':
    ExceptionJobTest.run_test(debug=True)
    sys.exit(0)
