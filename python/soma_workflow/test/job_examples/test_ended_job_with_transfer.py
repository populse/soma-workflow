# -*- coding: utf-8 -*-
"""
Created on Mon Oct 28 15:06:18 2013

@author: laure
"""
import sys
from soma_workflow.test.job_examples.jobs_test import JobsTest


class EndedJobWithTransfer(JobsTest):
    '''
    Submission of a job with transfer
    '''
    def setUp(self):
        self.my_jobs = []
        self.my_transfers = []
        info = JobsTest.job_examples.submit_job1()
        self.my_jobs.append(info[0])
        self.output_files = info[1]
        self.client_files = []

        JobsTest.wf_ctrl.wait_job(self.my_jobs)

    def tearDown(self):
        JobsTest.tearDown(self)

    def test_result(self):
        self.failUnless(True)


if __name__ == '__main__':
    EndedJobWithTransfer.run_test(debug=False)
    sys.exit(0)
