# -*- coding: utf-8 -*-
"""
Created on Mon Oct 28 15:00:17 2013

@author: laure.hugo@cea.fr
"""
import os
import sys

import soma_workflow.constants as constants
from soma_workflow.test.utils import check_files
from soma_workflow.test.job_examples.jobs_test import JobsTest
from soma_workflow.configuration import LIGHT_MODE
from soma_workflow.configuration import LOCAL_MODE
#from soma_workflow.configuration import REMOTE_MODE


class LocalCustomSubmission(JobsTest):
    '''
    Submission of a job using user's files only (even for stdout and stderr)
    '''
    allowed_resources = [LIGHT_MODE, LOCAL_MODE]

    def setUp(self):
        self.my_jobs = []
        self.my_transfers = []
        info = JobsTest.job_examples.local_custom_submission()
        self.my_jobs.append(info[0])
        self.output_files = info[1]
        self.stdouterr_files = info[2]

    def tearDown(self):
        JobsTest.tearDown(self)
        for file in self.output_files:
            if os.path.isfile(file):
                os.remove(file)
        for file in self.stdouterr_files:
            if os.path.isfile(file):
                os.remove(file)

    def test_result(self):
        jobid = self.my_jobs[0]
        JobsTest.wf_ctrl.wait_job(self.my_jobs)
        status = JobsTest.wf_ctrl.job_status(jobid)

        self.failUnless(status == constants.DONE,
                        'Job %s status after wait: %s' % (jobid, status))
        job_termination_status = JobsTest.wf_ctrl.job_termination_status(jobid)
        exit_status = job_termination_status[0]
        self.failUnless(exit_status == constants.FINISHED_REGULARLY,
                        'Job %s exit status: %s' % (jobid, exit_status))
        exit_value = job_termination_status[1]
        self.failUnless(exit_value == 0,
                        'Job %s exit value: %d' % (jobid, exit_value))

        # checking output files
        for file in self.output_files:
            self.failUnless(os.path.isfile(file),
                            'File %s doesn t exit' % file)

        (correct, msg) = check_files(
            self.output_files,
            JobsTest.job_examples.job1_output_file_models)
        self.failUnless(correct, msg)

        # checking stderr and stdout files
        for file in self.stdouterr_files:
            self.failUnless(os.path.isfile(file),
                            'File %s doesn t exit' % file)
        (correct, msg) = check_files(
            self.stdouterr_files,
            JobsTest.job_examples.job1_stdouterr_models)
        self.failUnless(correct, msg)


if __name__ == '__main__':
    LocalCustomSubmission.run_test(debug=False)
    sys.exit(0)
