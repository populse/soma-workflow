# -*- coding: utf-8 -*-
"""
Created on Mon Oct 28 15:09:02 2013

@author: laure.hugo@cea.fr
@author: Soizic Laguitton
@organization: U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
 ##########################  BROKEN : Doesn't work  ##########################
"""
import os
import sys

import soma_workflow.constants as constants
from soma_workflow.test.utils import check_files
from soma_workflow.test.job_tests.job_tests import JobTests
from soma_workflow.configuration import LIGHT_MODE
from soma_workflow.configuration import LOCAL_MODE
from soma_workflow.configuration import REMOTE_MODE


class ExceptionJobTest(JobTests):
    '''
    Submission of a job raising an exception
    '''
    allowed_resources = [LIGHT_MODE, LOCAL_MODE, REMOTE_MODE]

    def setUp(self):
        self.my_jobs = []
        self.my_transfers = []
        info = self.job_examples.submit_exception_job()
        self.my_jobs.append(info[0])
        self.client_files = []

    def tearDown(self):
        super(ExceptionJobTest, self).tearDown()
        for file in self.client_files:
            if os.path.isfile(file):
                os.remove(file)

    def test_result(self):
        jobid = self.my_jobs[0]
        self.wf_ctrl.wait_job(self.my_jobs)
        status = self.wf_ctrl.job_status(jobid)
        self.failUnless(status == constants.DONE or constants.FAILED,
                        'Job %s status after wait: %s' % (jobid, status))
        job_termination_status = self.wf_ctrl.job_termination_status(jobid)
        exit_status = job_termination_status[0]
        self.failUnless(exit_status == constants.FINISHED_REGULARLY,
                        'Job %s exit status: %s' % (jobid, exit_status))
        exit_value = job_termination_status[1]
        self.failUnless(exit_value == 1,
                        'Job exit value: %d' % exit_value)
        # checking stdout and stderr
        client_stdout = os.path.join(self.job_examples.output_dir,
                                     "stdout_exception_job")
        client_stderr = os.path.join(self.job_examples.output_dir,
                                     "stderr_exception_job")
        self.wf_ctrl.retrieve_job_stdouterr(jobid, client_stdout,
                                                client_stderr)
        self.client_files.append(client_stdout)
        self.client_files.append(client_stderr)

        (identical, msg) = check_files(
            self.client_files,
            self.job_examples.exceptionjobstdouterr, 1)
        self.failUnless(identical, msg)


if __name__ == '__main__':
    ExceptionJobTest.run_test(debug=True)
    sys.exit(0)
