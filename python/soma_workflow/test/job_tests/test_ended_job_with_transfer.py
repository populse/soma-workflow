# -*- coding: utf-8 -*-
"""
Created on Mon Oct 28 15:06:18 2013

@author: Soizic Laguitton
@organization: U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
@author: laure.hugo@cea.fr
"""
import sys
from soma_workflow.test.job_tests.job_tests import JobTests
from soma_workflow.configuration import LIGHT_MODE
from soma_workflow.configuration import LOCAL_MODE
from soma_workflow.configuration import REMOTE_MODE


class EndedJobWithTransfer(JobTests):

    '''
    Submission of a job with transfer
    '''
    allowed_resources = [LIGHT_MODE, LOCAL_MODE, REMOTE_MODE]

    def setUp(self):
        self.my_jobs = []
        self.my_transfers = []
        info = self.job_examples.submit_job1()
        self.my_jobs.append(info[0])
        self.output_files = info[1]
        self.client_files = []

        self.wf_ctrl.wait_job(self.my_jobs)

    def tearDown(self):
        super(EndedJobWithTransfer, self).tearDown()

    def test_result(self):
        self.failUnless(True)


if __name__ == '__main__':
    EndedJobWithTransfer.run_test(debug=False)
    sys.exit(0)
