# -*- coding: utf-8 -*-
#"""
# Created on Mon Oct 28 15:09:02 2013
#
#@author: laure.hugo@cea.fr
#@author: Soizic Laguitton
#@organization: U{IFR 49<http://www.ifr49.org>}
#@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
# BROKEN : Doesn't work  ##########################
#"""
# import os
# import sys
#
# import soma_workflow.constants as constants
# from soma_workflow.test.utils import identical_files
# from soma_workflow.test.job_tests.job_tests import JobTests
# from soma_workflow.configuration import LIGHT_MODE
# from soma_workflow.configuration import LOCAL_MODE
# from soma_workflow.configuration import REMOTE_MODE
#
#
# class ExceptionJobTest(JobTests):
#    '''
#    Submission of a job raising an exception
#    '''
#    allowed_resources = [LIGHT_MODE, LOCAL_MODE, REMOTE_MODE]
#
#    def setUp(self):
#        self.my_jobs = []
#        self.my_transfers = []
#        info = self.job_examples.submit_exception_job()
#        self.my_jobs.append(info[0])
#        self.client_files = []
#
#    def tearDown(self):
#        super(ExceptionJobTest, self).tearDown()
#        for file in self.client_files:
#            if os.path.isfile(file):
#                os.remove(file)
#
#    def test_result(self):
#        jobid = self.my_jobs[0]
#        self.wf_ctrl.wait_job(self.my_jobs)
#        status = self.wf_ctrl.job_status(jobid)
# Test end of workflow
#        self.failUnless(status == constants.DONE or constants.DONE,
#                        'Job %s status after wait: %s, instead of %s or %s' %
#                        (jobid, status, constants.DONE, constants.DONE))
#        job_termination_status = self.wf_ctrl.job_termination_status(jobid)
#        exit_status = job_termination_status[0]
#        self.failUnless(exit_status == constants.FINISHED_REGULARLY,
#                        'Job %s exit status: %s, instead of %s' %
#                        (jobid, exit_status, constants.FINISHED_REGULARLY))
#        exit_value = job_termination_status[1]
#        self.failUnless(exit_value == 1,
#                        'Job %s exit value: %d, instead of %i' %
#                        (jobid, exit_value, 1))
# checking stdout and stderr
#        client_stdout = os.path.join(self.job_examples.output_dir,
#                                     "stdout_exception_job")
#        client_stderr = os.path.join(self.job_examples.output_dir,
#                                     "stderr_exception_job")
#        self.wf_ctrl.retrieve_job_stdouterr(jobid, client_stdout,
#                                            client_stderr)
#        self.client_files.append(client_stdout)
#        self.client_files.append(client_stderr)
#
# Test stdout
#        (identical, msg) = identical_files(
#            client_stdout,
#            self.job_examples.exceptionjobstdouterr[0])
#        self.failUnless(identical, msg)
# Test the last line of stderr
#        with open(client_stderr) as f:
#            lines = f.readlines()
#        expected_error = 'Exception: Paf Boum Boum Bada Boum !!!\n'
#        isSame = (lines[-1] == expected_error)
#        self.assertTrue(isSame,
#                        "Job exception : %s. Expected : %s" %
#                        (lines[-1], expected_error))
#
# if __name__ == '__main__':
#    ExceptionJobTest.run_test(debug=True)
#    sys.exit(0)
