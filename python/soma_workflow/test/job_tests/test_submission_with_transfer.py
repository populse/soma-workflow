# -*- coding: utf-8 -*-
#"""
# Created on Mon Oct 28 15:04:47 2013
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
# from soma_workflow.test.utils import check_files
# from soma_workflow.test.job_tests.job_tests import JobTests
# from soma_workflow.configuration import LIGHT_MODE
# from soma_workflow.configuration import LOCAL_MODE
# from soma_workflow.configuration import REMOTE_MODE
#
#
# class SubmissionWithTransferTest(JobTests):
#    '''
#    Submission of a job with transfer
#    '''
#    allowed_resources = [LIGHT_MODE, LOCAL_MODE, REMOTE_MODE]
#
#    def setUp(self):
#        self.my_jobs = []
#        self.my_transfers = []
#        info = self.job_examples.submit_job1()
#        self.my_jobs.append(info[0])
#        self.output_files = info[1]
#        self.client_files = []
#
#    def tearDown(self):
#        super(SubmissionWithTransferTest, self).tearDown()
#        for file in self.client_files:
#            if os.path.isfile(file):
#                os.remove(file)
#
#    def test_result(self):
#        jobid = self.my_jobs[0]
#        self.wf_ctrl.wait_job(self.my_jobs)
#        status = self.wf_ctrl.job_status(jobid)
#        self.failUnless(status == constants.DONE,
#                        'Job %s status after wait: %s' % (jobid, status))
#        job_termination_status = self.wf_ctrl.job_termination_status(jobid)
#        exit_status = job_termination_status[0]
#        self.failUnless(exit_status == constants.FINISHED_REGULARLY,
#                        'Job %s exit status: %s' % (jobid, exit_status))
#        exit_value = job_termination_status[1]
#        self.failUnless(exit_value == 0,
#                        'Job exit value: %d' % exit_value)
#
# checking output files
#        for file in self.output_files:
#            client_file = self.wf_ctrl.transfers([file])[file][0]
#            self.failUnless(client_file)
#            self.wf_ctrl.transfer_files(file)
#            self.failUnless(os.path.isfile(client_file),
#                            'File %s doesn t exit' % file)
#            self.client_files.append(client_file)
#
#        (correct, msg) = check_files(
#            self.client_files,
#            self.job_examples.job1_output_file_models)
#        self.failUnless(correct, msg)
#
# checking stdout and stderr
#        client_stdout = os.path.join(self.job_examples.output_dir,
#                                     "stdout_submit_with_transfer")
#        client_stderr = os.path.join(self.job_examples.output_dir,
#                                     "stderr_submit_with_transfer")
#        self.wf_ctrl.retrieve_job_stdouterr(self.my_jobs[0],
#                                            client_stdout,
#                                            client_stderr)
#        self.client_files.append(client_stdout)
#        self.client_files.append(client_stderr)
#
#        (correct, msg) = check_files(
#            self.client_files[2:5],
#            self.job_examples.job1_stdouterr_models)
#        self.failUnless(correct, msg)
#
#
# if __name__ == '__main__':
#    SubmissionWithTransferTest.run_test(debug=False)
#    sys.exit(0)
