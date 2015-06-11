# -*- coding: utf-8 -*-
#"""
# Created on Mon Oct 28 15:02:25 2013
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
# class LocalSubmissionTest(JobTests):
#    '''
#    Submission of a job without transfer
#    '''
#    allowed_resources = [LIGHT_MODE, LOCAL_MODE]
#
#    def setUp(self):
#        self.my_jobs = []
#        self.my_transfers = []
#        info = self.job_examples.local_submission()
#        self.my_jobs.append(info[0])
#        self.output_files = info[1]
#
#    def tearDown(self):
#        super(LocalSubmissionTest, self).tearDown()
#        for file in self.output_files:
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
#            self.failUnless(os.path.isfile(file),
#                            'File %s doesn t exit' % file)
#
#        (correct, msg) = check_files(
#            self.output_files,
#            self.job_examples.job1_output_file_models)
#        self.failUnless(correct, msg)
#
# checking stdout and stderr
#        stdout = os.path.join(self.job_examples.output_dir,
#                              "stdout_local_submission")
#        stderr = os.path.join(self.job_examples.output_dir,
#                              "stderr_local_submission")
#        self.wf_ctrl.retrieve_job_stdouterr(self.my_jobs[0],
#                                            stdout,
#                                            stderr)
#        self.output_files.append(stdout)
#        self.output_files.append(stderr)
#
#        (correct, msg) = check_files(
#            self.output_files[2:5],
#            self.job_examples.job1_stdouterr_models)
#        self.failUnless(correct, msg)
#
#
# if __name__ == '__main__':
#    LocalSubmissionTest.run_test(debug=False)
#    sys.exit(0)
