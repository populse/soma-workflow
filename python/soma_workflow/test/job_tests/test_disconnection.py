# -*- coding: utf-8 -*-
#"""
# Created on Mon Oct 28 15:10:19 2013
#
#@author: laure.hugo@cea.fr
#@author: Soizic Laguitton
#@organization: U{IFR 49<http://www.ifr49.org>}
#@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
# BROKEN : Doesn't work  ##########################
#"""
# import time
# import os
# import sys
#
# import soma_workflow.constants as constants
# from soma_workflow.client import WorkflowController
# from soma_workflow.test.utils import check_files
# from soma_workflow.test.job_tests.job_tests import JobTests
# from soma_workflow.configuration import LIGHT_MODE
# from soma_workflow.configuration import LOCAL_MODE
# from soma_workflow.configuration import REMOTE_MODE
#
#
# class DisconnectionTest(JobTests):
#    '''
#    Submission of a job pipeline with transfer
#    '''
#    allowed_resources = [LIGHT_MODE, LOCAL_MODE, REMOTE_MODE]
#
#    def setUp(self):
#        self.my_jobs = []
#        self.my_transfers = []
#        self.client_files = []
#        self.output_files = []
#
# Job1
#        info1 = self.job_examples.submit_job1()
#        self.my_jobs.append(info1[0])
#        self.output_files.extend(info1[1])
#
#        self.wf_ctrl.wait_job(self.my_jobs)
#        status = self.wf_ctrl.job_status(self.my_jobs[0])
#        self.failUnless(status == constants.DONE,
#                        'Job %s status after wait: %s. Expected %s' %
#                        (self.my_jobs[0], status, constants.DONE))
#        job_termination_status = self.wf_ctrl.job_termination_status(
#            self.my_jobs[0])
#        exit_status = job_termination_status[0]
#        self.failUnless(exit_status == constants.FINISHED_REGULARLY,
#                        'Job %s exit status: %s. Expected %s' %
#                        (self.my_jobs[0], exit_status,
#                         constants.FINISHED_REGULARLY))
#        exit_value = job_termination_status[1]
#        self.failUnless(exit_value == 0,
#                        'Job exit value: %d. Expected %d' % exit_value, 0)
#
# Job2 & 3
#        info2 = self.job_examples.submit_job2(time=60)
#        self.my_jobs.append(info2[0])
#        self.output_files.extend(info2[1])
#
#        info3 = self.job_examples.submit_job3(time=30)
#        self.my_jobs.append(info3[0])
#        self.output_files.extend(info3[1])
#
#        time.sleep(10)
#        sys.stdout.write("Disconnection....\n")
#        self.wf_ctrl.disconnect()
#        del self.wf_ctrl
#        time.sleep(20)
#        sys.stdout.write(".... Reconnection \n")
#
#        self.wf_ctrl = WorkflowController(self.resource_id,
#                                              self.login,
#                                              self.password)
#
#        self.job_examples.set_new_connection(self.wf_ctrl)
# time.sleep(1)
#
#    def tearDown(self):
#        super(DisconnectionTest, self).tearDown()
#        for file in self.client_files:
#            if os.path.isfile(file):
#                sys.stdout.write("remove " + file + '\n')
#                os.remove(file)
#
#    def test_result(self):
#        self.wf_ctrl.wait_job(self.my_jobs)
#        status = self.wf_ctrl.job_status(self.my_jobs[1])
#        self.failUnless(status == constants.DONE,
#                        'Job %s status after wait: %s' %
#                        (self.my_jobs[1], status))
#        job_termination_status = self.wf_ctrl.job_termination_status(
#            self.my_jobs[1])
#        exit_status = job_termination_status[0]
#        self.failUnless(exit_status == constants.FINISHED_REGULARLY,
#                        'Job %s exit status: %s' %
#                        (self.my_jobs[1], exit_status))
#        exit_value = job_termination_status[1]
#        self.failUnless(exit_value == 0, 'Job exit value: %d' % exit_value)
#        status = self.wf_ctrl.job_status(self.my_jobs[2])
#        self.failUnless(status == constants.DONE,
#                        'Job %s status after wait: %s' %
#                        (self.my_jobs[2], status))
#        job_termination_status = self.wf_ctrl.job_termination_status(
#            self.my_jobs[2])
#        exit_status = job_termination_status[0]
#        self.failUnless(exit_status == constants.FINISHED_REGULARLY,
#                        'Job %s exit status: %s' %
#                        (self.my_jobs[2], exit_status))
#        exit_value = job_termination_status[1]
#        self.failUnless(exit_value == 0, 'Job exit value: %d' % exit_value)
#
# Job 4
#        info4 = self.job_examples.submit_job4()
#        self.failUnless(not info4[0] == -1, "The job was not submitted.")
#        self.my_jobs.append(info4[0])
#        self.output_files.extend(info4[1])
#
#        jobid = self.my_jobs[len(self.my_jobs) - 1]
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
#        models = (self.job_examples.job1_output_file_models +
#                  self.job_examples.job2_output_file_models +
#                  self.job_examples.job3_output_file_models +
#                  self.job_examples.job4_output_file_models)
#        (correct, msg) = check_files(self.client_files, models)
#        self.failUnless(correct, msg)
#
# checking stdout and stderr
#        client_stdout = os.path.join(self.job_examples.output_dir,
#                                     "stdout_pipeline_job1")
#        client_stderr = os.path.join(self.job_examples.output_dir,
#                                     "stderr_pipeline_job1")
#        self.wf_ctrl.retrieve_job_stdouterr(self.my_jobs[0],
#                                            client_stdout,
#                                            client_stderr)
#        self.client_files.append(client_stdout)
#        self.client_files.append(client_stderr)
#
#        client_stdout = os.path.join(self.job_examples.output_dir,
#                                     "stdout_pipeline_job2")
#        client_stderr = os.path.join(self.job_examples.output_dir,
#                                     "stderr_pipeline_job2")
#        self.wf_ctrl.retrieve_job_stdouterr(self.my_jobs[1],
#                                            client_stdout,
#                                            client_stderr)
#        self.client_files.append(client_stdout)
#        self.client_files.append(client_stderr)
#
#        client_stdout = os.path.join(self.job_examples.output_dir,
#                                     "stdout_pipeline_job3")
#        client_stderr = os.path.join(self.job_examples.output_dir,
#                                     "stderr_pipeline_job3")
#        self.wf_ctrl.retrieve_job_stdouterr(self.my_jobs[2],
#                                            client_stdout,
#                                            client_stderr)
#        self.client_files.append(client_stdout)
#        self.client_files.append(client_stderr)
#
#        client_stdout = os.path.join(self.job_examples.output_dir,
#                                     "stdout_pipeline_job4")
#        client_stderr = os.path.join(self.job_examples.output_dir,
#                                     "stderr_pipeline_job4")
#        self.wf_ctrl.retrieve_job_stdouterr(self.my_jobs[3],
#                                            client_stdout,
#                                            client_stderr)
#        self.client_files.append(client_stdout)
#        self.client_files.append(client_stderr)
#
#        models = (self.job_examples.job1_stdouterr_models +
#                  self.job_examples.job2_stdouterr_models +
#                  self.job_examples.job3_stdouterr_models +
#                  self.job_examples.job4_stdouterr_models)
#        (correct, msg) = check_files(self.client_files[5:13], models, 1)
#        self.failUnless(correct, msg)
# pass
#
#
# if __name__ == '__main__':
#    DisconnectionTest.run_test(debug=True)
#    sys.exit(0)
