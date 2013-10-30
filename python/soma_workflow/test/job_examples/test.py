'''
@author: Soizic Laguitton
@organization: U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''
import unittest
import time
import os
import getpass
import sys
from datetime import datetime
from datetime import timedelta
from abc import abstractmethod

import soma_workflow.constants as constants
from soma_workflow.client import WorkflowController
import soma_workflow.configuration as configuration
from soma_workflow.test.utils import checkFiles
from soma_workflow.test.job_examples.job_examples import JobExamples
from soma_workflow.test.job_examples.jobs_test import JobsTest


class LocalCustomSubmission(JobsTest):
    '''
    Submission of a job using user's files only (even for stdout and stderr)
    '''
    def setUp(self):
        self.myJobs = []
        self.myTransfers = []
        info = JobsTest.jobExamples.localCustomSubmission()
        self.myJobs.append(info[0])
        self.outputFiles = info[1]
        self.stdouterrFiles = info[2]

    def tearDown(self):
        JobsTest.tearDown(self)
        for file in self.outputFiles:
            if os.path.isfile(file):
                os.remove(file)
        for file in self.stdouterrFiles:
            if os.path.isfile(file):
                os.remove(file)

    def test_result(self):
        jobid = self.myJobs[0]
        JobsTest.wf_ctrl.wait_job(self.myJobs)
        status = JobsTest.wf_ctrl.job_status(jobid)

        self.failUnless(status == constants.DONE,
                        'Job %s status after wait: %s' % (jobid, status))
        job_termination_status = JobsTest.wf_ctrl.job_termination_status(jobid)
        exitStatus = job_termination_status[0]
        self.failUnless(exitStatus == constants.FINISHED_REGULARLY,
                        'Job %s exit status: %s' % (jobid, exitStatus))
        exitValue = job_termination_status[1]
        self.failUnless(exitValue == 0,
                        'Job %s exit value: %d' % (jobid, exitValue))

        # checking output files
        for file in self.outputFiles:
            self.failUnless(os.path.isfile(file),
                            'File %s doesn t exit' % file)

        (correct, msg) = checkFiles(self.outputFiles,
                                    JobsTest.jobExamples.job1OutputFileModels)
        self.failUnless(correct, msg)

        # checking stderr and stdout files
        for file in self.stdouterrFiles:
            self.failUnless(os.path.isfile(file),
                            'File %s doesn t exit' % file)
        (correct, msg) = checkFiles(self.stdouterrFiles,
                                    JobsTest.jobExamples.job1stdouterrModels)
        self.failUnless(correct, msg)


class LocalSubmission(JobsTest):
    '''
    Submission of a job without transfer
    '''
    def setUp(self):
        self.myJobs = []
        self.myTransfers = []
        info = JobsTest.jobExamples.localSubmission()
        self.myJobs.append(info[0])
        self.outputFiles = info[1]

    def tearDown(self):
        JobsTest.tearDown(self)
        for file in self.outputFiles:
            if os.path.isfile(file):
                os.remove(file)

    def test_result(self):
        jobid = self.myJobs[0]
        JobsTest.wf_ctrl.wait_job(self.myJobs)
        status = JobsTest.wf_ctrl.job_status(jobid)
        self.failUnless(status == constants.DONE,
                        'Job %s status after wait: %s' % (jobid, status))
        job_termination_status = JobsTest.wf_ctrl.job_termination_status(jobid)
        exitStatus = job_termination_status[0]
        self.failUnless(exitStatus == constants.FINISHED_REGULARLY,
                        'Job %s exit status: %s' % (jobid, exitStatus))
        exitValue = job_termination_status[1]
        self.failUnless(exitValue == 0,
                        'Job exit value: %d' % exitValue)

        # checking output files
        for file in self.outputFiles:
            self.failUnless(os.path.isfile(file),
                            'File %s doesn t exit' % file)

        (correct, msg) = checkFiles(self.outputFiles,
                                    JobsTest.jobExamples.job1OutputFileModels)
        self.failUnless(correct, msg)

        # checking stdout and stderr
        stdout = os.path.join(JobsTest.jobExamples.output_dir,
                              "stdout_local_submission")
        stderr = os.path.join(JobsTest.jobExamples.output_dir,
                              "stderr_local_submission")
        JobsTest.wf_ctrl.retrieve_job_stdouterr(self.myJobs[0], stdout, stderr)
        self.outputFiles.append(stdout)
        self.outputFiles.append(stderr)

        (correct, msg) = checkFiles(self.outputFiles[2:5],
                                    JobsTest.jobExamples.job1stdouterrModels)
        self.failUnless(correct, msg)


class SubmissionWithTransfer(JobsTest):
    '''
    Submission of a job with transfer
    '''
    def setUp(self):
        self.myJobs = []
        self.myTransfers = []
        info = JobsTest.jobExamples.submitJob1()
        self.myJobs.append(info[0])
        self.outputFiles = info[1]
        self.clientFiles = []

    def tearDown(self):
        JobsTest.tearDown(self)
        for file in self.clientFiles:
            if os.path.isfile(file):
                os.remove(file)

    def test_result(self):
        jobid = self.myJobs[0]
        JobsTest.wf_ctrl.wait_job(self.myJobs)
        status = JobsTest.wf_ctrl.job_status(jobid)
        self.failUnless(status == constants.DONE,
                        'Job %s status after wait: %s' % (jobid, status))
        job_termination_status = JobsTest.wf_ctrl.job_termination_status(jobid)
        exitStatus = job_termination_status[0]
        self.failUnless(exitStatus == constants.FINISHED_REGULARLY,
                        'Job %s exit status: %s' % (jobid, exitStatus))
        exitValue = job_termination_status[1]
        self.failUnless(exitValue == 0,
                        'Job exit value: %d' % exitValue)

        # checking output files
        for file in self.outputFiles:
            client_file = JobsTest.wf_ctrl.transfers([file])[file][0]
            self.failUnless(client_file)
            JobsTest.wf_ctrl.transfer_files(file)
            self.failUnless(os.path.isfile(client_file),
                            'File %s doesn t exit' % file)
            self.clientFiles.append(client_file)

        (correct, msg) = checkFiles(self.clientFiles,
                                    JobsTest.jobExamples.job1OutputFileModels)
        self.failUnless(correct, msg)

        # checking stdout and stderr
        client_stdout = os.path.join(JobsTest.jobExamples.output_dir,
                                     "stdout_submit_with_transfer")
        client_stderr = os.path.join(JobsTest.jobExamples.output_dir,
                                     "stderr_submit_with_transfer")
        JobsTest.wf_ctrl.retrieve_job_stdouterr(self.myJobs[0],
                                                client_stdout,
                                                client_stderr)
        self.clientFiles.append(client_stdout)
        self.clientFiles.append(client_stderr)

        (correct, msg) = checkFiles(self.clientFiles[2:5],
                                    JobsTest.jobExamples.job1stdouterrModels)
        self.failUnless(correct, msg)


class EndedJobWithTransfer(JobsTest):
    '''
    Submission of a job with transfer
    '''
    def setUp(self):
        self.myJobs = []
        self.myTransfers = []
        info = JobsTest.jobExamples.submitJob1()
        self.myJobs.append(info[0])
        self.outputFiles = info[1]
        self.clientFiles = []

        JobsTest.wf_ctrl.wait_job(self.myJobs)

    def tearDown(self):
        JobsTest.tearDown(self)

    def test_result(self):
        self.failUnless(True)


class JobPipelineWithTransfer(JobsTest):
    '''
    Submission of a job pipeline with transfer
    '''
    def setUp(self):
        self.myJobs = []
        self.myTransfers = []
        self.clientFiles = []
        self.outputFiles = []

        # Job1
        info1 = JobsTest.jobExamples.submitJob1()
        self.myJobs.append(info1[0])
        self.outputFiles.extend(info1[1])

        JobsTest.wf_ctrl.wait_job(self.myJobs)
        status = JobsTest.wf_ctrl.job_status(self.myJobs[0])
        self.failUnless(status == constants.DONE,
                        'Job %s status after wait: %s' %
                        (self.myJobs[0], status))
        job_termination_status = JobsTest.wf_ctrl.job_termination_status(
            self.myJobs[0])
        exitStatus = job_termination_status[0]
        self.failUnless(exitStatus == constants.FINISHED_REGULARLY,
                        'Job %s exit status: %s' %
                        (self.myJobs[0], exitStatus))
        exitValue = job_termination_status[1]
        self.failUnless(exitValue == 0,
                        'Job exit value: %d' % exitValue)

        # Job2 & 3
        info2 = JobsTest.jobExamples.submitJob2()
        self.myJobs.append(info2[0])
        self.outputFiles.extend(info2[1])

        info3 = JobsTest.jobExamples.submitJob3()
        self.myJobs.append(info3[0])
        self.outputFiles.extend(info3[1])

        JobsTest.wf_ctrl.wait_job(self.myJobs)
        status = JobsTest.wf_ctrl.job_status(self.myJobs[1])
        self.failUnless(status == constants.DONE,
                        'Job %s status after wait: %s' %
                        (self.myJobs[1], status))
        job_termination_status = JobsTest.wf_ctrl.job_termination_status(
            self.myJobs[1])
        exitStatus = job_termination_status[0]
        self.failUnless(exitStatus == constants.FINISHED_REGULARLY,
                        'Job %s exit status: %s' %
                        (self.myJobs[1], exitStatus))
        exitValue = job_termination_status[1]
        self.failUnless(exitValue == 0,
                        'Job exit value: %d' % exitValue)

        status = JobsTest.wf_ctrl.job_status(self.myJobs[2])
        self.failUnless(status == constants.DONE,
                        'Job %s status after wait: %s' %
                        (self.myJobs[2], status))
        job_termination_status = JobsTest.wf_ctrl.job_termination_status(
            self.myJobs[2])
        exitStatus = job_termination_status[0]
        self.failUnless(exitStatus == constants.FINISHED_REGULARLY,
                        'Job %s exit status: %s' %
                        (self.myJobs[2], exitStatus))
        exitValue = job_termination_status[1]
        self.failUnless(exitValue == 0,
                        'Job exit value: %d' % exitValue)

        # Job 4
        info4 = JobsTest.jobExamples.submitJob4()
        self.myJobs.append(info4[0])
        self.outputFiles.extend(info4[1])

    def tearDown(self):
        JobsTest.tearDown(self)
        for file in self.clientFiles:
            if os.path.isfile(file):
                os.remove(file)

    def test_result(self):
        jobid = self.myJobs[len(self.myJobs) - 1]
        JobsTest.wf_ctrl.wait_job(self.myJobs)
        status = JobsTest.wf_ctrl.job_status(jobid)
        self.failUnless(status == constants.DONE,
                        'Job %s status after wait: %s' % (jobid, status))
        job_termination_status = JobsTest.wf_ctrl.job_termination_status(jobid)
        exitStatus = job_termination_status[0]
        self.failUnless(exitStatus == constants.FINISHED_REGULARLY,
                        'Job %s exit status: %s' % (jobid, exitStatus))
        exitValue = job_termination_status[1]
        self.failUnless(exitValue == 0,
                        'Job exit value: %d' % exitValue)

        # checking output files
        for file in self.outputFiles:
            client_file = JobsTest.wf_ctrl.transfers([file])[file][0]
            self.failUnless(client_file)
            JobsTest.wf_ctrl.transfer_files(file)
            self.failUnless(os.path.isfile(client_file),
                            'File %s doesn t exit' % file)
            self.clientFiles.append(client_file)

        models = (JobsTest.jobExamples.job1OutputFileModels +
                  JobsTest.jobExamples.job2OutputFileModels +
                  JobsTest.jobExamples.job3OutputFileModels +
                  JobsTest.jobExamples.job4OutputFileModels)
        (correct, msg) = checkFiles(self.clientFiles, models)
        self.failUnless(correct, msg)

        # checking stdout and stderr
        client_stdout = os.path.join(JobsTest.jobExamples.output_dir,
                                     "stdout_pipeline_job1")
        client_stderr = os.path.join(JobsTest.jobExamples.output_dir,
                                     "stderr_pipeline_job1")
        JobsTest.wf_ctrl.retrieve_job_stdouterr(self.myJobs[0],
                                                client_stdout,
                                                client_stderr)
        self.clientFiles.append(client_stdout)
        self.clientFiles.append(client_stderr)

        client_stdout = os.path.join(JobsTest.jobExamples.output_dir,
                                     "stdout_pipeline_job2")
        client_stderr = os.path.join(JobsTest.jobExamples.output_dir,
                                     "stderr_pipeline_job2")
        JobsTest.wf_ctrl.retrieve_job_stdouterr(self.myJobs[1],
                                                client_stdout,
                                                client_stderr)
        self.clientFiles.append(client_stdout)
        self.clientFiles.append(client_stderr)

        client_stdout = os.path.join(JobsTest.jobExamples.output_dir,
                                     "stdout_pipeline_job3")
        client_stderr = os.path.join(JobsTest.jobExamples.output_dir,
                                     "stderr_pipeline_job3")
        JobsTest.wf_ctrl.retrieve_job_stdouterr(self.myJobs[2],
                                                client_stdout,
                                                client_stderr)
        self.clientFiles.append(client_stdout)
        self.clientFiles.append(client_stderr)

        client_stdout = os.path.join(JobsTest.jobExamples.output_dir,
                                     "stdout_pipeline_job4")
        client_stderr = os.path.join(JobsTest.jobExamples.output_dir,
                                     "stderr_pipeline_job4")
        JobsTest.wf_ctrl.retrieve_job_stdouterr(self.myJobs[3],
                                                client_stdout,
                                                client_stderr)
        self.clientFiles.append(client_stdout)
        self.clientFiles.append(client_stderr)

        models = (JobsTest.jobExamples.job1stdouterrModels +
                  JobsTest.jobExamples.job2stdouterrModels +
                  JobsTest.jobExamples.job3stdouterrModels +
                  JobsTest.jobExamples.job4stdouterrModels)
        (correct, msg) = checkFiles(self.clientFiles[5:13], models)
        self.failUnless(correct, msg)


class ExceptionJobTest(JobsTest):
    '''
    Submission of a job raising an exception
    '''
    def setUp(self):
        self.myJobs = []
        self.myTransfers = []
        info = JobsTest.jobExamples.submitExceptionJob()
        self.myJobs.append(info[0])
        self.clientFiles = []

    def tearDown(self):
        JobsTest.tearDown(self)
        for file in self.clientFiles:
            if os.path.isfile(file):
                os.remove(file)

    def test_result(self):
        jobid = self.myJobs[0]
        JobsTest.wf_ctrl.wait_job(self.myJobs)
        status = JobsTest.wf_ctrl.job_status(jobid)
        self.failUnless(status == constants.DONE or constants.FAILED,
                        'Job %s status after wait: %s' % (jobid, status))
        job_termination_status = JobsTest.wf_ctrl.job_termination_status(jobid)
        exitStatus = job_termination_status[0]
        self.failUnless(exitStatus == constants.FINISHED_REGULARLY,
                        'Job %s exit status: %s' % (jobid, exitStatus))
        exitValue = job_termination_status[1]
        self.failUnless(exitValue == 1,
                        'Job exit value: %d' % exitValue)
        # checking stdout and stderr
        client_stdout = os.path.join(JobsTest.jobExamples.output_dir,
                                     "stdout_exception_job")
        client_stderr = os.path.join(JobsTest.jobExamples.output_dir,
                                     "stderr_exception_job")
        JobsTest.wf_ctrl.retrieve_job_stdouterr(jobid, client_stdout,
                                                client_stderr)
        self.clientFiles.append(client_stdout)
        self.clientFiles.append(client_stderr)

        (identical, msg) = checkFiles(
            self.clientFiles,
            JobsTest.jobExamples.exceptionjobstdouterr, 1)
        self.failUnless(identical, msg)


class DisconnectionTest(JobsTest):

    '''
    Submission of a job pipeline with transfer
    '''

    def setUp(self):
        self.myJobs = []
        self.myTransfers = []
        self.clientFiles = []
        self.outputFiles = []

        # Job1
        info1 = JobsTest.jobExamples.submitJob1()
        self.myJobs.append(info1[0])
        self.outputFiles.extend(info1[1])

        JobsTest.wf_ctrl.wait_job(self.myJobs)
        status = JobsTest.wf_ctrl.job_status(self.myJobs[0])
        self.failUnless(status == constants.DONE,
                        'Job %s status after wait: %s' %
                        (self.myJobs[0], status))
        job_termination_status = JobsTest.wf_ctrl.job_termination_status(
            self.myJobs[0])
        exitStatus = job_termination_status[0]
        self.failUnless(exitStatus == constants.FINISHED_REGULARLY,
                        'Job %s exit status: %s' %
                        (self.myJobs[0], exitStatus))
        exitValue = job_termination_status[1]
        self.failUnless(exitValue == 0,
                        'Job exit value: %d' % exitValue)

        # Job2 & 3
        info2 = JobsTest.jobExamples.submitJob2(time=60)
        self.myJobs.append(info2[0])
        self.outputFiles.extend(info2[1])

        info3 = JobsTest.jobExamples.submitJob3(time=30)
        self.myJobs.append(info3[0])
        self.outputFiles.extend(info3[1])

        time.sleep(10)
        print "Disconnection...."
        JobsTest.wf_ctrl.disconnect()
        del JobsTest.wf_ctrl
        time.sleep(20)
        print ".... Reconnection"

        JobsTest.wf_ctrl = WorkflowController(JobsTest.resource_id,
                                              JobsTest.login,
                                              JobsTest.password)

        JobsTest.jobExamples.setNewConnection(JobsTest.wf_ctrl)
        # time.sleep(1)

    def tearDown(self):
        # pass
        JobsTest.tearDown(self)
        for file in self.clientFiles:
            if os.path.isfile(file):
                print "remove " + file
                os.remove(file)

    def test_result(self):
        JobsTest.wf_ctrl.wait_job(self.myJobs)
        status = JobsTest.wf_ctrl.job_status(self.myJobs[1])
        self.failUnless(status == constants.DONE,
                        'Job %s status after wait: %s' %
                        (self.myJobs[1], status))
        job_termination_status = JobsTest.wf_ctrl.job_termination_status(
            self.myJobs[1])
        exitStatus = job_termination_status[0]
        self.failUnless(exitStatus == constants.FINISHED_REGULARLY,
                        'Job %s exit status: %s' %
                        (self.myJobs[1], exitStatus))
        exitValue = job_termination_status[1]
        self.failUnless(exitValue == 0,
                        'Job exit value: %d' % exitValue)

        status = JobsTest.wf_ctrl.job_status(self.myJobs[2])
        self.failUnless(status == constants.DONE,
                        'Job %s status after wait: %s' %
                        (self.myJobs[2], status))
        job_termination_status = JobsTest.wf_ctrl.job_termination_status(
            self.myJobs[2])
        exitStatus = job_termination_status[0]
        self.failUnless(exitStatus == constants.FINISHED_REGULARLY,
                        'Job %s exit status: %s' %
                        (self.myJobs[2], exitStatus))
        exitValue = job_termination_status[1]
        self.failUnless(exitValue == 0,
                        'Job exit value: %d' % exitValue)

        # Job 4
        info4 = JobsTest.jobExamples.submitJob4()
        self.failUnless(not info4[0] == -1, "The job was not submitted.")
        self.myJobs.append(info4[0])
        self.outputFiles.extend(info4[1])

        jobid = self.myJobs[len(self.myJobs) - 1]
        JobsTest.wf_ctrl.wait_job(self.myJobs)
        status = JobsTest.wf_ctrl.job_status(jobid)
        self.failUnless(status == constants.DONE,
                        'Job %s status after wait: %s' % (jobid, status))
        job_termination_status = JobsTest.wf_ctrl.job_termination_status(jobid)
        exitStatus = job_termination_status[0]
        self.failUnless(exitStatus == constants.FINISHED_REGULARLY,
                        'Job %s exit status: %s' % (jobid, exitStatus))
        exitValue = job_termination_status[1]
        self.failUnless(exitValue == 0,
                        'Job exit value: %d' % exitValue)

        # checking output files
        for file in self.outputFiles:
            client_file = JobsTest.wf_ctrl.transfers([file])[file][0]
            self.failUnless(client_file)
            JobsTest.wf_ctrl.transfer_files(file)
            self.failUnless(os.path.isfile(client_file),
                            'File %s doesn t exit' % file)
            self.clientFiles.append(client_file)

        models = (JobsTest.jobExamples.job1OutputFileModels +
                  JobsTest.jobExamples.job2OutputFileModels +
                  JobsTest.jobExamples.job3OutputFileModels +
                  JobsTest.jobExamples.job4OutputFileModels)
        (correct, msg) = checkFiles(self.clientFiles, models)
        self.failUnless(correct, msg)

        # checking stdout and stderr
        client_stdout = os.path.join(JobsTest.jobExamples.output_dir,
                                     "stdout_pipeline_job1")
        client_stderr = os.path.join(JobsTest.jobExamples.output_dir,
                                     "stderr_pipeline_job1")
        JobsTest.wf_ctrl.retrieve_job_stdouterr(self.myJobs[0],
                                                client_stdout,
                                                client_stderr)
        self.clientFiles.append(client_stdout)
        self.clientFiles.append(client_stderr)

        client_stdout = os.path.join(JobsTest.jobExamples.output_dir,
                                     "stdout_pipeline_job2")
        client_stderr = os.path.join(JobsTest.jobExamples.output_dir,
                                     "stderr_pipeline_job2")
        JobsTest.wf_ctrl.retrieve_job_stdouterr(self.myJobs[1],
                                                client_stdout,
                                                client_stderr)
        self.clientFiles.append(client_stdout)
        self.clientFiles.append(client_stderr)

        client_stdout = os.path.join(JobsTest.jobExamples.output_dir,
                                     "stdout_pipeline_job3")
        client_stderr = os.path.join(JobsTest.jobExamples.output_dir,
                                     "stderr_pipeline_job3")
        JobsTest.wf_ctrl.retrieve_job_stdouterr(self.myJobs[2],
                                                client_stdout,
                                                client_stderr)
        self.clientFiles.append(client_stdout)
        self.clientFiles.append(client_stderr)

        client_stdout = os.path.join(JobsTest.jobExamples.output_dir,
                                     "stdout_pipeline_job4")
        client_stderr = os.path.join(JobsTest.jobExamples.output_dir,
                                     "stderr_pipeline_job4")
        JobsTest.wf_ctrl.retrieve_job_stdouterr(self.myJobs[3],
                                                client_stdout,
                                                client_stderr)
        self.clientFiles.append(client_stdout)
        self.clientFiles.append(client_stderr)

        models = (JobsTest.jobExamples.job1stdouterrModels +
                  JobsTest.jobExamples.job2stdouterrModels +
                  JobsTest.jobExamples.job3stdouterrModels +
                  JobsTest.jobExamples.job4stdouterrModels)
        (correct, msg) = checkFiles(self.clientFiles[5:13], models, 1)
        self.failUnless(correct, msg)
        # pass


#class MPIParallelJobTest(JobsTest):
#    '''
#    Submission of a parallel job (MPI)
#    '''
#    def setUp(self):
#        self.myJobs = []
#        self.myTransfers = []
#        self.node_num = 4
#        info = JobsTest.jobExamples.mpiJobSubmission(node_num=self.node_num)
#        self.myJobs.append(info[0])
#        self.outputFiles = info[1]
#
#    def tearDown(self):
#        JobsTest.tearDown(self)
#        # for file in self.outputFiles:
#            #if os.path.isfile(file): os.remove(file)
#
#    def test_result(self):
#        jobid = self.myJobs[0]
#        JobsTest.wf_ctrl.wait_job(self.myJobs)
#
#        status = JobsTest.wf_ctrl.job_status(jobid)
#        self.failUnless(status == constants.DONE,
#                        'Job %s status after wait: %s' % (jobid, status))
#        job_termination_status = JobsTest.wf_ctrl.job_termination_status(jobid)
#        exitStatus = job_termination_status[0]
#        self.failUnless(exitStatus == constants.FINISHED_REGULARLY,
#                        'Job %s exit status: %s' % (jobid, exitStatus))
#        exitValue = job_termination_status[1]
#        self.failUnless(exitValue == 0,
#                        'Job exit value: %d' % exitValue)
#
#        print "stdout: "
#        line = JobsTest.wf_ctrl.stdoutReadLine(jobid)
#        process_num = 1
#        while line:
#            splitted_line = line.split()
#            if splitted_line[0] == "Grettings":
#                self.failUnless(line.rstrip() == "Grettings from process %d!" %
#                                (process_num),
#                                "stdout line:  %sinstead of  : "
#                                "'Grettings from process %d!'" %
#                                (line, process_num))
#                process_num = process_num + 1
#            line = JobsTest.wf_ctrl.stdoutReadLine(jobid)
#
#        self.failUnless(process_num == self.node_num,
#                        "%d process(es) run instead of %d." %
#                        (process_num - 1, self.node_num))


if __name__ == '__main__':
    sys.stdout.write("----- soma-workflow tests: JOBS -------------\n")

    config_file_path = configuration.Configuration.search_config_path()
    sys.stdout.write("Configuration file: " + config_file_path)
    resource_ids = configuration.Configuration.get_configured_resources(
        config_file_path)

    # Resource
    sys.stdout.write("Configured resources:\n")
    for i in range(0, len(resource_ids)):
        sys.stdout.write("  " + repr(i) + " -> " +
                         repr(resource_ids[i]) + "\n")
    sys.stdout.write("Select a resource number: ")
    resource_index = int(sys.stdin.readline())
    resource_id = resource_ids[resource_index]
    sys.stdout.write("Selected resource => " + repr(resource_id) + "\n")
    sys.stdout.write("---------------------------------\n")
    login = None
    password = None

    config = configuration.Configuration.load_from_file(resource_id,
                                                        config_file_path)

    if config.get_mode() == 'remote':
        sys.stdout.write("This is a remote connection\n")
        sys.stdout.write("login:")
        login = sys.stdin.readline()
        login = login.rstrip()
        sys.stdout.write("password:")
        password = sys.stdin.readline()
        password = login.rstrip()
    sys.stdout.write("Login => " + repr(login) + "\n")
    sys.stdout.write("---------------------------------\n")

    # Job type
    job_types = ["LocalCustomSubmission",
                 "LocalSubmission",
                 "SubmissionWithTransfer",
                 "ExceptionJobTest",
                 "JobPipelineWithTransfer",
                 "DisconnectionTest",
                 "EndedJobWithTransfer",
#                 "MPIParallelJobTest"
                 ]
    sys.stdout.write("Jobs example to test: \n")
    sys.stdout.write("all -> all \n")
    for i in range(0, len(job_types)):
        sys.stdout.write("  " + repr(i) + " -> " + repr(job_types[i]) + "\n")
    sys.stdout.write("Select one or several job type : \n")
    selected_job_type_indexes = []
    line = sys.stdin.readline()
    line = line.rstrip()
    if line == "all":
        selected_job_type = job_types
        sys.stdout.write("Selected job types: all \n")
    else:
        for strindex in line.split(" "):
            selected_job_type_indexes.append(int(strindex))
        selected_job_type = []
        sys.stdout.write("Selected job types: \n")
        for job_type_index in selected_job_type_indexes:
            selected_job_type.append(job_types[int(job_type_index)])
            sys.stdout.write("  => " + repr(job_types[int(job_type_index)]) +
                             "\n")

    # Test type
    sys.stdout.write("---------------------------------\n")
    test_types = ["test_result",
                  "test_jobs",
                  "test_wait",
                  "test_wait2",
                  "test_kill",
                  "test_restart"
                  ]
    sys.stdout.write("Tests to perform: \n")
    sys.stdout.write("all -> all \n")
    for i in range(0, len(test_types)):
        sys.stdout.write("  " + repr(i) + " -> " + repr(test_types[i]) + "\n")
    sys.stdout.write("Select one or several test : \n")
    selected_test_type_indexes = []
    line = sys.stdin.readline()
    line = line.rstrip()
    if line == "all":
        selected_test_type = test_types
        sys.stdout.write("Selected test types: all \n")
    else:
        for strindex in line.split(" "):
            selected_test_type_indexes.append(int(strindex))
        selected_test_type = []
        sys.stdout.write("Selected test types: \n")
        for test_type_index in selected_test_type_indexes:
            selected_test_type.append(test_types[int(test_type_index)])
            sys.stdout.write("  => " + repr(test_types[int(test_type_index)]) +
                             "\n")
    sys.stdout.write("---------------------------------\n")

    JobsTest.setupConnection(resource_id,
                             login,
                             password)
    suite_list = []
    tests = selected_test_type
    if "LocalCustomSubmission" in selected_job_type:
        suite_list.append(
            unittest.TestSuite(map(LocalCustomSubmission, tests)))
    if "LocalSubmission" in selected_job_type:
        print "tests = " + repr(tests)
        suite_list.append(unittest.TestSuite(map(LocalSubmission, tests)))
    if "SubmissionWithTransfer" in selected_job_type:
        suite_list.append(
            unittest.TestSuite(map(SubmissionWithTransfer, tests)))
    if "ExceptionJobTest" in selected_job_type:
        suite_list.append(unittest.TestSuite(map(ExceptionJobTest, tests)))
    if "JobPipelineWithTransfer" in selected_job_type:
        suite_list.append(
            unittest.TestSuite(map(JobPipelineWithTransfer, tests)))
    if "DisconnectionTest" in selected_job_type:
        suite_list.append(unittest.TestSuite(map(DisconnectionTest, tests)))
    if "EndedJobWithTransfer" in selected_job_type:
        suite_list.append(unittest.TestSuite(map(EndedJobWithTransfer, tests)))
#    if "MPIParallelJobTest" in selected_job_type:
#        suite_list.append(unittest.TestSuite(map(MPIParallelJobTest, tests)))

    alltests = unittest.TestSuite(suite_list)
    unittest.TextTestRunner(verbosity=2).run(alltests)

    sys.exit(0)
