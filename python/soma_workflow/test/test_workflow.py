from __future__ import with_statement

'''
@author: Soizic Laguitton
@organization: U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''
#-----------------------------------------------------------------------------
# Imports
#-----------------------------------------------------------------------------

import unittest
import sys
import inspect


from soma_workflow.client import WorkflowController
from soma_workflow.client import Helper
from soma_workflow.configuration import Configuration
from soma_workflow.configuration import LIGHT_MODE
from soma_workflow.configuration import REMOTE_MODE
from soma_workflow.configuration import LOCAL_MODE
import soma_workflow.constants as constants
from soma_workflow.utils import identicalFiles


from soma_workflow.test.workflow_local import WorkflowExamplesLocal
from soma_workflow.test.workflow_shared import WorkflowExamplesShared
#from soma_workflow.test.workflow_shared_transfer import WorkflowExamplesSharedTransfer
from soma_workflow.test.workflow_transfer import WorkflowExamplesTransfer

from soma_workflow.test.utils import get_user_id
from soma_workflow.test.utils import suppress_stdout


#-----------------------------------------------------------------------------
# Classes and functions
#-----------------------------------------------------------------------------

class WorkflowTest(unittest.TestCase):

    LOCAL_PATH = "local path"
    FILE_TRANSFER = "file transfer"
    SHARED_RESOURCE_PATH = "shared resource path"

    allowed_config = [(LIGHT_MODE, LOCAL_PATH),
                      (LOCAL_MODE, LOCAL_PATH),
                      (REMOTE_MODE, FILE_TRANSFER),
#                      (REMOTE_MODE, SHARED_RESOURCE_PATH)
                      ]
    # Redefine allowed_file_systems in each testclass depending on the test
    allowed_file_systems = []

    wf_ctrl = None
    path_management = None
    wf_examples = None
    wf_id = None

    @classmethod
    def setup_wf_controller(cls, workflow_controller):
        cls.wf_ctrl = workflow_controller

    @classmethod
    def setup_path_management(cls, path_management):
        '''
        * path_management: LOCAL_PATH, FILE_TRANSFER or SHARED_RESOURCE_PATH
        '''
        cls.path_management = path_management

    def setUp(self):
        if WorkflowTest.path_management == WorkflowTest.LOCAL_PATH:
            workflow_examples = WorkflowExamplesLocal()
        elif WorkflowTest.path_management == WorkflowTest.FILE_TRANSFER:
            workflow_examples = WorkflowExamplesTransfer()
        elif WorkflowTest.path_management == WorkflowTest.SHARED_RESOURCE_PATH:
            workflow_examples = WorkflowExamplesShared()
        WorkflowTest.wf_examples = workflow_examples
        #raise Exception("WorkflowTest is an abstract class.")

    def tearDown(self):
        if self.wf_id:
            WorkflowTest.wf_ctrl.delete_workflow(self.wf_id)


class SpecialTransferTest(WorkflowTest):
    allowed_file_systems = [WorkflowTest.FILE_TRANSFER,
                            WorkflowTest.SHARED_RESOURCE_PATH]

    def test_result(self):
        workflow = WorkflowTest.wf_examples.example_special_transfer()
        self.wf_id = WorkflowTest.wf_ctrl.submit_workflow(
            workflow=workflow,
            name=self.__class__.__name__)

        # Transfer input files if file transfer
        if self.path_management == WorkflowTest.FILE_TRANSFER:
            Helper.transfer_input_files(self.wf_id, WorkflowTest.wf_ctrl)
        # Wait for the worklow to finish
        Helper.wait_workflow(self.wf_id, WorkflowTest.wf_ctrl)
        status = self.wf_ctrl.workflow_status(self.wf_id)
        # Transfer output files if file transfer
        if self.path_management == WorkflowTest.FILE_TRANSFER:
            Helper.transfer_output_files(self.wf_id, WorkflowTest.wf_ctrl)

        self.assertTrue(status == constants.WORKFLOW_DONE)
        self.assertTrue(len(Helper.list_failed_jobs(
            self.wf_id,
            WorkflowTest.wf_ctrl)) == 0)
        self.assertTrue(len(Helper.list_failed_jobs(
            self.wf_id,
            WorkflowTest.wf_ctrl,
            include_aborted_jobs=True)) == 0)
        # TODO: check the stdout


class SpecialCommandTest(WorkflowTest):
    allowed_file_systems = [WorkflowTest.LOCAL_PATH,
                            WorkflowTest.SHARED_RESOURCE_PATH,
                            WorkflowTest.FILE_TRANSFER]

    def test_result(self):
        workflow = WorkflowTest.wf_examples.example_special_command()
        self.wf_id = WorkflowTest.wf_ctrl.submit_workflow(
            workflow=workflow,
            name=self.__class__.__name__)

        # Transfer input files if file transfer
        if self.path_management == WorkflowTest.FILE_TRANSFER:
            Helper.transfer_input_files(self.wf_id, WorkflowTest.wf_ctrl)

        # Wait for the worklow to finish
        Helper.wait_workflow(self.wf_id, WorkflowTest.wf_ctrl)
        status = self.wf_ctrl.workflow_status(self.wf_id)

        self.assertTrue(status == constants.WORKFLOW_DONE)
        self.assertTrue(len(Helper.list_failed_jobs(
            self.wf_id,
            WorkflowTest.wf_ctrl)) == 0)
        self.assertTrue(len(Helper.list_failed_jobs(
            self.wf_id,
            WorkflowTest.wf_ctrl,
            include_aborted_jobs=True)) == 0)
        # TODO: check the stdout


class SimpleTest(WorkflowTest):
    allowed_file_systems = [WorkflowTest.LOCAL_PATH,
                            WorkflowTest.SHARED_RESOURCE_PATH,
                            WorkflowTest.FILE_TRANSFER]

    def test_result(self):
        workflow = WorkflowTest.wf_examples.example_simple()
        self.wf_id = WorkflowTest.wf_ctrl.submit_workflow(
            workflow=workflow,
            name=self.__class__.__name__)
        # Transfer input files if file transfer
        if self.path_management == WorkflowTest.FILE_TRANSFER:
            Helper.transfer_input_files(self.wf_id, WorkflowTest.wf_ctrl)
        # Wait for the workflow to finish
        Helper.wait_workflow(self.wf_id, WorkflowTest.wf_ctrl)
        # Transfer output files if file transfer
        if self.path_management == WorkflowTest.FILE_TRANSFER:
            Helper.transfer_output_files(self.wf_id, WorkflowTest.wf_ctrl)

        status = self.wf_ctrl.workflow_status(self.wf_id)
        self.assertTrue(status == constants.WORKFLOW_DONE)
        self.assertTrue(len(Helper.list_failed_jobs(
                        self.wf_id,
                        WorkflowTest.wf_ctrl)) == 0)
        self.assertTrue(len(Helper.list_failed_jobs(
                        self.wf_id,
                        WorkflowTest.wf_ctrl,
                        include_aborted_jobs=True)) == 0)

        (jobs_info, transfers_info, workflow_status, workflow_queue) = \
            WorkflowTest.wf_ctrl.workflow_elements_status(self.wf_id)
#    print "len(jobs_info)=" + repr(len(jobs_info)) + "\n"

        # TODO: check the stdout and stderrr
        for (job_id, tmp_status, queue, exit_info, dates) in jobs_info:
            job_list = self.wf_ctrl.jobs([job_id])
            job_name, job_command, job_submission_date = job_list[job_id]

            if exit_info[0] == constants.FINISHED_REGULARLY:
                # To check job standard out
                job_stdout_file = "/tmp/job_soma_out_log_" + repr(job_id)
                job_stderr_file = "/tmp/job_soma_outerr_log_" + repr(job_id)
                self.wf_ctrl.retrieve_job_stdouterr(job_id,
                                                    job_stdout_file,
                                                    job_stderr_file)
                if job_name == 'job1':
                    isSame, msg = identicalFiles(
                        job_stdout_file,
                        WorkflowTest.wf_examples.lo_stdout[1])
                    self.assertTrue(isSame)
                    if self.path_management == WorkflowTest.LOCAL_PATH:
                        isSame, msg = identicalFiles(
                            WorkflowTest.wf_examples.lo_out_model_file[11],
                            WorkflowTest.wf_examples.lo_file[11])
                        self.assertTrue(isSame)
                        isSame, msg = identicalFiles(
                            WorkflowTest.wf_examples.lo_out_model_file[12],
                            WorkflowTest.wf_examples.lo_file[12])
                        self.assertTrue(isSame)
                        isSame, msg = identicalFiles(
                            job_stderr_file,
                            WorkflowTest.wf_examples.lo_stderr[1])
                        self.assertTrue(isSame)
                    if self.path_management == WorkflowTest.FILE_TRANSFER:
                        isSame, msg = identicalFiles(
                            WorkflowTest.wf_examples.lo_out_model_file[11],
                            WorkflowTest.wf_examples.tr_file[11].client_path)
                        self.assertTrue(isSame)
                        isSame, msg = identicalFiles(
                            WorkflowTest.wf_examples.lo_out_model_file[12],
                            WorkflowTest.wf_examples.tr_file[12].client_path)
                        self.assertTrue(isSame)
                        # For unknown reason, it raises some errors
                        # http://stackoverflow.com/questions/10496758/unexpected-end-of-file-and-error-importing-function-definition-error-running
                        #isSame,	msg	= identicalFiles(job_stderr_file,WorkflowTest.wf_examples.lo_stderr[1])
                        #self.failUnless(isSame == True)

                if job_name in ['job2', 'job3', 'job4']:
                    job_nb = int(job_name[3])
                    isSame, msg = identicalFiles(
                        job_stdout_file,
                        WorkflowTest.wf_examples.lo_stdout[job_nb])
                    self.assertTrue(isSame)
                    if self.path_management == WorkflowTest.LOCAL_PATH:
                        isSame, msg = identicalFiles(
                            WorkflowTest.wf_examples.lo_out_model_file[job_nb],
                            WorkflowTest.wf_examples.lo_file[job_nb])
                        self.assertTrue(isSame)
                        isSame, msg = identicalFiles(
                            job_stderr_file,
                            WorkflowTest.wf_examples.lo_stderr[job_nb])
                        self.assertTrue(isSame)
                    if self.path_management == WorkflowTest.FILE_TRANSFER:
                        isSame, msg = identicalFiles(
                            WorkflowTest.wf_examples.lo_out_model_file[job_nb],
                            WorkflowTest.wf_examples.tr_file[job_nb].client_path)
                        self.assertTrue(isSame)


class WrongNativeSpecPbsTest(WorkflowTest):
    allowed_file_systems = [WorkflowTest.LOCAL_PATH,
                            WorkflowTest.SHARED_RESOURCE_PATH,
                            WorkflowTest.FILE_TRANSFER]

    def test_result(self):
        workflow = WorkflowTest.wf_examples.example_wrong_native_spec_pbs()
        self.wf_id = WorkflowTest.wf_ctrl.submit_workflow(
            workflow=workflow,
            name=self.__class__.__name__)
        # Transfer input files if file transfer
        if self.path_management == WorkflowTest.FILE_TRANSFER:
            Helper.transfer_input_files(self.wf_id, WorkflowTest.wf_ctrl)
        # Wait for the workflow to finish
        Helper.wait_workflow(self.wf_id, WorkflowTest.wf_ctrl)
        # Transfer output files if file transfer
        if self.path_management == WorkflowTest.FILE_TRANSFER:
            Helper.transfer_output_files(self.wf_id, WorkflowTest.wf_ctrl)

        status = self.wf_ctrl.workflow_status(self.wf_id)
        self.assertTrue(status == constants.WORKFLOW_DONE)
        self.assertTrue(len(Helper.list_failed_jobs(
                        self.wf_id,
                        WorkflowTest.wf_ctrl)) == 0)
        if self.path_management == WorkflowTest.FILE_TRANSFER:
            self.assertTrue(len(Helper.list_failed_jobs(
                            self.wf_id,
                            WorkflowTest.wf_ctrl,
                            include_aborted_jobs=True)) == 1)
        else:
            self.assertTrue(len(Helper.list_failed_jobs(
                            self.wf_id,
                            WorkflowTest.wf_ctrl,
                            include_aborted_jobs=True)) == 0)


class NativeSpecPbsTest(WorkflowTest):
    allowed_file_systems = [WorkflowTest.LOCAL_PATH,
                            WorkflowTest.SHARED_RESOURCE_PATH,
                            WorkflowTest.FILE_TRANSFER]

    def test_result(self):
        workflow = WorkflowTest.wf_examples.example_native_spec_pbs()
        self.wf_id = WorkflowTest.wf_ctrl.submit_workflow(
            workflow=workflow,
            name=self.__class__.__name__)
        # Transfer input files if file transfer
        if self.path_management == WorkflowTest.FILE_TRANSFER:
            Helper.transfer_input_files(self.wf_id, WorkflowTest.wf_ctrl)
        # Wait for the workflow to finish
        Helper.wait_workflow(self.wf_id, WorkflowTest.wf_ctrl)
        # Transfer output files if file transfer
        if self.path_management == WorkflowTest.FILE_TRANSFER:
            Helper.transfer_output_files(self.wf_id, WorkflowTest.wf_ctrl)

        status = self.wf_ctrl.workflow_status(self.wf_id)
        self.assertTrue(status == constants.WORKFLOW_DONE)
        print "len failed jobs: ", len(Helper.list_failed_jobs(
                                       self.wf_id,
                                       WorkflowTest.wf_ctrl,
                                       include_aborted_jobs=True))
        self.assertTrue(len(Helper.list_failed_jobs(
                        self.wf_id,
                        WorkflowTest.wf_ctrl,
                        include_aborted_jobs=True)) == 0)


class SimpleException1Test(WorkflowTest):
    allowed_file_systems = [WorkflowTest.LOCAL_PATH,
                            WorkflowTest.SHARED_RESOURCE_PATH,
                            WorkflowTest.FILE_TRANSFER]

    def test_result(self):
        workflow = WorkflowTest.wf_examples.example_simple_exception1()
        self.wf_id = WorkflowTest.wf_ctrl.submit_workflow(
            workflow=workflow,
            name=self.__class__.__name__)
        # Transfer input files if file transfer
        if self.path_management == WorkflowTest.FILE_TRANSFER:
            Helper.transfer_input_files(self.wf_id, WorkflowTest.wf_ctrl)
        # Wait for the workflow to finish
        Helper.wait_workflow(self.wf_id, WorkflowTest.wf_ctrl)
        # Transfer output files if file transfer
        if self.path_management == WorkflowTest.FILE_TRANSFER:
            Helper.transfer_output_files(self.wf_id, WorkflowTest.wf_ctrl)

        status = self.wf_ctrl.workflow_status(self.wf_id)
        self.assertTrue(status == constants.WORKFLOW_DONE)
        self.assertTrue(len(Helper.list_failed_jobs(
                        self.wf_id,
                        WorkflowTest.wf_ctrl)) == 1)
        self.assertTrue(len(Helper.list_failed_jobs(
                        self.wf_id,
                        WorkflowTest.wf_ctrl,
                        include_aborted_jobs=True)) == 4)

        (jobs_info, transfers_info, workflow_status, workflow_queue) = \
            WorkflowTest.wf_ctrl.workflow_elements_status(self.wf_id)

        for (job_id, tmp_status, queue, exit_info, dates) in jobs_info:
            job_list = self.wf_ctrl.jobs([job_id])
            job_name, job_command, job_submission_date = job_list[job_id]

            if exit_info[0] == constants.FINISHED_REGULARLY:
                # To check job standard out
                job_stdout_file = "/tmp/job_soma_out_log_" + repr(job_id)
                job_stderr_file = "/tmp/job_soma_outerr_log_" + repr(job_id)
                self.wf_ctrl.retrieve_job_stdouterr(job_id,
                                                    job_stdout_file,
                                                    job_stderr_file)

                if job_name in ['job2', 'job3', 'job4']:
                    job_nb = int(job_name[3])
                    isSame, msg = identicalFiles(
                        job_stdout_file,
                        WorkflowTest.wf_examples.lo_stdout[job_nb])
                    self.assertTrue(isSame)
                    if self.path_management == WorkflowTest.LOCAL_PATH:
                        isSame, msg = identicalFiles(
                            WorkflowTest.wf_examples.lo_out_model_file[job_nb],
                            WorkflowTest.wf_examples.lo_file[job_nb])
                        self.assertTrue(isSame)
                        isSame, msg = identicalFiles(
                            job_stderr_file,
                            WorkflowTest.wf_examples.lo_stderr[job_nb])
                        self.assertTrue(isSame)
                    if self.path_management == WorkflowTest.FILE_TRANSFER:
                        isSame, msg = identicalFiles(
                            WorkflowTest.wf_examples.lo_out_model_file[job_nb],
                            WorkflowTest.wf_examples.tr_file[job_nb].client_path)
                        self.assertTrue(isSame)

                if job_name == 'job1 with exception':
                    isSame, msg = identicalFiles(
                        job_stdout_file,
                        WorkflowTest.wf_examples.lo_stdout1_exception_model)
                    self.assertTrue(isSame)


class SimpleException2Test(WorkflowTest):
    allowed_file_systems = [WorkflowTest.LOCAL_PATH,
                            WorkflowTest.SHARED_RESOURCE_PATH,
                            WorkflowTest.FILE_TRANSFER]

    def test_result(self):
        workflow = WorkflowTest.wf_examples.example_simple_exception2()
        self.wf_id = WorkflowTest.wf_ctrl.submit_workflow(
            workflow=workflow,
            name=self.__class__.__name__)
        # Transfer input files if file transfer
        if self.path_management == WorkflowTest.FILE_TRANSFER:
            Helper.transfer_input_files(self.wf_id, WorkflowTest.wf_ctrl)
        # Wait for the workflow to finish
        Helper.wait_workflow(self.wf_id, WorkflowTest.wf_ctrl)
        # Transfer output files if file transfer
        if self.path_management == WorkflowTest.FILE_TRANSFER:
            Helper.transfer_output_files(self.wf_id, WorkflowTest.wf_ctrl)

        status = self.wf_ctrl.workflow_status(self.wf_id)
        self.assertTrue(status == constants.WORKFLOW_DONE)
        self.assertTrue(len(Helper.list_failed_jobs(
                        self.wf_id,
                        WorkflowTest.wf_ctrl)) == 1)
        self.assertTrue(len(Helper.list_failed_jobs(
                        self.wf_id,
                        WorkflowTest.wf_ctrl,
                        include_aborted_jobs=True)) == 2)

        (jobs_info, transfers_info, workflow_status, workflow_queue) = \
            WorkflowTest.wf_ctrl.workflow_elements_status(self.wf_id)

        # TODO: check the stdout and stderrr
        for (job_id, tmp_status, queue, exit_info, dates) in jobs_info:
            job_list = self.wf_ctrl.jobs([job_id])
            job_name, job_command, job_submission_date = job_list[job_id]

            if exit_info[0] == constants.FINISHED_REGULARLY:
                # To check job standard out
                job_stdout_file = "/tmp/job_soma_out_log_" + repr(job_id)
                job_stderr_file = "/tmp/job_soma_outerr_log_" + repr(job_id)
                self.wf_ctrl.retrieve_job_stdouterr(job_id,
                                                    job_stdout_file,
                                                    job_stderr_file)
                if job_name == 'job1':
                    isSame, msg = identicalFiles(
                        job_stdout_file,
                        WorkflowTest.wf_examples.lo_stdout[1])
                    self.assertTrue(isSame)
                    if self.path_management == WorkflowTest.LOCAL_PATH:
                        isSame, msg = identicalFiles(
                            WorkflowTest.wf_examples.lo_out_model_file[11],
                            WorkflowTest.wf_examples.lo_file[11])
                        self.assertTrue(isSame)
                        isSame, msg = identicalFiles(
                            WorkflowTest.wf_examples.lo_out_model_file[12],
                            WorkflowTest.wf_examples.lo_file[12])
                        self.assertTrue(isSame)
                        isSame, msg = identicalFiles(
                            job_stderr_file,
                            WorkflowTest.wf_examples.lo_stderr[1])
                        self.assertTrue(isSame)
                    if self.path_management == WorkflowTest.FILE_TRANSFER:
                        isSame, msg = identicalFiles(
                            WorkflowTest.wf_examples.lo_out_model_file[11],
                            WorkflowTest.wf_examples.tr_file[11].client_path)
                        self.assertTrue(isSame)
                        isSame, msg = identicalFiles(
                            WorkflowTest.wf_examples.lo_out_model_file[12],
                            WorkflowTest.wf_examples.tr_file[12].client_path)
                        self.assertTrue(isSame)

                if job_name in ['job2', 'job4']:
                    job_nb = int(job_name[3])
                    isSame, msg = identicalFiles(
                        job_stdout_file,
                        WorkflowTest.wf_examples.lo_stdout[job_nb])
                    self.assertTrue(isSame)
                    if self.path_management == WorkflowTest.LOCAL_PATH:
                        isSame, msg = identicalFiles(
                            WorkflowTest.wf_examples.lo_out_model_file[job_nb],
                            WorkflowTest.wf_examples.lo_file[job_nb])
                        self.assertTrue(isSame)
                        isSame, msg = identicalFiles(
                            job_stderr_file,
                            WorkflowTest.wf_examples.lo_stderr[job_nb])
                        self.assertTrue(isSame)
                    if self.path_management == WorkflowTest.FILE_TRANSFER:
                        isSame, msg = identicalFiles(
                            WorkflowTest.wf_examples.lo_out_model_file[job_nb],
                            WorkflowTest.wf_examples.tr_file[job_nb].client_path)
                        self.assertTrue(isSame)

                if job_name == 'job3 with exception':
                    isSame, msg = identicalFiles(
                        job_stdout_file,
                        WorkflowTest.wf_examples.lo_stdout1_exception_model)
                    self.assertTrue(isSame)


class NJobsTest(WorkflowTest):
    allowed_file_systems = [WorkflowTest.LOCAL_PATH,
                            WorkflowTest.SHARED_RESOURCE_PATH,
                            WorkflowTest.FILE_TRANSFER]

    def test_result(self):
        workflow = WorkflowTest.wf_examples.example_n_jobs(nb=20, time=1)
        self.wf_id = WorkflowTest.wf_ctrl.submit_workflow(
            workflow=workflow,
            name=self.__class__.__name__)
        # Transfer input files if file transfer
        if self.path_management == WorkflowTest.FILE_TRANSFER:
            Helper.transfer_input_files(self.wf_id, WorkflowTest.wf_ctrl)
        # Wait for the workflow to finish
        Helper.wait_workflow(self.wf_id, WorkflowTest.wf_ctrl)
        # Transfer output files if file transfer
        if self.path_management == WorkflowTest.FILE_TRANSFER:
            Helper.transfer_output_files(self.wf_id, WorkflowTest.wf_ctrl)

        status = self.wf_ctrl.workflow_status(self.wf_id)
        self.assertTrue(status == constants.WORKFLOW_DONE)
        self.assertTrue(len(Helper.list_failed_jobs(
                        self.wf_id,
                        WorkflowTest.wf_ctrl,
                        include_aborted_jobs=True)) == 0)


#class NJobsWithDependencies(WorkflowTest):
#    pass
#
#
#class SerialJobs(WorkflowTest):
#    pass
#
#
#class FakePipelineT1(WorkflowTest):
#    pass


class MultipleTest(WorkflowTest):
    allowed_file_systems = [WorkflowTest.LOCAL_PATH,
                            WorkflowTest.SHARED_RESOURCE_PATH,
                            WorkflowTest.FILE_TRANSFER]

    def test_result(self):
        workflow = WorkflowTest.wf_examples.example_multiple()
        self.wf_id = WorkflowTest.wf_ctrl.submit_workflow(
            workflow=workflow,
            name=self.__class__.__name__)

        # Transfer input files if file transfer
        if self.path_management == WorkflowTest.FILE_TRANSFER:
            Helper.transfer_input_files(self.wf_id, WorkflowTest.wf_ctrl)

        # Wait for the workflow to finish
        Helper.wait_workflow(self.wf_id, WorkflowTest.wf_ctrl)

        # Transfer output files if file transfer
        if self.path_management == WorkflowTest.FILE_TRANSFER:
            Helper.transfer_output_files(self.wf_id, WorkflowTest.wf_ctrl)

        status = self.wf_ctrl.workflow_status(self.wf_id)
        self.assertTrue(status == constants.WORKFLOW_DONE)
        self.assertTrue(len(Helper.list_failed_jobs(
                        self.wf_id,
                        WorkflowTest.wf_ctrl)) == 2)
        self.assertTrue(len(Helper.list_failed_jobs(
                        self.wf_id,
                        WorkflowTest.wf_ctrl,
                        include_aborted_jobs=True)) == 6)
#        fail_jobs = Helper.list_failed_jobs(self.wf_id, WorkflowTest.wf_ctrl)

#    num_list_fail_jobs=len(fail_jobs)
#    print "num_list_fail_jobs=" + repr(num_list_fail_jobs)
#    for fail_job_id in fail_jobs:
#        print "fail job id :" +repr(fail_job_id)+"\n"

        (jobs_info, transfers_info, workflow_status, workflow_queue) = \
            WorkflowTest.wf_ctrl.workflow_elements_status(self.wf_id)
#    print "len(jobs_info)=" + repr(len(jobs_info)) + "\n"

        # TODO: check the stdout and stderrr
        for (job_id, tmp_status, queue, exit_info, dates) in jobs_info:
#        print "job_id="         +repr(job_id)+"\n"
            job_list = self.wf_ctrl.jobs([job_id])
            # print 'len(job_list)='+repr(len(job_list))+"\n"
            job_name, job_command, job_submission_date = job_list[job_id]
#        print "name="			+repr(job_name)+"\n"
#        print "command="        +repr(job_command)+"\n"
#        print "submission="     +repr(job_submission_date)+"\n"
#        print "tmp_status="     +repr(tmp_status)+"\n"
#        print "exit_info="		+repr(exit_info)+"\n"
#        print "dates="			+repr(dates)+"\n"

            if exit_info[0] == constants.FINISHED_REGULARLY:
                # To check job standard out
                # print "Verify "+repr(job_name)+" \n"
                job_stdout_file = "/tmp/job_soma_out_log_" + repr(job_id)
                job_stderr_file = "/tmp/job_soma_outerr_log_" + repr(job_id)
                self.wf_ctrl.retrieve_job_stdouterr(job_id,
                                                    job_stdout_file,
                                                    job_stderr_file)
                if job_name == 'job1':
                    isSame, msg = identicalFiles(
                        job_stdout_file,
                        WorkflowTest.wf_examples.lo_stdout[1])
                    self.assertTrue(isSame)
                    if self.path_management == WorkflowTest.LOCAL_PATH:
                        isSame, msg = identicalFiles(
                            WorkflowTest.wf_examples.lo_out_model_file[11],
                            WorkflowTest.wf_examples.lo_file[11])
                        self.assertTrue(isSame)
                        isSame, msg = identicalFiles(
                            WorkflowTest.wf_examples.lo_out_model_file[12],
                            WorkflowTest.wf_examples.lo_file[12])
                        self.assertTrue(isSame)
                        isSame, msg = identicalFiles(
                            job_stderr_file,
                            WorkflowTest.wf_examples.lo_stderr[1])
                        self.assertTrue(isSame)
                    if self.path_management == WorkflowTest.FILE_TRANSFER:
                        isSame, msg = identicalFiles(
                            WorkflowTest.wf_examples.lo_out_model_file[11],
                            WorkflowTest.wf_examples.tr_file[11].client_path)
                        self.assertTrue(isSame)
                        isSame, msg = identicalFiles(
                            WorkflowTest.wf_examples.lo_out_model_file[12],
                            WorkflowTest.wf_examples.tr_file[12].client_path)
                        self.assertTrue(isSame)
                        # For unknown reason, it raises some errors
                        # http://stackoverflow.com/questions/10496758/unexpected-end-of-file-and-error-importing-function-definition-error-running
                        #isSame,	msg	= identicalFiles(job_stderr_file,WorkflowTest.wf_examples.lo_stderr[1])
                        #self.failUnless(isSame == True)

                if job_name in ['job2', 'job3', 'job4']:
                    job_nb = int(job_name[3])
                    isSame, msg = identicalFiles(
                        job_stdout_file,
                        WorkflowTest.wf_examples.lo_stdout[job_nb])
                    self.assertTrue(isSame)
                    if self.path_management == WorkflowTest.LOCAL_PATH:
                        isSame, msg = identicalFiles(
                            WorkflowTest.wf_examples.lo_out_model_file[job_nb],
                            WorkflowTest.wf_examples.lo_file[job_nb])
                        self.assertTrue(isSame)
                        isSame, msg = identicalFiles(
                            job_stderr_file,
                            WorkflowTest.wf_examples.lo_stderr[job_nb])
                        self.assertTrue(isSame)
                    if self.path_management == WorkflowTest.FILE_TRANSFER:
                        isSame, msg = identicalFiles(
                            WorkflowTest.wf_examples.lo_out_model_file[job_nb],
                            WorkflowTest.wf_examples.tr_file[job_nb].client_path)
                        self.assertTrue(isSame)

                if job_name in ['job1 with exception',
                                'job3 with exception']:
                    isSame, msg = identicalFiles(
                        job_stdout_file,
                        WorkflowTest.wf_examples.lo_stdout1_exception_model)
                    self.assertTrue(isSame)


if __name__ == '__main__':
    debug = False
    # List of all the classes in this module
    clsmembers = inspect.getmembers(sys.modules[__name__],
                                    lambda member: inspect.isclass(member) and
                                    member.__module__ == __name__)

    sys.stdout.write("*********** soma-workflow tests: WORKFLOW ***********\n")

    config_file_path = Configuration.search_config_path()
#    sys.stdout.write("Configuration file: " + config_file_path + "\n")
    resource_ids = Configuration.get_configured_resources(config_file_path)

    for resource_id in resource_ids:
        print "Resource :", resource_id
        config = Configuration.load_from_file(resource_id, config_file_path)
        (login, password) = get_user_id(config)

        with suppress_stdout(debug):
            wf_controller = WorkflowController(resource_id, login, password)
            WorkflowTest.setup_wf_controller(wf_controller)

        allowed_config = WorkflowTest.allowed_config[:]
        for configuration in WorkflowTest.allowed_config:
            if config.get_mode() != configuration[0]:
                allowed_config.remove(configuration)

        for configuration in allowed_config:
            (mode, file_system) = configuration
            print "\n---------------------------------------------------------"
            print "Mode :", mode
            print "File system :", file_system
            WorkflowTest.setup_path_management(file_system)

            suite_list = []
            for clsmember in clsmembers:
                (clsname, cls) = clsmember
                if clsname == 'WorkflowTest':
                    continue
                if file_system not in cls.allowed_file_systems:
                    continue
                # Get all the available tests for each class
                list_tests = []
                for test in dir(cls):
                    prefix = "test_"
                    if len(test) < len(prefix):
                        continue
                    if test[0: len(prefix)] == prefix:
                        list_tests.append(test)

                suite_list.append(unittest.TestSuite(map(cls,
                                                         list_tests)))
            alltests = unittest.TestSuite(suite_list)
            with suppress_stdout(debug):
                unittest.TextTestRunner(verbosity=2).run(alltests)

    sys.exit(0)
