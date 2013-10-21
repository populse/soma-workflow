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
import os
#import getpass
import sys

#from soma_workflow.client import Job
#from soma_workflow.client import SharedResourcePath
#from soma_workflow.client import FileTransfer
#from soma_workflow.client import Group
#from soma_workflow.client import Workflow
from soma_workflow.client import WorkflowController
from soma_workflow.client import Helper
from soma_workflow.configuration import Configuration
from soma_workflow.errors import ConfigurationError
#from soma_workflow.errors import UnknownObjectError
#from soma_workflow.utils import checkFiles
from soma_workflow.utils import identicalFiles
import soma_workflow.constants as constants

from soma_workflow.test.workflow_local import WorkflowExamplesLocal
from soma_workflow.test.workflow_shared import WorkflowExamplesShared
#from soma_workflow.test.workflow_shared_transfer import WorkflowExamplesSharedTransfer
from soma_workflow.test.workflow_transfer import WorkflowExamplesTransfer

from soma_workflow.test.utils import select_resources
from soma_workflow.test.utils import select_configuration
from soma_workflow.test.utils import select_workflow_type
from soma_workflow.test.utils import select_file_path_type
from soma_workflow.test.utils import select_test_type


#-----------------------------------------------------------------------------
# Classes and functions
#-----------------------------------------------------------------------------

class WorkflowTest(unittest.TestCase):

    LOCAL_PATH = "local path"
    FILE_TRANSFER = "file transfer"
    SHARED_RESOURCE_PATH = "shared resource path"

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


class MultipleTest(WorkflowTest):

    def test_result(self):
        workflow = WorkflowTest.wf_examples.multiple_simple_example()
        self.wf_id = WorkflowTest.wf_ctrl.submit_workflow(
            workflow=workflow,
            name="unit test multiple")
        if self.path_management == WorkflowTest.FILE_TRANSFER:
            Helper.transfer_input_files(self.wf_id, WorkflowTest.wf_ctrl)

        Helper.wait_workflow(self.wf_id, WorkflowTest.wf_ctrl)

        if self.path_management == WorkflowTest.FILE_TRANSFER:
            Helper.transfer_output_files(self.wf_id, WorkflowTest.wf_ctrl)

        status = self.wf_ctrl.workflow_status(self.wf_id)

        fail_jobs = Helper.list_failed_jobs(self.wf_id, WorkflowTest.wf_ctrl)

#    num_list_fail_jobs=len(fail_jobs)
#    print "num_list_fail_jobs=" + repr(num_list_fail_jobs)
#    for fail_job_id in fail_jobs:
#        print "fail job id :" +repr(fail_job_id)+"\n"

        (jobs_info, transfers_info, workflow_status, workflow_queue) = \
            WorkflowTest.wf_ctrl.workflow_elements_status(self.wf_id)
#    print "len(jobs_info)=" + repr(len(jobs_info)) + "\n"

        list_possible_jobs = ["'job1'", "'job1 with exception'", "'job2'",
                              "'job3'", "'job4'", "'job3 with exception'"]
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

            # To check job standard out
            if repr(job_name) in list_possible_jobs and \
                    exit_info[0] == constants.FINISHED_REGULARLY:
                # print "Verify "+repr(job_name)+" \n"
                job_stdout_file = "/tmp/job_soma_out_log_" + repr(job_id)
                job_stderr_file = "/tmp/job_soma_outerr_log_" + repr(job_id)
                self.wf_ctrl.retrieve_job_stdouterr(job_id,
                                                    job_stdout_file,
                                                    job_stderr_file)
                if repr(job_name) == "'job1'":
                    isSame, msg = identicalFiles(
                        job_stdout_file, WorkflowTest.wf_examples.lo_stdout[1])
                    self.failUnless(isSame)

                    if self.path_management == WorkflowTest.LOCAL_PATH:
                        isSame, msg = identicalFiles(
                            WorkflowTest.wf_examples.lo_out_model_file[11],
                            WorkflowTest.wf_examples.lo_file[11])
                        self.failUnless(isSame)
                        isSame, msg = identicalFiles(
                            WorkflowTest.wf_examples.lo_out_model_file[12],
                            WorkflowTest.wf_examples.lo_file[12])
                        self.failUnless(isSame)
                        isSame, msg = identicalFiles(
                            job_stderr_file,
                            WorkflowTest.wf_examples.lo_stderr[1])
                        self.failUnless(isSame)
                    if self.path_management == WorkflowTest.FILE_TRANSFER:
                        isSame, msg = identicalFiles(
                            WorkflowTest.wf_examples.lo_out_model_file[11],
                            WorkflowTest.wf_examples.tr_file[11].client_path)
                        self.failUnless(isSame)
                        isSame, msg = identicalFiles(
                            WorkflowTest.wf_examples.lo_out_model_file[12],
                            WorkflowTest.wf_examples.tr_file[12].client_path)
                        self.failUnless(isSame)
                        # For unknown reason, it raises some errors
                        # http://stackoverflow.com/questions/10496758/unexpected-end-of-file-and-error-importing-function-definition-error-running
                        #isSame,	msg	= identicalFiles(job_stderr_file,WorkflowTest.wf_examples.lo_stderr[1])
                        #self.failUnless(isSame == True)

                if repr(job_name) == "'job1 with exception'":
                    isSame, msg = identicalFiles(
                        job_stdout_file,
                        WorkflowTest.wf_examples.lo_stdout1_exception_model)
                    self.failUnless(isSame)

                if repr(job_name) == "'job2'":
                    if self.path_management == WorkflowTest.FILE_TRANSFER:
                        isSame, msg = identicalFiles(
                            WorkflowTest.wf_examples.tr_file[2].client_path,
                            WorkflowTest.wf_examples.lo_out_model_file[2])
                    if self.path_management == WorkflowTest.LOCAL_PATH:
                        isSame, msg = identicalFiles(
                            WorkflowTest.wf_examples.lo_file[2],
                            WorkflowTest.wf_examples.lo_out_model_file[2])
                        self.failUnless(isSame)
                        isSame, msg = identicalFiles(
                            job_stderr_file,
                            WorkflowTest.wf_examples.lo_stderr[2])
                        self.failUnless(isSame)
                    isSame, msg = identicalFiles(
                        job_stdout_file, WorkflowTest.wf_examples.lo_stdout[2])
                    self.failUnless(isSame)

                if repr(job_name) == "'job3'":
                    isSame, msg = identicalFiles(
                        job_stdout_file, WorkflowTest.wf_examples.lo_stdout[3])
                    self.failUnless(isSame)
                    if self.path_management == WorkflowTest.LOCAL_PATH:
                        isSame, msg = identicalFiles(
                            job_stderr_file,
                            WorkflowTest.wf_examples.lo_stderr[3])
                        self.failUnless(isSame)
                        isSame, msg = identicalFiles(
                            WorkflowTest.wf_examples.lo_file[3],
                            WorkflowTest.wf_examples.lo_out_model_file[3])
                        self.failUnless(isSame)
                    if self.path_management == WorkflowTest.FILE_TRANSFER:
                        isSame, msg = identicalFiles(
                            WorkflowTest.wf_examples.tr_file[3].client_path,
                            WorkflowTest.wf_examples.lo_out_model_file[3])
                        self.failUnless(isSame)

                if repr(job_name) == "'job3 with exception'":
                    isSame, msg = identicalFiles(
                        job_stdout_file,
                        WorkflowTest.wf_examples.lo_stdout1_exception_model)
                    self.failUnless(isSame)

                if repr(job_name) == "'job4'":
                    isSame, msg = identicalFiles(
                        job_stdout_file, WorkflowTest.wf_examples.lo_stdout[4])
                    self.failUnless(isSame)
                    if self.path_management == WorkflowTest.LOCAL_PATH:
                        isSame, msg = identicalFiles(
                            WorkflowTest.wf_examples.lo_file[4],
                            WorkflowTest.wf_examples.lo_out_model_file[4])
                        self.failUnless(isSame)
                        isSame, msg = identicalFiles(
                            job_stderr_file,
                            WorkflowTest.wf_examples.lo_stderr[4])
                        self.failUnless(isSame)
                    if self.path_management == WorkflowTest.FILE_TRANSFER:
                        isSame, msg = identicalFiles(
                            WorkflowTest.wf_examples.tr_file[4].client_path,
                            WorkflowTest.wf_examples.lo_out_model_file[4])
                        self.failUnless(isSame)

        self.assert_(status == constants.WORKFLOW_DONE)
        self.assert_(len(Helper.list_failed_jobs(
            self.wf_id,
            WorkflowTest.wf_ctrl)) == 2)
        self.assert_(len(Helper.list_failed_jobs(
            self.wf_id,
            WorkflowTest.wf_ctrl,
            include_aborted_jobs=True)) == 6)


class SpecialCommandTest(WorkflowTest):

    def test_result(self):
        workflow = WorkflowTest.wf_examples.special_command()
        self.wf_id = WorkflowTest.wf_ctrl.submit_workflow(
            workflow=workflow,
            name="unit test command check")
        if self.path_management == WorkflowTest.FILE_TRANSFER:
            Helper.transfer_input_files(self.wf_id, WorkflowTest.wf_ctrl)

        Helper.wait_workflow(self.wf_id, WorkflowTest.wf_ctrl)
        status = self.wf_ctrl.workflow_status(self.wf_id)

        self.assert_(status == constants.WORKFLOW_DONE)
        self.assert_(len(Helper.list_failed_jobs(
            self.wf_id,
            WorkflowTest.wf_ctrl)) == 0)
        self.assert_(len(Helper.list_failed_jobs(
            self.wf_id,
            WorkflowTest.wf_ctrl,
            include_aborted_jobs=True)) == 0)
        # TODO: check the stdout


if __name__ == '__main__':
    # Define example directories
    import soma_workflow
    job_examples_dir = os.path.join(soma_workflow.__path__[0],
                                    "..", "..", "test", "jobExamples")
    output_dir = os.path.join(soma_workflow.__path__[0],
                              "..", "..", "test", "out")
    if not os.path.isdir(output_dir):
        os.mkdir(output_dir)
    if (not os.path.isdir(job_examples_dir) or
            not os.path.isdir(output_dir)):
        raise ConfigurationError("%s or %s does not exist." % (
                                 job_examples_dir,
                                 output_dir))

    sys.stdout.write("----- soma-workflow tests: WORKFLOW -------------\n")

    config_file_path = Configuration.search_config_path()
    sys.stdout.write("Configuration file: " + repr(config_file_path) + "\n")
    resource_ids = Configuration.get_configured_resources(config_file_path)

    # Resource
    resource_id = select_resources(resource_ids)

    # Configuration
    (login, password) = select_configuration(resource_id, config_file_path)

    # Workflow types
    wf_types = ["multiple", "special command"]
    selected_wf_type = select_workflow_type(wf_types)

    # File path type
    path_types = [WorkflowTest.LOCAL_PATH,
                  WorkflowTest.FILE_TRANSFER,
                  WorkflowTest.SHARED_RESOURCE_PATH]
    selected_path_type = select_file_path_type(path_types)

    # Test type
    test_types = ["test_result"]
    # "test_workflows", "test_stop", "test_restart"]
    selected_test_type = select_test_type(test_types)

    wf_controller = WorkflowController(resource_id, login, password)
    WorkflowTest.setup_wf_controller(wf_controller)

    test_results = {}

    for path_management in selected_path_type:
        print "\n*************************************************************"
        print "Test Suite: " + repr(path_management) + ":\n"
        WorkflowTest.setup_path_management(path_management)

        suite_list = []
        tests = selected_test_type

        if "multiple" in selected_wf_type:
            suite_list.append(unittest.TestSuite(map(MultipleTest,
                                                     selected_test_type)))
        if "special command" in selected_wf_type:
            suite_list.append(unittest.TestSuite(map(SpecialCommandTest,
                                                     selected_test_type)))

        alltests = unittest.TestSuite(suite_list)
        test_result = unittest.TextTestRunner(verbosity=2).run(alltests)
        test_results[path_management] = test_result

    if len(test_results) > 1:
        for path_management, test_result in test_results.iteritems():
            if not test_result.wasSuccessful():
                print repr(path_management) + " test suite failed !"

    sys.exit(0)
