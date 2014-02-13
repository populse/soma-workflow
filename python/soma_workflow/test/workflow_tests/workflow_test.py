from __future__ import with_statement
'''
@author: laure.hugo@cea.fr
@author: Soizic Laguitton
@organization: U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''

import unittest
import sys

from soma_workflow.client import WorkflowController
from soma_workflow.configuration import Configuration
from soma_workflow.test.utils import get_user_id
from soma_workflow.test.utils import suppress_stdout

from soma_workflow.test.workflow_tests import WorkflowExamplesLocal
from soma_workflow.test.workflow_tests import WorkflowExamplesShared
from soma_workflow.test.workflow_tests import WorkflowExamplesSharedTransfer
from soma_workflow.test.workflow_tests import WorkflowExamplesTransfer


class WorkflowTest(unittest.TestCase):

    LOCAL_PATH = "local path"
    FILE_TRANSFER = "file transfer"
    SHARED_RESOURCE_PATH = "shared resource path"
    SHARED_TRANSFER = "file transfer and shared resource path"

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

    @classmethod
    def setUp(cls):
        if cls.path_management == cls.LOCAL_PATH:
            workflow_examples = WorkflowExamplesLocal()
        elif cls.path_management == cls.FILE_TRANSFER:
            workflow_examples = WorkflowExamplesTransfer()
        elif cls.path_management == cls.SHARED_RESOURCE_PATH:
            workflow_examples = WorkflowExamplesShared()
        elif cls.path_management == cls.SHARED_TRANSFER:
            workflow_examples = WorkflowExamplesSharedTransfer()
        cls.wf_examples = workflow_examples
        #raise Exception("WorkflowTest is an abstract class.")

    def tearDown(self):
        if self.wf_id:
            self.__class__.wf_ctrl.delete_workflow(self.wf_id)

    def test_result(self):
        pass

    @classmethod
    def run_test(cls, debug=False):
        sys.stdout.write("********* soma-workflow tests: WORKFLOW *********\n")

        config_file_path = Configuration.search_config_path()
    #    sys.stdout.write("Configuration file: " + config_file_path + "\n")
        resource_ids = Configuration.get_configured_resources(config_file_path)

        for resource_id in resource_ids:
            sys.stdout.write("============ Resource : " + resource_id +
                             " =================== \n")
            sys.stdout.write("Do you want to test the resource "
                             "%s (Y/n) ? " % resource_id)
            test_resource = sys.stdin.readline()
            if test_resource.strip() in ['no', 'n', 'N', 'No', 'NO']:
                # Skip the resource
                sys.stdout.write('Resource %s is not tested \n' % resource_id)
                continue
            config = Configuration.load_from_file(resource_id,
                                                  config_file_path)
            (login, password) = get_user_id(resource_id, config)

            with suppress_stdout(debug):
                wf_controller = WorkflowController(resource_id,
                                                   login,
                                                   password)
                cls.setup_wf_controller(wf_controller)

            allowed_config = cls.allowed_config[:]
            for configuration in cls.allowed_config:
                if config.get_mode() != configuration[0]:
                    allowed_config.remove(configuration)
            if len(allowed_config) == 0:
                sys.stdout.write("No tests available for the resource %s \n"
                                 % resource_id)

            for configuration in allowed_config:
                (mode, file_system) = configuration
                sys.stdout.write("\n---------------------------------------\n")
                sys.stdout.write("Mode : " + mode + '\n')
                sys.stdout.write("File system : " + file_system + '\n')
                cls.setup_path_management(file_system)

                suite_list = []
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
