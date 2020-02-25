# -*- coding: utf-8 -*-
'''
@author: laure.hugo@cea.fr
@author: Soizic Laguitton
@organization: U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''

from __future__ import absolute_import
import unittest
import time
# import os
# import getpass
import sys
from datetime import datetime
from datetime import timedelta
from abc import abstractmethod

import soma_workflow.constants as constants
from soma_workflow.client import WorkflowController
from soma_workflow.configuration import Configuration
# from soma_workflow.test.utils import check_files
from soma_workflow.test.job_tests.job_examples import JobExamples
from soma_workflow.test.utils import get_user_id
from soma_workflow.test.utils import suppress_stdout
from six.moves import map


class JobTests(unittest.TestCase):

    '''
    Abstract class for soma_workflow job and transfer common tests.
    '''
    @classmethod
    def setup_connection(cls, resource_id, login, password):
        cls.login = login
        cls.password = password
        cls.resource_id = resource_id
        cls.wf_ctrl = WorkflowController(resource_id,
                                         login,
                                         password)
        cls.transfer_timeout = -24
        cls.jobs_timeout = 1
        cls.job_examples = JobExamples(cls.wf_ctrl,
                                       'python',
                                       cls.transfer_timeout,
                                       cls.jobs_timeout)

    @abstractmethod
    def setUp(self):
        raise Exception('JobTest is an abstract class'
                        'SetUp must be implemented in subclass')

    def tearDown(self):
        # for jid in self.my_jobs:
            # self.wf_ctrl.delete_job(jid)
        remaining_jobs = frozenset(list(self.wf_ctrl.jobs().keys()))
        self.failUnless(len(remaining_jobs.intersection(self.my_jobs)) == 0)

    def test_jobs(self):
        res = set(self.wf_ctrl.jobs().keys())
        self.failUnless(res.issuperset(self.my_jobs))

    def test_wait(self):
        self.wf_ctrl.wait_job(self.my_jobs)
        for jid in self.my_jobs:
            status = self.wf_ctrl.job_status(jid)
            self.failUnless(status == constants.DONE or
                            status == constants.FAILED,
                            'Job %s status after wait: %s. Expected %s or %s' %
                            (jid, status, constants.DONE, constants.FAILED))

    def test_wait2(self):
        start_time = datetime.now()
        interval = 5
        self.wf_ctrl.wait_job(self.my_jobs, interval)
        delta = datetime.now() - start_time
        if delta < timedelta(seconds=interval):
            for jid in self.my_jobs:
                status = self.wf_ctrl.job_status(jid)
                self.failUnless(status == constants.DONE or
                                status == constants.FAILED,
                                'Job %s status after wait: %s.'
                                'Expected %s or %s' %
                                (self.my_jobs[0], status, constants.DONE,
                                 constants.FAILED))
        else:
            self.failUnless(abs(delta - timedelta(seconds=interval)) <
                            timedelta(seconds=1))

    # def test_restart(self):
        # jobid = self.my_jobs[len(self.my_jobs) - 1]
        # self.wf_ctrl.kill_job(jobid)
        # self.wf_ctrl.restart_job(jobid)
        # status = self.wf_ctrl.job_status(jobid)
        # self.failUnless(not status == constants.USER_ON_HOLD and
                        # not status == constants.USER_SYSTEM_ON_HOLD and
                        # not status == constants.USER_SUSPENDED and
                        # not status == constants.USER_SYSTEM_SUSPENDED,
                        #'Job status after restart: %s' % status)

    # def test_kill(self):
        # jobid = self.my_jobs[0]
        # time.sleep(2)
        # self.wf_ctrl.kill_job(jobid)
        # job_termination_status = self.wf_ctrl.job_termination_status(jobid)
        # exit_status = job_termination_status[0]
        # status = self.wf_ctrl.job_status(jobid)
        # self.failUnless(status == constants.FAILED or status == constants.DONE,
                        #'Job status after kill: %s. Expected %s or %s' %
                        #(status, constants.FAILED, constants.DONE))
        # self.failUnless(exit_status == constants.USER_KILLED or
                        # exit_status == constants.FINISHED_REGULARLY or
                        # exit_status == constants.EXIT_ABORTED,
                        #'Job exit status after kill: %s. Expected %s, %s or %s'
                        #% (exit_status, constants.USER_KILLED,
                           # constants.FINISHED_REGULARLY,
                           # constants.EXIT_ABORTED))

    @abstractmethod
    def test_result(self):
        raise Exception('JobTest is an abstract class. test_result must be'
                        'implemented in subclass')

    @classmethod
    def run_test(cls, debug=False):
        sys.stdout.write("********* soma-workflow tests: WORKFLOW *********\n")

        config_file_path = Configuration.search_config_path()
    #    sys.stdout.write("Configuration file: " + config_file_path + "\n")
        resource_ids = Configuration.get_configured_resources(config_file_path)

        for resource_id in resource_ids:
            sys.stdout.write("===== Resource : " + resource_id + " ====== \n")
            sys.stdout.write("Do you want to test the resource %s (Y/n) ?" %
                             resource_id)
            test_resource = sys.stdin.readline()
            if test_resource.strip() in ['no', 'n', 'N', 'No', 'NO']:
                # Skip the resource
                sys.stdout.write('Resource %s is not tested \n' % resource_id)
                continue

            config = Configuration.load_from_file(resource_id,
                                                  config_file_path)
            if config.get_mode() not in cls.allowed_resources:
                sys.stdout.write('No tests available for this resource \n')
                continue
            (login, password) = get_user_id(resource_id, config)

#            with suppress_stdout(debug):
#                wf_controller = WorkflowController(resource_id,
#                                                   login,
#                                                   password)
#                cls.setup_wf_controller(wf_controller)

#            allowed_config = cls.allowed_config[:]
#            for configuration in cls.allowed_config:
#                if config.get_mode() != configuration[0]:
#                    allowed_config.remove(configuration)
#            if len(allowed_config) == 0:
#                sys.stdout.write("No tests available for the resource %s \n" %
#                                  resource_id)

#            for configuration in allowed_config:
#                (mode, file_system) = configuration
#                sys.stdout.write("\n--------------------------------------\n")
#                sys.stdout.write("Mode : " + mode + \n")
#                sys.stdout.write("File system : " + file_system + \n")
#                cls.setup_path_management(file_system)
            with suppress_stdout(debug):
                JobTests.setup_connection(resource_id,
                                          login,
                                          password)

            suite_list = []
            list_tests = []
            for test in dir(cls):
                prefix = "test_"
                if len(test) < len(prefix):
                    continue
                if test[0: len(prefix)] == prefix:
                    list_tests.append(test)

            suite_list.append(unittest.TestSuite(list(map(cls,
                                                 list_tests))))
            alltests = unittest.TestSuite(suite_list)
            with suppress_stdout(debug):
                unittest.TextTestRunner(verbosity=2).run(alltests)
