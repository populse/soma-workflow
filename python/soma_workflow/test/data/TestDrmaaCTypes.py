#!/usr/bin/env python


'''
@author: Jinpeng Li

@organization: I2BM, Neurospin, Gif-sur-Yvette, France
@organization: U{IFR 49<http://www.ifr49.org>}

@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''

from __future__ import print_function

import unittest
from unittest import TestSuite
import sys
import threading
import time
import logging
import os

import Pyro.naming
import Pyro.core
from Pyro.errors import PyroError, NamingError, ProtocolError

import soma_workflow.engine
import soma_workflow.scheduler
import soma_workflow.connection
from soma_workflow.errors import EngineError
from soma_workflow.database_server import WorkflowDatabaseServer
from soma_workflow.engine_types import Job, EngineJob


class Configuration(Pyro.core.ObjBase,
                    soma_workflow.configuration.Configuration):

    def __init__(self,
                 resource_id,
                 mode,
                 scheduler_type,
                 database_file,
                 transfered_file_dir,
                 submitting_machines=None,
                 cluster_address=None,
                 name_server_host=None,
                 server_name=None,
                 queues=None,
                 queue_limits=None,
                 drmaa_implementation=None):
        Pyro.core.ObjBase.__init__(self)
        soma_workflow.configuration.Configuration.__init__(self,
                                                           resource_id,
                                                           mode,
                                                           scheduler_type,
                                                           database_file,
                                                           transfered_file_dir,
                                                           submitting_machines,
                                                           cluster_address,
                                                           name_server_host,
                                                           server_name,
                                                           queues,
                                                           queue_limits,
                                                           drmaa_implementation)
        pass


class DrmaaCTypesTest(unittest.TestCase):

    resource_id = ""
    _drmaa = None

    def setUp(self):
        self.resource_id = "DSV_cluster_ed203246"
        pass

    def tearDown(self):
        rmpaths = []
        rmpaths.append(
            os.path.join(os.path.expanduser("~"),
                         "soma-workflow-empty-job-patch-torque.o"))
        rmpaths.append(
            os.path.join(os.path.expanduser("~"),
                         "soma-workflow-empty-job-patch-torque.e"))

        # To refresh the file system
        os.listdir(os.path.expanduser("~"))

        for rmpath in rmpaths:
            if os.path.isfile(rmpath):
                os.remove(rmpath)

    def checkoutput(self, sch):
        # To refresh the file system
        os.listdir(sch.tmp_file_path)

        outputfilepath = os.path.join(
            sch.tmp_file_path,
            "soma-workflow-empty-job-patch-torque.o")
        self.failUnless(os.path.isfile(outputfilepath))
        # print("outputfilepath="+outputfilepath)
        outfile = open(outputfilepath)
        line = outfile.readline()
        line = line.strip()
        self.failUnless(line, "hello jinpeng")
        outfile.close()

#    def test_ctype_drmaa_session(self):
#        import drmaa
#        self._drmaa=somadrmaa.Session()
#        self._drmaa.initialize()
#        self._drmaa.exit()
#
#    def test_DrmaaCTypes(self):
#        config = Configuration.load_from_file(self.resource_id)
#
# print("config.get_drmaa_implementation()="+repr(config.get_drmaa_implementation()))
# print("config.get_parallel_job_config()="+repr(config.get_parallel_job_config()))
# print("config.get_native_specification()="+repr(config.get_native_specification()))
#
#        sch = soma_workflow.scheduler.DrmaaCTypes('PBS',
#                                    config.get_parallel_job_config(),
#                                    os.path.expanduser("~"),
#                                    configured_native_spec=config.get_native_specification())
#        sch.submit_simple_test_job('hello jinpeng','soma-workflow-empty-job-patch-torque.o','soma-workflow-empty-job-patch-torque.e')
#        self.checkoutput(sch)
#
#    def test_DrmaaCTypesSleepWake(self):
#        config = Configuration.load_from_file(self.resource_id)
#
# print("config.get_drmaa_implementation()="+repr(config.get_drmaa_implementation()))
# print("config.get_parallel_job_config()="+repr(config.get_parallel_job_config()))
# print("config.get_native_specification()="+repr(config.get_native_specification()))
#
#        sch = soma_workflow.scheduler.DrmaaCTypes('PBS',
#                                    config.get_parallel_job_config(),
#                                    os.path.expanduser("~"),
#                                    configured_native_spec=config.get_native_specification())
#        sch.submit_simple_test_job('hello jinpeng','soma-workflow-empty-job-patch-torque.o','soma-workflow-empty-job-patch-torque.e')
#        self.checkoutput(sch)
#
#        sch.clean()
#        sch.sleep()
#        sch.wake()
#        sch.submit_simple_test_job('hello jinpeng','soma-workflow-empty-job-patch-torque.o','soma-workflow-empty-job-patch-torque.e')
#        self.checkoutput(sch)
    def test_DrmaaCTypesSubAJob(self):
        config = Configuration.load_from_file(self.resource_id)

        sch = soma_workflow.scheduler.DrmaaCTypes(
            'PBS',
            config.get_parallel_job_config(),
            os.path.expanduser("~"),
            configured_native_spec=config.get_native_specification())

        job = Job(["echo", "hello jinpeng"],
                  None,
                  None,
                  None, False, 168, "for test",
                  os.path.join(
                      sch.tmp_file_path,
                      'soma-workflow-empty-job-patch-torque.o'),
                  os.path.join(sch.tmp_file_path,
                               'soma-workflow-empty-job-patch-torque.e'))

        ejob = EngineJob(job, '')
        jobid = sch.job_submission(ejob)

        i = 5
        while i > 0:
            print(sch.get_job_status(jobid))
            os.system("sleep 1")
            i = i - 1

        exit_status, exit_value, term_sig, str_rusage = sch.get_job_exit_info(
            jobid)
        print("exit_status=" + repr(exit_status))
        print("exit_value=" + repr(exit_value))
        print("term_sig=" + repr(term_sig))
        print("str_rusage=" + repr(str_rusage))

        self.checkoutput(sch)


suite = unittest.TestLoader().loadTestsFromTestCase(DrmaaCTypesTest)
unittest.TextTestRunner(verbosity=2).run(suite)
