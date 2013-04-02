#!/usr/bin/env python


'''
@author: Jinpeng Li

@organization: I2BM, Neurospin, Gif-sur-Yvette, France
@organization: U{IFR 49<http://www.ifr49.org>}

@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''

import unittest
import sys
import threading
import time
import logging
import os

import Pyro.naming
import Pyro.core
from Pyro.errors import PyroError, NamingError, ProtocolError

import soma.workflow.engine
import soma.workflow.scheduler
import soma.workflow.connection
from soma.workflow.errors import EngineError
from soma.workflow.database_server import WorkflowDatabaseServer

class Configuration(Pyro.core.ObjBase,
                    soma.workflow.configuration.Configuration):

  def __init__( self,
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
    soma.workflow.configuration.Configuration.__init__(self,
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

    resource_id=""
    _drmaa=None

    def setUp(self):
        self.resource_id="DSV_cluster_ed203246"
        pass

    def test_ctype_drmaa_session(self):
        import drmaa
        self._drmaa=drmaa.Session()
        self._drmaa.initialize()
        self._drmaa.exit()

    def checkoutput(self,sch):
        outputfilepath=os.path.join(sch.tmp_file_path, "soma-workflow-empty-job-patch-torque.o")
        outfile = open(outputfilepath)
        line=outfile.readline()
        line=line.strip()
        self.failUnless(line, "hello jinpeng")
        outfile.close()

    def test_DrmaaCTypes(self):
        config = Configuration.load_from_file(self.resource_id)
        #print "config.get_drmaa_implementation()="+repr(config.get_drmaa_implementation())
        #print "config.get_parallel_job_config()="+repr(config.get_parallel_job_config())
        #print "config.get_native_specification()="+repr(config.get_native_specification())

        sch = soma.workflow.scheduler.DrmaaCTypes('PBS',
                                    config.get_parallel_job_config(),
                                    os.path.expanduser("~"),
                                    configured_native_spec=config.get_native_specification())
        os.system("sleep 1")
        self.checkoutput(sch) 
        sch.clean()
        sch.sleep()
        sch.wake()
        sch.submit_simple_test_job('hello jinpeng','soma-workflow-empty-job-patch-torque.o','soma-workflow-empty-job-patch-torque.e',os.path.expanduser("~"))
        os.system("sleep 1")
        self.checkoutput(sch)

 
unittest.main()


