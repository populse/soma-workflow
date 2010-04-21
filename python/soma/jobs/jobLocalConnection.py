'''
@author: Soizic Laguitton
@organization: U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''

import pexpect
import Pyro.naming, Pyro.core
from Pyro.errors import NamingError
import time
import socket
from connectionCheck import ConnectionHolder
import pwd
import os
import sys


__docformat__ = "epytext en"



class JobLocalConnection( object ):
  
  def __init__(self,
               local_process_src):
    '''
    '''

    login = pwd.getpwuid(os.getuid())[0] 

    pyro_objet_name = "jobScheduler_" + login


    # run the local job process and get back the    #
    # JobScheduler and ConnectionChecker URIs       #

    command = "python " + local_process_src + " " + pyro_objet_name
    print command
    self.__job_process_child = pexpect.spawn(command)
    #fout = file('/neurospin/tmp/Soizic/jobFiles/mylog.txt','w')
    #self.__job_process_child.logfile = fout
    self.__job_process_child.logfile = sys.stdout
    self.__job_process_child.expect(pyro_objet_name + " URI: ")
    job_scheduler_uri = self.__job_process_child.readline()
    self.__job_process_child.expect(" connectionChecker URI: ")
    connection_checker_uri = self.__job_process_child.readline()  
    
    # create the proxies                     #
    self.jobScheduler = Pyro.core.getProxyForURI(job_scheduler_uri)
    connection_checker = Pyro.core.getAttrProxyForURI(connection_checker_uri)
  
    # create the connection holder objet for #
    # a clean disconnection in any case      #
    self.__connection_holder = ConnectionHolder(connection_checker)
    self.__connection_holder.start()

  def getJobScheduler(self):
    return self.jobScheduler



