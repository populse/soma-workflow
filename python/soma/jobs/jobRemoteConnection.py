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


__docformat__ = "epytext en"

'''
The L{JobRemoteConnection} class makes it possible to sumbit jobs from a machine which is
not a submitting machine of the pool and possibly doesn't share a file system with these 
machines. The fonction L{getJobScheduler} gets back a proxy of a L{JobScheduler} object. 
The connection between the remote machine and the pool is done via ssh using port 
forwarding (tunneling).
The protocol used inside the tunnel is Pyro's protocol.

requirements: Pyro must be installed on the remote machine.

To be consistent: "local" means on a submitting machine of the pool 
                  "remote" refers to all other machine
'''


class JobRemoteConnection( object ):
  
  def __init__(self,
               login, 
               password, 
               submitting_machine = "is143016",
               local_process_src = "/neurospin/tmp/Soizic/jobFiles/srcServers/localJobProcess.py"):
    '''
    Run the local job process, create a connection and get back a L(JobScheduler)
    proxy which can be used to submit, monitor and control jobs on the pool.
    
    @type  login: string
    @param login: user's login on the pool 
    @type  password: string
    @param password: associted password
    @type  submitting_machine: string
    @param submitting_machine: address of a submitting machine of the pool.
    @type  local_process_src: string
    @param local_process_src: path to the localJobProcess.py on the submitting_machine
    '''

    def createTunnel(port, host, hostport, login, server_address, password):
      command = "ssh -N -L %s:%s:%s %s@%s" %(port, host, hostport, login, server_address)
      print command
      child = pexpect.spawn(command) 
      child.expect('.ssword:*')
      child.sendline(password)
      time.sleep(2)
      return child
  
    def searchAvailablePort():
      s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # Create a TCP socket
      s.bind(('localhost',0)) #try to bind to the port 0 so that the system will find an available port
      available_port = s.getsockname()[1]
      s.close()
      return available_port 
    
    pyro_objet_name = "jobScheduler_" + login


    # run the local job process and get back the    #
    # JobScheduler and ConnectionChecker URIs       #

    command = "ssh %s@%s python %s %s" %( login, 
                                          submitting_machine, 
                                          local_process_src, 
                                          pyro_objet_name) 
    print command
    self.__job_process_child = pexpect.spawn(command)
    self.__job_process_child.expect('.ssword:*')
    self.__job_process_child.sendline(password)
    self.__job_process_child.expect(pyro_objet_name + " URI: ")
    job_scheduler_uri = self.__job_process_child.readline()
    self.__job_process_child.expect(" connectionChecker URI: ")
    connection_checker_uri = self.__job_process_child.readline()

    local_pyro_daemon_port = Pyro.core.processStringURI(job_scheduler_uri).port
    print "Pyro object port: " + repr(local_pyro_daemon_port)
  

    # find an available port              #
    remote_pyro_daemon_port = searchAvailablePort()
    print "client pyro object port: " + repr(remote_pyro_daemon_port)

    
    # tunnel creation                      #
    self.__pyro_daemon_tunnel_child = createTunnel(remote_pyro_daemon_port, 
                                                  submitting_machine, 
                                                  local_pyro_daemon_port, 
                                                  login, 
                                                  submitting_machine, 
                                                  password)
    
    
    
    # create the proxies                     #
    self.jobScheduler = Pyro.core.getProxyForURI(job_scheduler_uri)
    connection_checker = Pyro.core.getAttrProxyForURI(connection_checker_uri)
  
    # setting the proxies to use the tunnel  #
    self.jobScheduler.URI.port = remote_pyro_daemon_port
    self.jobScheduler.URI.address = 'localhost'
    connection_checker.URI.port = remote_pyro_daemon_port
    connection_checker.URI.address = 'localhost'

    # create the connection holder objet for #
    # a clean disconnection in any case      #
    self.__connection_holder = ConnectionHolder(connection_checker)
    self.__connection_holder.start()

  def getJobScheduler(self):
    return self.jobScheduler



