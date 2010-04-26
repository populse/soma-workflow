'''
@author: Soizic Laguitton
@organization: U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''


from __future__ import with_statement
from datetime import datetime
from datetime import timedelta
import threading
import time
import pexpect
import Pyro.naming, Pyro.core
from Pyro.errors import NamingError
import socket
import pwd 
import os
import sys
import shutil

__docformat__ = "epytext en"


class DaemonicPexpect(pexpect.spawn):
  
  def __del__(self):
    pass




class JobConnectionError( Exception):
  pass

'''
requirements: Pyro must be installed on the remote machine.

To be consistent: "local" means on a submitting machine of the pool 
                  "remote" refers to all other machine
'''


class JobRemoteConnection( object ):
  '''
  The L{JobRemoteConnection} class makes it possible to sumbit jobs from a machine which is
  not a submitting machine of the pool and possibly doesn't share a file system with these 
  machines. The fonction L{getJobScheduler} gets back a proxy of a L{JobScheduler} object. 
  The connection between the remote machine and the pool is done via ssh using port 
  forwarding (tunneling).
  The protocol used inside the tunnel is Pyro's protocol.
  '''
  
  def __init__(self,
               login, 
               password, 
               submitting_machine,
               local_process_src,
               log = ""):
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

    if not login:
      raise JobConnectionError("Remote connection requires a login")
    print 'login ' + login
    print 'submitting machine ' + submitting_machine

    def createTunnel(port, host, hostport, login, server_address, password):
      command = "ssh -N -L %s:%s:%s %s@%s" %(port, host, hostport, login, server_address)
      print "tunnel command: " + command
      child = DaemonicPexpect(command)#pexpect.spawn(command) 
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

    command = "ssh %s@%s python %s %s %s" %( login, 
                                          submitting_machine, 
                                          local_process_src, 
                                          pyro_objet_name,
                                          log) 
    print "local process command: " + command
    self.__job_process_child = DaemonicPexpect(command) #pexpect.spawn(command)
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

  def stop(self):
    '''
    For test purpose only !
    '''
    self.__connection_holder.stop()

  def getJobScheduler(self):
    return self.jobScheduler


class JobLocalConnection( object ):
  
  def __init__(self,
               local_process_src,
               log = ""):
    '''
    '''
    
    login = pwd.getpwuid(os.getuid())[0] 

    pyro_objet_name = "jobScheduler_" + login

    # run the local job process and get back the    #
    # JobScheduler and ConnectionChecker URIs       #

    command = "python " + local_process_src + " " + pyro_objet_name + " " + log
    print command
    self.__job_process_child = DaemonicPexpect(command)#pexpect.spawn(command)
    
    #fout = file('/neurospin/tmp/Soizic/jobFiles/mylog.txt','w')
    #self.__job_process_child.logfile = fout
    #self.__job_process_child.logfile = sys.stdout
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

  def stop(self):
    '''
    For test purpose only !
    '''
    self.__connection_holder.stop()

  def getJobScheduler(self):
    return self.jobScheduler




class FileTransferError( Exception ): pass

class FileTransfer( object ):

  def __init__( self, jobScheduler):
    '''
    @type  jobScheduler: L{JobScheduler} or proxy of L{JobScheduler} 
    '''
    self.jobScheduler = jobScheduler

  def transferInputFile(self, remote_input_file, disposal_timeout):
    '''
    An unique local path is generated and associated with the remote path. 
    Each remote files is copied to its associated local location.
    When the disposal timout will be past, and no exisiting job will 
    declare using the file as input, the files will be disposed. 
    
    @type  remote_input_file: string 
    @param remote_input_file: remote path of input file
    @type  disposalTimeout: int
    @param disposalTimeout: Number of hours before each local file is considered 
    to have been forgotten by the user. Passed that delay, and if no existing job 
    declares using the file as input, the local file and information 
    related to the transfer are disposed. 
    Default delay is 168 hours (7 days).
    @rtype: string 
    @return: local file path where the file were copied 
    '''
    raise Exception('FileTransfer is an abstract class. transferInputFile must be implemented in subclass')
     

  def transferOutputFile(self, local_file):
    '''
    Copy the local file to the associated remote file path. 
    The local file path must belong to the user's transfered files (ie belong to 
    the sequence returned by the L{getTransfers} method). 
    
    @type  local_file: string or sequence of string
    @param local_file: local file path(s) 
    '''
    raise Exception('FileTransfer is an abstract class. transferOutputFile must be implemented in subclass')
  
  

class LocalFileTransfer(FileTransfer):
  
    def transferInputFile(self, remote_input_file, disposal_timeout):
     
      local_input_file_path = self.jobScheduler.registerTransfer(remote_input_file, disposal_timeout)
      
      try:
        shutil.copy(remote_input_file,local_input_file_path)
      except IOError, e:
        raise FileTransferError("The input file was not transfered. %s: %s" %(type(e), e) )

      return local_input_file_path
    
    def transferOutputFile(self, local_file):
    
      local_file_path, remote_file_path, expiration_date = self.jobScheduler.getTransferInformation(local_file)
      
      try:
        shutil.copy(local_file_path,remote_file_path)
      except IOError, e:
        raise FileTransferError("The output file was not transfered back. %s: %s" %(type(e), e) )


class RemoteFileTransfer(FileTransfer):

    def transferInputFile(self, remote_input_file, disposal_timeout):
      local_input_file_path = self.jobScheduler.registerTransfer(remote_input_file, disposal_timeout)
      
      infile = open(remote_input_file)
      line = infile.readline()
      while line:
          self.jobScheduler.writeLine(line, local_input_file_path)
          line = infile.readline()
      infile.close()
  
      return local_input_file_path
    
    
    def transferOutputFile(self, local_file):
      local_file_path, remote_file_path, expiration_date = self.jobScheduler.getTransferInformation(local_file)
      
      outfile = open(remote_file_path, "w")
      line = self.jobScheduler.readline(local_file_path)
      while line:
          outfile.write(line)
          line = self.jobScheduler.readline(local_file_path)
      outfile.close()





class ConnectionChecker(object):
  
  def __init__(self, interval = 1, controlInterval = 3):
    self.connected = False
    self.lock = threading.RLock()
    self.interval = timedelta(seconds = interval)
    self.controlInterval = controlInterval
    self.lastSignal = datetime.now() - timedelta(days = 15)
    
    def controlLoop(self, control_interval):
      while True:
        with self.lock:
          ls = self.lastSignal
        delta = datetime.now()-ls
        if delta > self.interval * 3:
          self.connected = False
        else:
          self.connected = True
          time.sleep(control_interval)
        
    self.controlThread = threading.Thread(name = "connectionControlThread", 
                                          target = controlLoop, 
                                          args = (self, 4))
    self.controlThread.setDaemon(True)
    self.controlThread.start()
      
  def signalConnectionExist(self):
    with self.lock:
      #print "ConnectionChecker <= a signal was received"
      self.lastSignal = datetime.now()

  def isConnected(self):
    return self.connected
  



class ConnectionHolder(threading.Thread):
  def __init__(self, connectionChecker):
    threading.Thread.__init__(self)
    self.setDaemon(True)
    self.name = "connectionHolderThread"
    self.connectionChecker = connectionChecker
    self.interval = self.connectionChecker.interval.seconds
   
  def run(self):
    self.stopped = False
    while not self.stopped :
      #print "ConnectionHolder => signal"
      self.connectionChecker.signalConnectionExist()
      time.sleep(self.interval)

  def stop(self):
    self.stopped = True
    
      