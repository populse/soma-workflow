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
import Pyro.naming, Pyro.core
from Pyro.errors import NamingError
import socket
import pwd 
import os
import sys
import shutil
import subprocess
import logging
import select
import SocketServer

__docformat__ = "epytext en"


class JobConnectionError( Exception):
  def __init__(self, msg, logger = None):
    self.args = (msg,)
    if logger:
      logger.critical('EXCEPTION ' + msg)

'''
requirements: Pyro and Paramiko must be installed on the remote machine.

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
               resource_id,
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
    
    import paramiko #required only on client host
    
    if not login:
      raise JobConnectionError("Remote connection requires a login")
    print 'login ' + login
    print 'submitting machine ' + submitting_machine
  
    def searchAvailablePort():
      s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # Create a TCP socket
      s.bind(('localhost',0)) #try to bind to the port 0 so that the system will find an available port
      available_port = s.getsockname()[1]
      s.close()
      return available_port 
    
    pyro_objet_name = "jobScheduler_" + login
    
    # run the local job process and get back the    #
    # JobScheduler and ConnectionChecker URIs       #
    command = "python %s %s %s %s" %(local_process_src, resource_id, pyro_objet_name, log) 
    print "local process command: " + command
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.connect(hostname = submitting_machine, port=22, username=login, password=password)
    stdin, stdout, stderr = client.exec_command(command)
    line = stdout.readline()
    while line and line.split()[0] != pyro_objet_name:
      line = stdout.readline()
    if not line: raise JobConnectionError("Can't read jobScheduler Pyro uri.")
    job_scheduler_uri = line.split()[1] 
    line = stdout.readline()
    while line and line.split()[0] != "connectionChecker":
      line = stdout.readline()
    if not line: raise JobConnectionError("Can't read jobScheduler Pyro uri.")
    connection_checker_uri = line.split()[1] 
    client.close()
    
    print "job_scheduler_uri: " +  job_scheduler_uri
    print "connection_checker_uri: " +  connection_checker_uri
    local_pyro_daemon_port = Pyro.core.processStringURI(job_scheduler_uri).port
    print "Pyro object port: " + repr(local_pyro_daemon_port)
  
    # find an available port              #
    remote_pyro_daemon_port = searchAvailablePort()
    print "client pyro object port: " + repr(remote_pyro_daemon_port)

    
    # tunnel creation                      #
    self.__transport = paramiko.Transport((submitting_machine, 22))
    self.__transport.setDaemon(True)
    self.__transport.connect(username = login, password = password)
    tunnel = Tunnel(remote_pyro_daemon_port, submitting_machine, local_pyro_daemon_port, self.__transport) 
    tunnel.start()

    # create the proxies                     #
    self.jobScheduler = Pyro.core.getProxyForURI(job_scheduler_uri)
    connection_checker = Pyro.core.getAttrProxyForURI(connection_checker_uri)
  
    # setting the proxies to use the tunnel  #
    self.jobScheduler.URI.port = remote_pyro_daemon_port
    self.jobScheduler.URI.address = 'localhost'
    connection_checker.URI.port = remote_pyro_daemon_port
    connection_checker.URI.address = 'localhost'
    
    # waiting for the tunnel to be set
    tunnelSet = False
    maxattemps = 10
    attempts = 0
    while not tunnelSet and attempts <= maxattemps :
      try:
        attempts = attempts + 1
        print "Communication through the ssh tunnel. Attempt no " + repr(attempts) + "/" + repr(maxattemps)
        self.jobScheduler.jobs()
        connection_checker.isConnected()
      except Pyro.errors.ProtocolError, e: 
        print "-> Communication through ssh tunnel Failed"
        time.sleep(1)
      else:
        print "-> Communication through ssh tunnel OK"  
        tunnelSet = True
    
    if attempts > maxattemps: 
      raise JobConnectionError("The ssh tunnel could not be started within " + repr(maxattemps) + " seconds. The waiting time delay might need to be extended. See the configuration file")

    # create the connection holder objet for #
    # a clean disconnection in any case      #
    self.__connection_holder = ConnectionHolder(connection_checker)
    self.__connection_holder.start()

  def stop(self):
    '''
    For test purpose only !
    '''
    self.__connection_holder.stop()
    self.__transport.close()

  def getJobScheduler(self):
    return self.jobScheduler


class JobLocalConnection( object ):
  
  def __init__(self,
               local_process_src,
               resource_id, 
               log = ""):
    '''
    '''
    
    
    login = pwd.getpwuid(os.getuid())[0] 

    pyro_objet_name = "jobScheduler_" + login

    # run the local job process and get back the    #
    # JobScheduler and ConnectionChecker URIs       #

    command = "python %s %s %s %s" %(local_process_src, resource_id, pyro_objet_name, log) 
    print command
   
    local_job_process = subprocess.Popen(command, shell = True, stdout=subprocess.PIPE)
    
    line = local_job_process.stdout.readline()
    while line and line.split()[0] != pyro_objet_name:
      line = local_job_process.stdout.readline()
    if not line: raise JobConnectionError("Can't read jobScheduler Pyro uri.")
    job_scheduler_uri = line.split()[1] 
    line = local_job_process.stdout.readline()
    while line and line.split()[0] != "connectionChecker":
      line = local_job_process.stdout.readline()
    if not line: raise JobConnectionError("Can't read jobScheduler Pyro uri.")
    connection_checker_uri = line.split()[1] 
    
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




class TransferError( Exception ): pass

class Transfer( object ):

  def __init__( self, jobScheduler):
    '''
    @type  jobScheduler: L{JobScheduler} or proxy of L{JobScheduler} 
    '''
    self.jobScheduler = jobScheduler

  def send(self, local_path, remote_path, remote_paths=None):
    '''
    Transfer remote files and/or directories to the specified local location.
    - Case 1: the remote_path is a file path and remote_paths is None
        The local_path must be a file path.
        The remote file will be copied to the local_path.
    - Case 2: the remote_path is a directory and the remote_paths is None
        The local_path must be a directory path
        The whole remote directory will be copied to the local_path
    - Case3: the remote_paths is a sequence of file and/or directory paths.
        The local_path must be a directory path
        The files and/or directories in remote_paths are copied to the local_path
    '''
    
     # Case 1
    if os.path.isfile(remote_path) and not remote_paths:
      self.sendFile(local_path, remote_path)
      
    # Case 2
    elif os.path.isdir(remote_path) and not remote_paths:
      self.sendDirectory(local_path, remote_path)
    
    # Case 3
    elif remote_paths:
      for r_path in remote_paths:
        if os.path.isfile(r_path):
          self.sendFile(local_path, r_path, )
        if os.path.isdir(r_path):
          self.sendDirectory(local_path, r_path)
         
    else:
      raise TransferError("Transfer.send: the files were not transfered.")
    

  def retrieve(self, local_path, remote_path, remote_paths):
    '''
    Transfer local files and/or directories to the specified remote location.
    - Case 1: the remote_path is a file path and remote_paths is None
        The local_path must be a file path.
        The local file will be copied to the remote_path.
    - Case 2: the remote_path is a directory and the remote_paths is None
        The local_path must be a directory path
        The whole local directory will be copied to the remote_path
    - Case3: the remote_paths is a sequence of file and/or directory paths.
        The local_path must be a directory path
        The whole local directory will be copied to the base directory of remote_path.
    '''

    # Case 1 and 2
    if not remote_paths:
      self.retrieveFileOrDirectory(local_path, remote_path)
    
    # Case 3
    elif remote_paths:
      remote_directory = os.path.dirname(remote_path)
      self.retrieveDirectoryContents(local_path, remote_directory)
         
    else:
      raise TransferError("Transfer.retrieve: the files were not transfered.")
   

  def sendFile(self, local_file, remote_file):
    '''
    Equivalents to:    cp remote_file local_file
    
    @type local_file: file path
    @type remote_file: file path 
    '''
    raise TransferError('Transfer is an abstract class. sendFile must be implemented in subclass')
 
  def sendDirectory(local_dir, remote_dir):
    '''
    Equivalent to:    cp -r remote_dir local_dir
    
    @type local_dir: directory path
    @type remote_dir: directory path 
    '''
    raise TransferError('Transfer is an abstract class. sendFile must be implemented in subclass')
 
  def retrieveFileOrDirectory(self, local_path, remote_path):
    '''
    if local_path is a directory path, equivalents to:    
      cp -r local_path remote_path 
    if local_path is a file path, equivalent to:
      cp local_path remote_path
    
    @type local_dir: file or directory path
    @type remote_dir: file or directory path 
    '''
    raise TransferError('Transfer is an abstract class. retrieveFileOrDirectory must be implemented in subclass')
   
   
   
   
  def retrieveDirectoryContents(self,local_dir, remote_dir):
    '''
    Equivalent to:    cp -r local_dir/* remote_dir
    
    @type local_file: directory path
    @type remote_file: directory path 
    '''
    raise TransferError('Transfer is an abstract class. retrieveDirectoryContents must be implemented in subclass')
  

class LocalTransfer(Transfer):
  
  def sendFile(self, local_file, remote_file):
    try:
      shutil.copy(remote_file, local_file)
    except IOError, e:
      raise TransferError("The file was not transfered. %s: %s" %(type(e), e) )
    os.chmod(local_file, 0777)
  
  def retrieveFileOrDirectory(self, local_path, remote_path):
    try:
      if os.path.isfile(local_path): 
        shutil.copy(local_path, remote_path)
      if os.path.isfile(local_path): 
        shutil.copy(local_path, remote_path)
    except IOError, e:
      raise TransferError("The file or directory was not transfered back. %s: %s" %(type(e), e) )

  def sendDirectory(self, local_dir, remote_dir):
    try:
      shutil.copytree(remote_dir, local_dir)
    except IOError, e:
      raise TransferError("The directory was not transfered. %s: %s" %(type(e), e) )
    os.chmod(local_file, 0777)
  
  def retrieveDirectoryContents(self, local_dir, remote_dir):
    try:
      for name in os.listdir(local_dir):
        element = os.path.join(local_dir,name)
        if os.path.isfile(element):
          shutil.copy(element, remote_dir)
        elif os.path.isdir(element):
          shutil.copytree(element, remote_dir)
    except IOError, e:
      raise TransferError("The directory was not transfered back. %s: %s" %(type(e), e) )

class RemoteTransfer(Transfer):

  def sendFile(self, local_file, remote_file):
      
    infile = open(remote_file)
    line = infile.readline()
    while line:
        self.jobScheduler.writeLine(line, local_file)
        line = infile.readline()
    infile.close()
    self.jobScheduler.endTransfers()
  
  
  def retrieveFileOrDirectory(self, local_path, remote_path): 
    outfile = open(remote_path, "w")
    line = self.jobScheduler.readline(local_path)
    while line:
        outfile.write(line)
        line = self.jobScheduler.readline(local_path)
    outfile.close()
    self.jobScheduler.endTransfers()

  def sendDirectory(self, local_dir, remote_dir):
    #TBI!!
    pass
  
  def retrieveDirectoryContents(self, local_dir, remote_dir):
    #TBI!!
    pass



class ConnectionChecker(object):
  
  def __init__(self, interval = 2, controlInterval = 3):
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
          self.disconnectionCallback()
          self.connected = False
        else:
          self.connected = True
        time.sleep(control_interval)
        
    self.controlThread = threading.Thread(name = "connectionControlThread", 
                                          target = controlLoop, 
                                          args = (self, controlInterval))
    self.controlThread.setDaemon(True)
    self.controlThread.start()
      
  def signalConnectionExist(self):
    with self.lock:
      #print "ConnectionChecker <= a signal was received"
      self.lastSignal = datetime.now()

  def isConnected(self):
    return self.connected
  
  def disconnectionCallback(self):
    pass
  
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
    
      
class Tunnel(threading.Thread):
  
  class ForwardServer (SocketServer.ThreadingTCPServer):
    daemon_threads = True
    allow_reuse_address = True
    
  class Handler (SocketServer.BaseRequestHandler):
    
    def setup(self):
      self.logger = logging.getLogger('ljp.connection')
      self.logger.debug('Setup : %s %d' %(repr(self.chain_host), self.chain_port))
      try:
        self.__chan = self.ssh_transport.open_channel('direct-tcpip',
                                              (self.chain_host, self.chain_port),
                                              self.request.getpeername())
      except Exception, e:
        raise JobConnectionError('Incoming request to %s:%d failed: %s' %(self.chain_host,self.chain_port,repr(e)), self.logger)
  
      if self.__chan is None:
        raise JobConnectionError('Incoming request to %s:%d was rejected by the SSH server.' %
                (self.chain_host, self.chain_port), self.logger)
  
      self.logger.info('Connected!  Tunnel open %r -> %r -> %r' %(self.request.getpeername(), self.__chan.getpeername(), (self.chain_host, self.chain_port)))
      #print 'Connected!  Tunnel open %r -> %r -> %r' %(self.request.getpeername(), self.__chan.getpeername(), (self.chain_host, self.chain_port))
      
    
    def handle(self):
      self.logger.debug('Handle : %s %d' %(repr(self.chain_host), self.chain_port))
      while True:
        r, w, x = select.select([self.request, self.__chan], [], [])
        if self.request in r:
          data = self.request.recv(1024)
          if len(data) == 0: break
          self.__chan.send(data)
        if self.__chan in r:
          data = self.__chan.recv(1024)
          if len(data) == 0: break
          self.request.send(data)
      
    def finish(self):
      self.logger.info('Tunnel closed from %r' %(self.request.getpeername(),))
      #print 'Tunnel closed from %r' %(self.request.getpeername(),)
      self.__chan.close()
      self.request.close()
    
    
  def __init__(self, port, host, hostport, transport):
    threading.Thread.__init__(self)
    self.__port = port 
    self.__host = host
    self.__hostport = hostport
    self.__transport = transport
    self.setDaemon(True)

  
  def run(self):
    host = self.__host
    hostport = self.__hostport
    transport = self.__transport
    port = self.__port
    class SubHander (Tunnel.Handler):
        chain_host = host
        chain_port = hostport
        ssh_transport = transport
    try:
      Tunnel.ForwardServer(('', port), SubHander).serve_forever()
    except KeyboardInterrupt:
      print 'tunnel %d:%s:%d stopped !' %(port, host, hostport)
      
      