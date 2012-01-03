
from __future__ import with_statement

'''
@author: Soizic Laguitton
@organization: U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''

''' 
soma-workflow local and remote connections from client to computing resource.

The RemoteConnection and LocalConnection instances should be created on the 
client side. Their constructor creates a WorkflowEngine object on the 
computing resource and setup the pyro communicaiton with it. The method
getWorkflowEngine get back a pyro proxy of the remote WorkflowEngine object.
The RemoteConnection and object contains a thead which signals to the 
WorkflowEngine object at regular time interval that the client is still
connected.

requirements: Pyro and Paramiko must be installed on the remote machine.
'''



#-------------------------------------------------------------------------------
# Imports
#-------------------------------------------------------------------------------

from datetime import datetime
from datetime import timedelta
import threading
import time
import socket
import pwd 
import os
import select
import SocketServer

from soma.workflow.errors import ConnectionError

__docformat__ = "epytext en"

#-------------------------------------------------------------------------------
# Classes and functions
#-------------------------------------------------------------------------------

class RemoteConnection( object ):
  '''
  Remote version of the connection.
  The WorkflowControler object is created using ssh with paramiko.
  The communication between the client and the computing resource is done with
  Pyro inside a ssh port forwarding tunnel.
  '''
  
  def __init__(self,
               login, 
               password, 
               cluster_address,
               submitting_machine,
               resource_id,
               log="",
               rsa_key_pass=None):
    '''
    @type  login: string
    @param login: user's login on the computing resource 
    @type  password: string
    @param password: associted password
    @type  submitting_machine: string
    @param submitting_machine: address of a submitting machine of the computing
                               resource.
    '''
   
    # required in the remote connection mode
    import paramiko 
    import Pyro.core
    from Pyro.errors import ConnectionClosedError
    

    if not login:
      raise ConnectionError("Remote connection requires a login")
   
    def searchAvailablePort():
      s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # Create a TCP socket
      s.bind(('localhost',0)) #try to bind to the port 0 so that the system 
                              #will find an available port
      available_port = s.getsockname()[1]
      s.close()
      return available_port 
    
    pyro_objet_name = "workflow_engine_" + login
    
    # run the workflow engine process and get back the    #
    # WorkflowEngine and ConnectionChecker URIs       #
    command = "python -m soma.workflow.start_workflow_engine %s %s %s" %(resource_id, pyro_objet_name, log) 
    print "start engine command: " + command
    try:
      client = paramiko.SSHClient()
      client.set_missing_host_key_policy(paramiko.AutoAddPolicy()) 
      client.load_system_host_keys()
      client.connect(hostname = cluster_address, port=22, username=login, password=password)
      stdin, stdout, stderr = client.exec_command(command)
    except paramiko.AuthenticationException, e:
      raise ConnectionError("The authentification failed. %s" %(e))
    except Exception, e:
      raise ConnectionError("Can not use ssh to log on the remote machine." 
                            " %s" %(e))

    line = stdout.readline()
    stdout_content = line
    while line and line.split()[0] != pyro_objet_name:
      line = stdout.readline()
      stdout_content = stdout_content + "\n" + line

    if not line: # A problem occured while starting the engine.
      line = stderr.readline()
      stderr_content = line
      while line:
        line = stderr.readline()
        stderr_content = stderr_content + "\n" + line
      raise ConnectionError("A problem occured while starting the engine "
                            "process.\n" 
                            "Engine process standard output:\n" 
                            "\n" + stdout_content + \
                            "Engine process standard error:\n" 
                            "\n" + stderr_content )

    workflow_engine_uri = line.split()[1] 
    line = stdout.readline()
    stdout_content = stdout_content + "\n" + line
    while line and line.split()[0] != "connection_checker":
      line = stdout.readline()
      stdout_content = stdout_content + "\n" + line

    if not line: # A problem occured while starting the engine.
      line = stderr.readline()
      stderr_content = line
      while line:
        line = stderr.readline()
        stderr_content = stderr_content + "\n" + line
      raise ConnectionError("A problem occured while starting the engine "
                            "process.\n" 
                            "Engine process standard output:\n" 
                            "\n" + stdout_content + \
                            "Engine process standard error:\n" 
                            "\n" + stderr_content )

    connection_checker_uri = line.split()[1] 
    line = stdout.readline()
    stdout_content = stdout_content + "\n" + line
    while line and line.split()[0] != "configuration":
      line = stdout.readline()
      stdout_content = stdout_content + "\n" + line

    if not line: # A problem occured while starting the engine.
      line = stderr.readline()
      stderr_content = line
      while line:
        line = stderr.readline()
        stderr_content = stderr_content + "\n" + line
      raise ConnectionError("A problem occured while starting the engine "
                            "process.\n" 
                            "Engine process standard output:\n" 
                            "\n" + stdout_content + \
                            "Engine process standard error:\n" 
                            "\n" + stderr_content )
    configuration_uri = line.split()[1] 
    client.close()
    
    print "workflow_engine_uri: " +  workflow_engine_uri
    print "connection_checker_uri: " +  connection_checker_uri
    print "configuration_uri: " + configuration_uri
    engine_pyro_daemon_port = Pyro.core.processStringURI(workflow_engine_uri).port
    print "Pyro object port: " + repr(engine_pyro_daemon_port)
  
    # find an available port              #
    client_pyro_daemon_port = searchAvailablePort()
    print "client pyro object port: " + repr(client_pyro_daemon_port)

    
    # tunnel creation                      #
    try:
      self.__transport = paramiko.Transport((cluster_address, 22))
      self.__transport.setDaemon(True)
      self.__transport.connect(username = login, password = password)
      if not password:
        rsa_file_path = os.path.join(os.environ['HOME'], '.ssh', 'id_rsa')
        print "reading RSA key in " + repr(rsa_file_path)
        if rsa_key_pass:
          key = paramiko.RSAKey.from_private_key_file(rsa_file_path, 
                                                      password=rsa_key_pass)
        else:
          key = paramiko.RSAKey.from_private_key_file(rsa_file_path)
        self.__transport.auth_publickey(login, key)
        #TBI DSA Key => see paramamiko/demos/demo.py for an example
      print "tunnel creation " + repr(login) + "@" + repr(cluster_address)
      print "   port: " + repr(client_pyro_daemon_port) + " host: " + \
      repr(submitting_machine) + " host port: " + repr(engine_pyro_daemon_port) 

      tunnel = Tunnel(client_pyro_daemon_port, submitting_machine, engine_pyro_daemon_port, self.__transport) 
      tunnel.start()

    except paramiko.AuthenticationException, e:
      raise ConnectionError("The authentification failed while "
                            "creating the ssh tunnel. %s" %(e))
    except Exception, e:
      raise ConnectionError("The ssh communication tunnel could not be created." 
                            "%s: %s" %(type(e), e))

    # create the proxies                     #
    self.workflow_engine = Pyro.core.getProxyForURI(workflow_engine_uri)
    connection_checker = Pyro.core.getAttrProxyForURI(connection_checker_uri)
    self.configuration = Pyro.core.getAttrProxyForURI(configuration_uri)
  
    # setting the proxies to use the tunnel  #
    self.workflow_engine.URI.port = client_pyro_daemon_port
    self.workflow_engine.URI.address = 'localhost'
    connection_checker.URI.port = client_pyro_daemon_port
    connection_checker.URI.address = 'localhost'
    
    # waiting for the tunnel to be set
    tunnelSet = False
    maxattemps = 3
    attempts = 0
    while not tunnelSet and attempts < maxattemps :
      try:
        attempts = attempts + 1
        print "Communication through the ssh tunnel. Attempt no " + repr(attempts) + "/" + repr(maxattemps)
        self.workflow_engine.jobs()
        connection_checker.isConnected()
      except Pyro.errors.ProtocolError, e: 
        print "-> Communication through ssh tunnel Failed. %s: %s" %(type(e), e)
        time.sleep(1)
      except Exception, e: 
        print "-> Communication through ssh tunnel Failed. %s: %s" %(type(e), e)
        time.sleep(1)

      else:
        print "-> Communication through ssh tunnel OK"  
        tunnelSet = True
    
    if attempts > maxattemps: 
      raise ConnectionError("The ssh tunnel could not be started within" \
                             + repr(maxattemps) + " seconds.")

    # create the connection holder objet for #
    # a clean disconnection in any case      #
    self.__connection_holder = ConnectionHolder(connection_checker)
    self.__connection_holder.start()

  def isValid(self):
    return self.__connection_holder.isAlive()

  def stop(self):
    '''
    For test purpose only !
    '''
    self.__connection_holder.stop()
    self.__transport.close()

  def get_workflow_engine(self):
    return self.workflow_engine

  def get_configuration(self):
    return self.configuration


class LocalConnection( object ):

  '''
  Local version of the connection.
  The worjkflow engine process is created using subprocess.
  '''
  
  def __init__(self,
               resource_id, 
               log = ""):

    # required in the local connection mode
    import Pyro.core
    from Pyro.errors import ConnectionClosedError 
    import subprocess

    login = pwd.getpwuid(os.getuid())[0] 
    pyro_objet_name = "workflow_engine_" + login

    # run the workflow engine process and get back the  
    # workflow_engine and ConnectionChecker URIs  
    #command = "python -m cProfile -o /home/soizic/profile/profile /home/soizic/svn/brainvisa/source/soma/soma-workflow/trunk/python/soma/workflow/start_workflow_engine.py %s %s %s" %( 
                                     #resource_id, 
                                     #pyro_objet_name, 
                                     #log) 
    command = "python -m soma.workflow.start_workflow_engine %s %s %s" %( 
                                     resource_id, 
                                     pyro_objet_name, 
                                     log) 
    #command = "rpdb2 -p Soizic -d /home/soizic/svn/brainvisa/source/soma/soma-workflow/trunk/python/soma/workflow/start_workflow_engine.py %s %s %s" %( 
                                     #resource_id, 
                                     #pyro_objet_name, 
                                     #log) 

    print command
   
    engine_process = subprocess.Popen(command, 
                                      shell = True, 
                                      stdout=subprocess.PIPE,
                                      stderr=subprocess.PIPE)
    
    line = engine_process.stdout.readline()
    stdout_content = line
    while line and line.split()[0] != pyro_objet_name:
      line = engine_process.stdout.readline()
      stdout_content = stdout_content + "\n" + line
   
    if not line: # A problem occured while starting the engine.
      line = engine_process.stderr.readline()
      stderr_content = line
      while line:
        line = engine_process.stderr.readline()
        stderr_content = stderr_content + "\n" + line
      raise ConnectionError("A problem occured while starting the engine "
                            "process.\n" 
                            "Engine process standard output:\n"
                            "\n" + stdout_content + \
                            "Engine process standard error:\n" 
                            "\n" + stderr_content)

    workflow_engine_uri = line.split()[1]
    line = engine_process.stdout.readline()
    stdout_content = stdout_content + "\n" + line
    while line and line.split()[0] != "connection_checker":
      line = engine_process.stdout.readline()
      stdout_content = stdout_content + "\n" + line

    if not line: # A problem occured while starting the engine.
      line = engine_process.stderr.readline()
      stderr_content = line
      while line:
        line = engine_process.stderr.readline()
        stderr_content = stderr_content + "\n" + line
      raise ConnectionError("A problem occured while starting the engine "
                            "process.\n" 
                            "Engine process standard output:\n" 
                            "\n" + stdout_content +\
                            "Engine process standard error:\n" 
                            "\n" + stderr_content)

    connection_checker_uri = line.split()[1] 
    line = engine_process.stdout.readline()
    stdout_content = stdout_content + "\n" + line
    while line and line.split()[0] != "configuration":
      line = engine_process.stdout.readline()
      stdout_content = stdout_content + "\n" + line

    if not line: # A problem occured while starting the engine.
      line = engine_process.stderr.readline()
      stderr_content = line
      while line:
        line = engine_process.stderr.readline()
        stderr_content = stderr_content + "\n" + line
      raise ConnectionError("A problem occured while starting the engine "
                            "process.\n" 
                            "Engine process standard output:\n" 
                            "\n" + stdout_content +\
                            "Engine process standard error:\n" 
                            "\n" + stderr_content)

    configuration_uri = line.split()[1]

    print "workflow_engine_uri: " +  workflow_engine_uri
    print "connection_checker_uri: " +  connection_checker_uri
    print "configuration_uri: " + configuration_uri

    # create the proxies                     #
    self.workflow_engine = Pyro.core.getProxyForURI(workflow_engine_uri)
    connection_checker = Pyro.core.getAttrProxyForURI(connection_checker_uri)
    self.configuration = Pyro.core.getAttrProxyForURI(configuration_uri)
  
    # create the connection holder objet for #
    # a clean disconnection in any case      #
    self.__connection_holder = ConnectionHolder(connection_checker)
    self.__connection_holder.start()

  def isValid(self):
    return self.__connection_holder.isAlive()

  def stop(self):
    '''
    For test purpose only !
    '''
    self.__connection_holder.stop()

  def get_workflow_engine(self):
    return self.workflow_engine

  def get_configuration(self):
    return self.configuration

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
    from Pyro.errors import ConnectionClosedError
    self.stopped = False
    while not self.stopped :
      #print "ConnectionHolder => signal"
      try:
        self.connectionChecker.signalConnectionExist()
      except ConnectionClosedError, e:
        print "Connection closed"
        break
      time.sleep(self.interval)

  def stop(self):
    self.stopped = True
    
      
class Tunnel(threading.Thread):
  
  class ForwardServer (SocketServer.ThreadingTCPServer):
    daemon_threads = True
    allow_reuse_address = True
    
  class Handler (SocketServer.BaseRequestHandler):
    
    def setup(self):
      #print 'Setup : %s %d' %(repr(self.chain_host), self.chain_port)
      try:
        self.__chan = self.ssh_transport.open_channel('direct-tcpip',
                                              (self.chain_host, self.chain_port),
                                              self.request.getpeername())
      except Exception, e:
        raise ConnectionError('Incoming request to %s:%d failed: %s' %(self.chain_host,self.chain_port,repr(e)))
  
      if self.__chan is None:
        raise ConnectionError('Incoming request to %s:%d was rejected by the SSH server.' %
                (self.chain_host, self.chain_port))
  
      print 'Connected!  Tunnel open %r -> %r -> %r' %(self.request.getpeername(), self.__chan.getpeername(), (self.chain_host, self.chain_port))
      
    
    def handle(self):
      #print 'Handle : %s %d' %(repr(self.chain_host), self.chain_port)
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
      print 'Tunnel closed from %r' %(self.request.getpeername(),)
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
    except Exception, e:
      print 'Tunnel Error. %s: %s' %(type(e), e)
      
      