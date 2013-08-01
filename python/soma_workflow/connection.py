
from __future__ import with_statement

'''
@author: Soizic Laguitton

@organization: I2BM, Neurospin, Gif-sur-Yvette, France
@organization: CATI, France
@organization: U{IFR 49<http://www.ifr49.org>}

@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''


#-------------------------------------------------------------------------------
# Imports
#-------------------------------------------------------------------------

from datetime import datetime
from datetime import timedelta
import threading
import time
import socket
import getpass
import os
import select
import SocketServer


from soma_workflow.errors import ConnectionError



def ReadOutput(stdout, tag=None, num_line_stdout=-1):
    is_limit_stdout = False

    if num_line_stdout != -1:
        is_limit_stdout = True

    std_out_lines = list()

    if (is_limit_stdout == False
       or (is_limit_stdout == True and num_line_stdout != 0)
            ):
        if tag:
            sline = stdout.readline()
            while sline and sline != tag+'\n':
                sline = stdout.readline()

        sline = stdout.readline()
        while (sline and num_line_stdout != 0):

            std_out_lines.append(sline.strip())
            num_line_stdout = num_line_stdout-1
            if(num_line_stdout == 0):
                break

            sline = stdout.readline()

    return std_out_lines


def SSHExecCmd(sshcommand,
               userid,
               ip_address_or_domain,
               userpw='',
               wait_output=True,
               sshport=22,
               isNeedErr=False,
               num_line_stdout=-1,  # How many stdout or stderr lines to read
               num_line_stderr=-1):  # -1 means unlimited

    if wait_output:
        tag = '----xxxx=====start to exec=====xxxxx----'
        sshcommand = "echo %s && %s" % (tag, sshcommand)

    
    is_limit_stdout = False
    is_limit_stderr = False

    if num_line_stdout != -1:
        is_limit_stdout = True
    if num_line_stderr != -1:
        is_limit_stderr = True

    stdin = []
    stdout = []
    stderr = []

    std_out_lines = []
    std_err_lines = []

    import paramiko

    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.load_system_host_keys()

        client.connect(hostname=ip_address_or_domain,
                       port=sshport,
                       username=userid,
                       password=userpw)

        stdin, stdout, stderr = client.exec_command(sshcommand)

    except paramiko.AuthenticationException, e:
        print   "The authentification failed. %s. " \
                "Please check your user and password. " \
                "You can test the connection in terminal with " \
                "command: ssh -p %s %s@%s" % (
                    e, sshport, userid, ip_address_or_domain)
        raise e
    except Exception, e:
        print   "Can not use ssh to log on the remote machine. "\
                "Please Make sure your network can be connected %s. "\
                "You can test the connection in terminal with "\
                "command: ssh -p %s %s@%s" % (
                    e, sshport, userid, ip_address_or_domain)
        raise e

    if wait_output:
        std_out_lines = ReadOutput(stdout, tag, num_line_stdout)
        if isNeedErr:
            std_err_lines = ReadOutput(stderr, None, num_line_stderr)

    client.close()

    if isNeedErr:
        return (std_out_lines, std_err_lines)
    else:
        return std_out_lines


def check_if_soma_wf_cr_on_server(
                            userid,
                            ip_address_or_domain,
                            userpw='',
                            sshport=22):
    command = "python -c 'import soma_workflow.check_requirement'"
    (std_out_lines, std_err_lines) = SSHExecCmd(
        command, 
        userid, 
        ip_address_or_domain, 
        userpw, 
        wait_output=True, 
        isNeedErr=True,
        sshport=sshport)

    if len(std_err_lines) > 0:
        return False
    
    return True
    
def check_if_ctype_drmaa_on_server(
                            userid,
                            ip_address_or_domain,
                            userpw='',
                            sshport=22):
    command = "python -m 'soma_workflow.check_requirement.drmaa'"
    (std_out_lines, std_err_lines) = SSHExecCmd(
        command, 
        userid, 
        ip_address_or_domain, 
        userpw, 
        wait_output=True, 
        isNeedErr=True,
        sshport=sshport)

    # print repr(std_out_lines)
    
    if len(std_out_lines) == 1:
        if std_out_lines[0] == "True":
            return True
    
    return False
    
def check_if_somawf_on_server(
                            userid,
                            ip_address_or_domain,
                            userpw='',
                            sshport=22):
    command = "python -c 'import soma_workflow'"
    (std_out_lines, std_err_lines) = SSHExecCmd(
        command, 
        userid, 
        ip_address_or_domain, 
        userpw, 
        wait_output=True, 
        isNeedErr=True,
        sshport=sshport)

    if len(std_err_lines) > 0:
        return False
    
    return True

def check_if_somawfdb_on_server(
                            ResName,
                            userid,
                            ip_address_or_domain,
                            userpw='',
                            sshport=22):

    command = "ps -ef | grep 'python -m soma_workflow.start_database_server'"\
    " | grep '%s' | grep -v grep | awk '{print $2}'" % (userid)

    std_out_lines = SSHExecCmd(
        command, 
        userid, 
        ip_address_or_domain, 
        userpw, 
        wait_output=True, 
        sshport=sshport)

    if len(std_out_lines) == 0:
        return False
    else:
        return True


def searchAvailablePort():
    s = socket.socket(
        socket.AF_INET, socket.SOCK_STREAM)  # Create a TCP socket
    s.bind(('localhost', 0))  # try to bind to the port 0 so that the system
                            # will find an available port
    available_port = s.getsockname()[1]
    s.close()
    return available_port


#-------------------------------------------------------------------------------
# Classes and functions
#-------------------------------------------------------------------------


class RemoteConnection(object):
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
        from paramiko.file import BufferedFile
        import Pyro.core
        from Pyro.errors import ConnectionClosedError

        if not login:
            raise ConnectionError("Remote connection requires a login")
        

        pyro_objet_name = "workflow_engine_" + login

        if not check_if_somawf_on_server(
                                        login, 
                                        cluster_address, 
                                        password):
            raise ConnectionError("Cannot find soma-workflow on %s ."\
                                  "Please verify if your PYTHONPATH "\
                                  "includes the soma-workflow."\
                                  % (cluster_address))

       
        if not check_if_soma_wf_cr_on_server(
                                        login, 
                                        cluster_address, 
                                        password):
            raise ConnectionError("Cannot find "\
                "soma_workflow.check_requirement on %s ."\
                "Please update your soma-workflow on %s."\
                                  % (cluster_address,cluster_address))
                                  
        if not check_if_ctype_drmaa_on_server(
                                        login, 
                                        cluster_address, 
                                        password):
            raise ConnectionError("Cannot find "\
                "drmaa libary on %s ."\
                "Please verify your drmaa libary on %s. "\
                "Or setup up enviroment variable DRMAA_LIBRARY_PATH."\
                                  % (cluster_address,cluster_address))
                                  
                                                
        if not check_if_somawfdb_on_server(
                                            resource_id, 
                                            login, 
                                            cluster_address, 
                                            password):
           command = "python -m soma_workflow.start_database_server %s & bg"%(resource_id)
           SSHExecCmd(
              command,
              login,
              cluster_address,
              userpw=password,
              wait_output=False)
 
         


        # run the workflow engine process and get back the    #
        # WorkflowEngine and ConnectionChecker URIs       #
        command = "python -m soma_workflow.start_workflow_engine"\
                  " %s %s %s" % (resource_id, pyro_objet_name, log)
        
        print "start engine command: "\
              "ssh %s@%s %s" % (login, cluster_address, command)

        (std_out_lines) = SSHExecCmd(
            command,
            login,
            cluster_address,
            userpw=password,
            wait_output=True,
            sshport=22,
            num_line_stdout=3)
        
        
        # print "std_out_lines="+repr(std_out_lines)

        workflow_engine_uri = None
        connection_checker_uri = None
        configuration_uri = None

        for std_out_line in std_out_lines:
            if std_out_line.split()[0] == pyro_objet_name:
                workflow_engine_uri = std_out_line.split()[1]
            elif std_out_line.split()[0] == "connection_checker":
                connection_checker_uri = std_out_line.split()[1]
            elif std_out_line.split()[0] == "configuration":
                configuration_uri = std_out_line.split()[1]

        if (not configuration_uri or
            not connection_checker_uri or
                not configuration_uri):
            raise ConnectionError("A problem occured while starting the engine "
                                  "process on the remote machine " +
                                  repr(cluster_address) + "\n"
                                  "**More details:**\n"
                                  "**Start engine process command line:** \n"
                                  "\n" + command + "\n\n"
                                  "**Engine process standard output:** \n"
                                  "\n" + repr(std_out_lines))

        # print "workflow_engine_uri: " +  workflow_engine_uri
        # print "connection_checker_uri: " +  connection_checker_uri
        # print "configuration_uri: " + configuration_uri
        engine_pyro_daemon_port = Pyro.core.processStringURI(
            workflow_engine_uri).port
        # print "Pyro object port: " + repr(engine_pyro_daemon_port)

        # find an available port              #
        client_pyro_daemon_port = searchAvailablePort()
        # print "client pyro object port: " + repr(client_pyro_daemon_port)

        # tunnel creation                      #
        try:
            self.__transport = paramiko.Transport((cluster_address, 22))
            self.__transport.setDaemon(True)
            self.__transport.connect(username=login, password=password)
            if not password:
                rsa_file_path = os.path.join(
                    os.environ['HOME'], '.ssh', 'id_rsa')
                print "reading RSA key in " + repr(rsa_file_path)
                if rsa_key_pass:
                    key = paramiko.RSAKey.from_private_key_file(
                                        rsa_file_path,
                                        password=rsa_key_pass)
                else:
                    key = paramiko.RSAKey.from_private_key_file(rsa_file_path)
                self.__transport.auth_publickey(login, key)
                # TBI DSA Key => see paramamiko/demos/demo.py for an example
            print "tunnel creation " + str(login) + "@" + cluster_address + \
                  "   port: " + repr(client_pyro_daemon_port) + " host: " + \
                  str(submitting_machine) + " host port: " + str(
                      engine_pyro_daemon_port)

            tunnel = Tunnel(client_pyro_daemon_port,
                            submitting_machine,
                            engine_pyro_daemon_port,
                            self.__transport)

            tunnel.start()

        except paramiko.AuthenticationException, e:
            raise ConnectionError("The authentification failed while "
                                  "creating the ssh tunnel. %s" % (e))
        except Exception, e:
            raise ConnectionError("The ssh communication tunnel could not be created."
                                  "%s: %s" % (type(e), e))

        # create the proxies                     #
        self.workflow_engine = Pyro.core.getProxyForURI(workflow_engine_uri)
        connection_checker = Pyro.core.getAttrProxyForURI(
            connection_checker_uri)
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
        while not tunnelSet and attempts < maxattemps:
            try:
                attempts = attempts + 1
                print "Communication through the ssh tunnel. Attempt no " + \
                repr(attempts) + "/" + repr(maxattemps)
                self.workflow_engine.jobs()
                connection_checker.isConnected()
            except Pyro.errors.ProtocolError, e:
                print "-> Communication through ssh tunnel Failed. %s: %s" \
                % (type(e), e)
                time.sleep(1)
            except Exception, e:
                print "-> Communication through ssh tunnel Failed. %s: %s" \
                % (type(e), e)
                time.sleep(1)

            else:
                print "-> Communication through ssh tunnel OK"
                tunnelSet = True

        if attempts > maxattemps:
            raise ConnectionError("The ssh tunnel could not be started within"
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


class LocalConnection(object):

    '''
    Local version of the connection.
    The worjkflow engine process is created using subprocess.
    '''

    def __init__(self,
                 resource_id,
                 log=""):

        # required in the local connection mode
        import Pyro.core
        from Pyro.errors import ConnectionClosedError
        import subprocess

        login = getpass.getuser()
        pyro_objet_name = "workflow_engine_" + login

        # run the workflow engine process and get back the
        # workflow_engine and ConnectionChecker URIs
        # command = "python -m cProfile -o /home/soizic/profile/profile /home/soizic/svn/brainvisa/source/soma/soma-workflow/trunk/python/soma_workflow/start_workflow_engine.py %s %s %s" %(
                                         # resource_id,
                                         # pyro_objet_name,
                                         # log)
        command = "python -m soma_workflow.start_workflow_engine %s %s %s" % (
            resource_id,
            pyro_objet_name,
            log)
        # command = "rpdb2 -p Soizic -d /home/soizic/svn/brainvisa/source/soma/soma-workflow/trunk/python/soma_workflow/start_workflow_engine.py %s %s %s" %(
                                         # resource_id,
                                         # pyro_objet_name,
                                         # log)

        # print command

        engine_process = subprocess.Popen(command,
                                          shell=True,
                                          stdout=subprocess.PIPE,
                                          stderr=subprocess.PIPE)

        line = engine_process.stdout.readline()
        stdout_content = line
        while line and line.split()[0] != pyro_objet_name:
            line = engine_process.stdout.readline()
            stdout_content = stdout_content + "\n" + line

        if not line:  # A problem occured while starting the engine.
            line = engine_process.stderr.readline()
            stderr_content = line
            while line:
                line = engine_process.stderr.readline()
                stderr_content = stderr_content + "\n" + line
            raise ConnectionError("A problem occured while starting the engine "
                                  "process on the local machine. \n"
                                  "**More details:**\n"
                                  "**Start engine process command line:** \n"
                                  "\n" + command + "\n\n"
                                  "**Engine process standard output:** \n"
                                  "\n" + stdout_content +
                                  "**Engine process standard error:** \n"
                                  "\n" + stderr_content)
        workflow_engine_uri = line.split()[1]
        line = engine_process.stdout.readline()
        stdout_content = stdout_content + "\n" + line
        while line and line.split()[0] != "connection_checker":
            line = engine_process.stdout.readline()
            stdout_content = stdout_content + "\n" + line

        if not line:  # A problem occured while starting the engine.
            line = engine_process.stderr.readline()
            stderr_content = line
            while line:
                line = engine_process.stderr.readline()
                stderr_content = stderr_content + "\n" + line
            raise ConnectionError("A problem occured while starting the engine "
                                  "process on the local machine. \n"
                                  "**More details:**\n"
                                  "**Start engine process command line:** \n"
                                  "\n" + command + "\n\n"
                                  "**Engine process standard output:** \n"
                                  "\n" + stdout_content +
                                  "**Engine process standard error:** \n"
                                  "\n" + stderr_content)

        connection_checker_uri = line.split()[1]
        line = engine_process.stdout.readline()
        stdout_content = stdout_content + "\n" + line
        while line and line.split()[0] != "configuration":
            line = engine_process.stdout.readline()
            stdout_content = stdout_content + "\n" + line

        if not line:  # A problem occured while starting the engine.
            line = engine_process.stderr.readline()
            stderr_content = line
            while line:
                line = engine_process.stderr.readline()
                stderr_content = stderr_content + "\n" + line
            raise ConnectionError("A problem occured while starting the engine "
                                  "process on the local machine. \n"
                                  "**More details:**\n"
                                  "**Start engine process command line:** \n"
                                  "\n" + command + "\n\n"
                                  "**Engine process standard output:** \n"
                                  "\n" + stdout_content +
                                  "**Engine process standard error:** \n"
                                  "\n" + stderr_content)
        configuration_uri = line.split()[1]

        print "workflow_engine_uri: " + workflow_engine_uri
        print "connection_checker_uri: " + connection_checker_uri
        print "configuration_uri: " + configuration_uri

        # create the proxies                     #
        self.workflow_engine = Pyro.core.getProxyForURI(workflow_engine_uri)
        connection_checker = Pyro.core.getAttrProxyForURI(
            connection_checker_uri)
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

    def __init__(self, interval=2, controlInterval=3):
        self.connected = False
        self.lock = threading.RLock()
        self.interval = timedelta(seconds=interval)
        self.controlInterval = controlInterval
        self.lastSignal = datetime.now() - timedelta(days=15)

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

        self.controlThread = threading.Thread(name="connectionControlThread",
                                              target=controlLoop,
                                              args=(self, controlInterval))
        self.controlThread.setDaemon(True)
        self.controlThread.start()

    def signalConnectionExist(self):
        with self.lock:
            # print "ConnectionChecker <= a signal was received"
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
        while not self.stopped:
            # print "ConnectionHolder => signal"
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
            # print 'Setup : %s %d' %(repr(self.chain_host), self.chain_port)
            try:
                self.__chan = self.ssh_transport.open_channel('direct-tcpip',
                                                             (self.chain_host,
                                                              self.chain_port),
                                                              self.request.getpeername())
            except Exception, e:
                raise ConnectionError('Incoming request to %s:%d failed: %s' % (
                    self.chain_host, self.chain_port, repr(e)))

            if self.__chan is None:
                raise ConnectionError('Incoming request to %s:%d was rejected by the SSH server.' %
                                     (self.chain_host, self.chain_port))

            print 'Connected!  Tunnel open %r -> %r -> %r' % (
            self.request.getpeername(), 
            self.__chan.getpeername(), 
            (self.chain_host, self.chain_port))

        def handle(self):
            # print 'Handle : %s %d' %(repr(self.chain_host), self.chain_port)
            while True:
                r, w, x = select.select([self.request, self.__chan], [], [])
                if self.request in r:
                    data = self.request.recv(1024)
                    if len(data) == 0:
                        break
                    self.__chan.send(data)
                if self.__chan in r:
                    data = self.__chan.recv(1024)
                    if len(data) == 0:
                        break
                    self.request.send(data)

        def finish(self):
            print 'Tunnel closed from %r' % (self.request.getpeername(),)
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
            print 'tunnel %d:%s:%d stopped !' % (port, host, hostport)
        except Exception, e:
            print 'Tunnel Error. %s: %s' % (type(e), e)
