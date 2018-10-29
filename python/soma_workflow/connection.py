from __future__ import with_statement, print_function

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
import re
import random
import errno
import logging
import sys
import io
import traceback

try:
    import socketserver # python3
except ImportError:
    import SocketServer as socketserver # python 2

from soma_workflow.errors import ConnectionError


def read_output(stdout, tag=None, num_line_stdout=-1):
    is_limit_stdout = False

    if num_line_stdout != -1:
        is_limit_stdout = True

    std_out_lines = list()

    if (is_limit_stdout == False
       or (is_limit_stdout == True and num_line_stdout != 0)
            ):
        if tag:
            sline = stdout.readline()
            while sline and sline != tag + '\n':
                sline = stdout.readline()

        sline = stdout.readline()
        while (sline and num_line_stdout != 0):

            std_out_lines.append(sline.strip())
            num_line_stdout = num_line_stdout - 1
            if(num_line_stdout == 0):
                break

            sline = stdout.readline()

    return std_out_lines


def SSH_exec_cmd(sshcommand,
                 userid,
                 ip_address_or_domain,
                 userpw='',
                 wait_output=True,
                 sshport=22,
                 isNeedErr=False,
                 num_line_stdout=-1,  # How many stdout or stderr lines to read
               num_line_stderr=-1,  # -1 means unlimited
               exit_status=None):   # None, 'return' or 'raise'

    if wait_output:
        tag = '----xxxx=====start to exec=====xxxxx----'
        sshcommand = "echo %s && %s" % (tag, sshcommand)

    # is_limit_stdout = False
    # is_limit_stderr = False

    # if num_line_stdout != -1:
    #    is_limit_stdout = True
    # if num_line_stderr != -1:
    #    is_limit_stderr = True

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

    except paramiko.AuthenticationException as e:
        print("The authentification failed. %s. "
              "Please check your user and password. "
              "You can test the connection in terminal with "
              "command: ssh -p %s %s@%s"
              % (e, sshport, userid, ip_address_or_domain))
        raise
    except Exception as e:
        print("Can not use ssh to log on the remote machine. "
              "Please Make sure your network can be connected %s. "
              "You can test the connection in terminal with "
              "command: ssh -p %s %s@%s"
              % (e, sshport, userid, ip_address_or_domain))
        raise

    if wait_output:
        std_out_lines = read_output(stdout, tag, num_line_stdout)
        if isNeedErr:
            std_err_lines = read_output(stderr, None, num_line_stderr)

    if exit_status is not  None:
        status = stdout.channel.recv_exit_status()
    client.close()

    if exit_status is not None:
        if status != 0:
            if exit_status == 'raise':
                raise OSError('SSH command failed with exit status %d: %s'
                              % (status, sshcommand))
            elif exit_status == 'return':
                if isNeedErr:
                    return (std_out_lines, std_err_lines, exit_status)
                else:
                    return (std_out_lines, exit_status)
            else:
                raise ValueError('unknown exit_status value: %s'
                                 % str(exit_status))

    if isNeedErr:
        return (std_out_lines, std_err_lines)
    else:
        return std_out_lines


def server_python_interpreter():
    ''' python command to run on server side '''
    python_interpreter = os.path.basename(sys.executable)
    if sys.version_info[0] >= 3 and python_interpreter == 'python':
        # force the use of python3 if we use it on client side
        python_interpreter = 'python%d' % sys.version_info[0]
    return python_interpreter


def check_if_soma_wf_cr_on_server(
    userid,
    ip_address_or_domain,
    userpw='',
    sshport=22,
    resource_id=None):
    """ Check if the check_requirement module exists
    """
    python_interpreter = server_python_interpreter()
    command = python_interpreter + " -m soma_workflow.check_requirement"
    if resource_id:
        command += ' ' + resource_id
    print(command)
    (std_out_lines, std_err_lines) = SSH_exec_cmd(
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


def check_if_somawf_on_server(
    userid,
    ip_address_or_domain,
    userpw='',
        sshport=22):
    """ Check if the soma_workflow module exists
    """
    python_interpreter = server_python_interpreter()
    command = python_interpreter + " -c 'import soma_workflow'"
    print(command)
    (std_out_lines, std_err_lines) = SSH_exec_cmd(
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

    command = "ps -ef | grep 'python. -m soma_workflow\.start_database_server'"\
        " | grep '%s' | grep -v grep | awk '{print $2}'" % (userid)

    std_out_lines = SSH_exec_cmd(
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


def search_available_port():
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
    The communication between the client and the computing resource is done
    with zro inside a ssh port forwarding tunnel.
    '''

    def __init__(self,
                 login,
                 password,
                 cluster_address,
                 submitting_machine,
                 resource_id,
                 log="",
                 rsa_key_pass=None,
                 config = None):
        '''
        @type  login: string
        @param login: user's login on the computing resource
        @type  password: string
        @param password: associted password
        @type  submitting_machine: string
        @param submitting_machine: address of a submitting machine of the
        computing resource.
        '''
        logging.info("************************************************")
        logging.info("***********Init remote connection***************")


        import soma_workflow.zro as zro
        import zmq

        # required in the remote connection mode
        # from paramiko.file import BufferedFile

        if not login:
            raise ConnectionError("Remote connection requires a login")

        if not isinstance(login, str):
            login = login.decode()

        remote_workflow_engine_name = "workflow_engine_" + login

        if not check_if_somawf_on_server(login, cluster_address, password):
            raise ConnectionError("Cannot find soma-workflow on %s. "
                                  "Please verify if your PYTHONPATH "
                                  "includes the soma-workflow."
                                  % (cluster_address))

        if not check_if_soma_wf_cr_on_server(login, cluster_address, password,
                                             resource_id=resource_id):
            raise ConnectionError("Cannot find "
                                  "soma_workflow.check_requirement on %s. "
                                  "Please update your soma-workflow on %s."
                                  % (cluster_address, cluster_address))

        # start_workflow_engine will run the database server
        #if not check_if_somawfdb_on_server(resource_id, login, cluster_address,
                                           #password):
            #command = "python -m soma_workflow.start_database_server %s & bg" % (
                #resource_id)
            #SSHExecCmd(
                #command,
                #login,
                #cluster_address,
                #userpw=password,
                #wait_output=False)

        # run the workflow engine process and get back the    #
        # WorkflowEngine and ConnectionChecker URIs       #
        python_interpreter = server_python_interpreter()
        command = python_interpreter + " -m soma_workflow.start_workflow_engine"\
                  " %s %s %s" % (resource_id, remote_workflow_engine_name, log)

        print("start engine command: "
              "ssh %s@%s %s" % (login, cluster_address, command))

        (std_out_lines) = SSH_exec_cmd(
            command,
            login,
            cluster_address,
            userpw=password,
            wait_output=True,
            sshport=22,
            num_line_stdout=5)
        workflow_engine_uri = None
        connection_checker_uri = None
        configuration_uri = None
        scheduler_config_uri = None

        # print(std_out_lines)

        for std_out_line in std_out_lines:
            if std_out_line.split()[0] == remote_workflow_engine_name:
                workflow_engine_uri = std_out_line.split()[1]
            elif std_out_line.split()[0] == "connection_checker":
                connection_checker_uri = std_out_line.split()[1]
            elif std_out_line.split()[0] == "configuration":
                configuration_uri = std_out_line.split()[1]
            elif std_out_line.split()[0] == "scheduler_config":
                scheduler_config_uri = std_out_line.split()[1]
                if scheduler_config_uri == "None":
                    scheduler_config_uri = None
            elif std_out_line.split()[0] == "zmq":
                version = std_out_line.split()[1]
                if len(std_out_line.split()) > 9:
                    python_path = std_out_line.split()[2:9]
                else:
                    python_path = std_out_line.split()[2:]
                if zmq.__version__ != version:
                    print("WARNING!!!: you are not using the same version of "
                          "zmq on the server and you might have some issues: \n"
                          "local version is: " + zmq.__version__ + "\nserver version is: "
                          + version)
                    print("Note, the beginning of your PYTHONPATH on host is: " + repr(python_path))
                else:
                    # print("DEBUG same version of ZMQ on both sides")
                    pass

        logging.debug("workflow_engine_uri: " +  str(workflow_engine_uri))
        logging.debug("connection_checker_uri: "
                      +  str(connection_checker_uri))
        logging.debug("configuration_uri: " + str(configuration_uri))

        if (not configuration_uri or
            not connection_checker_uri or
            not workflow_engine_uri):
            stdout = '\n'.join(std_out_lines)
            short_msg = "A problem occured while starting the engine " \
                "process on the remote machine " \
                + str(cluster_address) + "\n"
            if 'already been running with a different version of Python' \
                    in stdout:
                # in case of thie specific error, display it in the short
                # message to be immediately visible in the GUI
                short_msg += '\n' + stdout
            raise ConnectionError(
                short_msg +
                "**More details:**\n"
                "**Start engine process command line:** \n"
                "\n" + command + "\n\n"
                "**Engine process standard output:** \n"
                "\n" + stdout)


        whole_uri = str(workflow_engine_uri)
        (object_data_type, object_id, port) = whole_uri.split(":")
        remote_object_server_port = int(port)


        #checking
        logging.debug("zro object server port: %d" % remote_object_server_port)
        logging.debug('its type: %s' % repr(type(remote_object_server_port)))

        ### find an available port            ###
        tunnel_entrance_port = search_available_port()
        logging.debug("client tunel port on localhost: %s" % repr(tunnel_entrance_port))

        import paramiko

        ### tunnel creation                     ###
        try:
            self.__transport = paramiko.Transport((cluster_address, 22))
            self.__transport.setDaemon(True)
            self.__transport.set_keepalive(150)
            self.__transport.connect(username=login, password=password)

            if not password:
                rsa_file_path = os.path.join(
                    os.environ['HOME'], '.ssh', 'id_rsa')
                logging.info("reading RSA key in " + repr(rsa_file_path))
                if rsa_key_pass:
                    key = paramiko.RSAKey.from_private_key_file(
                        rsa_file_path,
                        password=rsa_key_pass)
                else:
                    key = paramiko.RSAKey.from_private_key_file(rsa_file_path)
                self.__transport.auth_publickey(login, key)

                # TBI DSA Key => see paramamiko/demos/demo.py for an example
            print("tunnel creation " + str(login) + "@" + cluster_address +
                  "   port: " + repr(tunnel_entrance_port) + " host: " +
                  str(submitting_machine) + " host port: "
                  + str(remote_object_server_port))

            tunnel = Tunnel(tunnel_entrance_port,
                            #submitting_machine,
                            remote_object_server_port,
                            self.__transport)

            tunnel.start()
            self.__tunnel = tunnel

        except paramiko.AuthenticationException as e:
            raise ConnectionError("The authentification failed while "
                                  "creating the ssh tunnel. %s" % (e))
        except Exception as e:
            raise ConnectionError("The ssh communication tunnel could not be created."
                                  "%s: %s" % (type(e), e))

        logging.debug("The workflow engine URI is: " + workflow_engine_uri)

        # rewritting the uri to use the tunnel entrance port
        (object_data_type, object_id, object_server_port) = workflow_engine_uri.split(":")
        workflow_engine_uri = object_data_type + ":" + object_id + ":" + str(tunnel_entrance_port)
        (object_data_type, object_id, object_server_port) = connection_checker_uri.split(":")
        connection_checker_uri = object_data_type + ":" + object_id + ":" + str(tunnel_entrance_port)
        (object_data_type, object_id, object_server_port) = configuration_uri.split(":")
        configuration_uri = object_data_type + ":" + object_id + ":" + str(tunnel_entrance_port)

        # create the proxy objects                     #

        self.workflow_engine = zro.Proxy(workflow_engine_uri)
        connection_checker = zro.Proxy(connection_checker_uri)
        self.configuration = zro.Proxy(configuration_uri)

        if scheduler_config_uri is not None:
            # setting the proxies to use the tunnel  #
            logging.info("Scheduler config available")
            (object_data_type, object_id, object_server_port) = scheduler_config_uri.split(":")
            scheduler_config_uri = object_data_type + ":" + object_id + ":" + str(tunnel_entrance_port)
            self.scheduler_config = zro.Proxy(scheduler_config_uri)
        else:
            logging.info('No scheduler config')
            self.scheduler_config = None

        # waiting for the tunnel to be set
        tunnelSet = False
        maxattemps = 3
        attempts = 0
        while not tunnelSet and attempts < maxattemps:
            try:
                attempts = attempts + 1
                print("Communication through the ssh tunnel. Attempt no " +
                       repr(attempts) + "/" + repr(maxattemps))
                # print("BEFORE calling a remote object")
                self.workflow_engine.jobs()
                # print("After calling the remote object")
                connection_checker.isConnected()
            except Exception as e:
                print("-> Communication through ssh tunnel Failed. %s: %s"
                      % (type(e), e))
                time.sleep(1)
            else:
                print("-> Communication through ssh tunnel OK")
                tunnelSet = True

        if attempts >= maxattemps:
            raise ConnectionError("The ssh tunnel could not be started within"
                                  + repr(maxattemps) + " attempts.")

        # create the connection holder object for #
        # a clean disconnection in any case      #
        logging.info("Launching the connection holder thread")
        self.__connection_holder = ConnectionHolder(connection_checker)
        self.__connection_holder.start()
        logging.info("End of the initialisation of RemoteConnection.")

    def __del__(self):
        print('del RemoteConnection')
        self.stop()

    def isValid(self):
        return self.__connection_holder.isAlive()

    def stop(self):
        '''
        For test purpose only !
        '''
        print('RemoteConnection.stop')
        if self.__tunnel is not None:
            self.__tunnel.shutdown()
        self.__tunnel = None
        self.__connection_holder.stop()
        self.__transport.close()

    def get_workflow_engine(self):
        return self.workflow_engine

    def get_configuration(self):
        return self.configuration

    def get_scheduler_config(self):
        return self.scheduler_config

    @staticmethod
    def kill_remote_servers(resource_id, login=None, passwd=None,
                            ssh_port=22, clear_db=False):
        '''Kill servers and possibly delete the database on the given remote
        computing resource.
        '''
        from soma_workflow import configuration
        import paramiko

        config = configuration.Configuration.load_from_file(resource_id)
        if login is None:
            login = config.get_login()

        submitting_machines = config.get_submitting_machines()
        sub_machine = submitting_machines[random.randint(
            0, len(submitting_machines) - 1)]

        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.load_system_host_keys()
            ssh.connect(sub_machine, port=ssh_port, username=login,
                        password=passwd)
        except paramiko.AuthenticationException as e:
            print("The authentification failed. %s. "
                  "Please check your user and password. "
                  "You can test the connection in terminal with "
                  "command: ssh -p %s %s@%s"
                  % (e, sshport, userid, ip_address_or_domain))
            raise

        stdin, stdout, stderr = ssh.exec_command('ps ux')

        db_re = re.compile('^[^ ]+ +([0-9]+) .*python[0-9]? -m soma_workflow.start_database_server ([^ ]+)$')
        en_re = re.compile('^[^ ]+ +([0-9]+) .*python[0-9]? -m soma_workflow.start_workflow_engine ([^ ]+) .*$')
        for psline in stdout.readlines():
            m = db_re.match(psline)
            if m:
                resource = m.group(2).strip()
                if resource == resource_id:
                    print('found database process id:', m.group(1))
                    cmd = 'kill %s' % m.group(1)
                    ssh.exec_command(cmd)
            else:
                m = en_re.match(psline)
                if m:
                    if m.group(2) == resource_id:
                        print('found engine process id:', m.group(1))
                        cmd = 'kill %s' % m.group(1)
                        ssh.exec_command(cmd)

        if clear_db:
            print('clearing database')
            python_interpreter = server_python_interpreter()
            cmd = '''. $HOME/.bashrc && %s -c 'from __future__ import print_function; from soma_workflow import configuration; config = configuration.Configuration.load_from_file("%s"); print(config.get_database_file())\'''' \
                % (python_interpreter, resource_id)
            stdin, stdout, stderr = ssh.exec_command(cmd)
            db_file = stdout.read().strip()
            if not isinstance(db_file, str):
                db_file = db_file.decode()
            if db_file:
                print('remove:', db_file)
                cmd = 'rm %s' % db_file
                ssh.exec_command(cmd)
            else:
                print('cannot retreive database file name from server config. '
                      'Installation problem on server side?')

class LocalConnection(object):
    '''
    Local version of the connection.
    The workflow engine process is created using subprocess.
    '''

    def __init__(self,
                 resource_id,
                 log=""):

        # required in the local connection mode

        import soma_workflow.zro as zro
        import sys
        try:
            import subprocess32 as subprocess
            import subprocess as _subprocess
            if hasattr(_subprocess, '_args_from_interpreter_flags'):
                # get this private function which is used somewhere in
                # multiprocessing
                subprocess._args_from_interpreter_flags \
                    = _subprocess._args_from_interpreter_flags
            del _subprocess
            import sys
            sys.modules['subprocess'] = sys.modules['subprocess32']
        except ImportError:
            import subprocess

        login = getpass.getuser()
        remote_workflow_engine_name = "workflow_engine_" + login

        # run the workflow engine process and get back the
        # workflow_engine and ConnectionChecker URIs
        # command = "python -m cProfile -o /home/soizic/profile/profile /home/soizic/svn/brainvisa/source/soma/soma-workflow/trunk/python/soma_workflow/start_workflow_engine.py %s %s %s" %(
                                         # resource_id,
                                         # remote_workflow_engine_name,
                                         # log)
        (local_dir, python_interpreter) = os.path.split(sys.executable)
        command = python_interpreter + " -m soma_workflow.start_workflow_engine %s %s %s" % (
            resource_id,
            remote_workflow_engine_name,
            log)

        engine_process = subprocess.Popen(command,
                                          shell=True,
                                          stdout=subprocess.PIPE,
                                          stderr=subprocess.PIPE)

        line = engine_process.stdout.readline().decode()
        stdout_content = line
        while line and line.split()[0] != remote_workflow_engine_name:
            line = engine_process.stdout.readline().decode()
            stdout_content = stdout_content + "\n" + line

        if not line:  # A problem occured while starting the engine.
            line = engine_process.stderr.readline().decode()
            stderr_content = line
            while line:
                line = engine_process.stderr.readline().decode()
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
        line = engine_process.stdout.readline().decode()
        stdout_content = stdout_content + "\n" + line
        while line and line.split()[0] != "connection_checker":
            line = engine_process.stdout.readline().decode()
            stdout_content = stdout_content + "\n" + line

        if not line:  # A problem occured while starting the engine.
            line = engine_process.stderr.readline().decode()
            stderr_content = line
            while line:
                line = engine_process.stderr.readline().decode()
                #TODO TBC
                print("this should contain the uri of the connection checker in 2nd position" +line)
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
        line = engine_process.stdout.readline().decode()
        stdout_content = stdout_content + "\n" + line
        while line and line.split()[0] != "configuration":
            line = engine_process.stdout.readline().decode()
            stdout_content = stdout_content + "\n" + line

        if not line:  # A problem occured while starting the engine.
            line = engine_process.stderr.readline().decode()
            stderr_content = line
            while line:
                line = engine_process.stderr.readline().decode()
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

        logging.debug("workflow_engine_uri: " + workflow_engine_uri)
        logging.debug("connection_checker_uri: " + connection_checker_uri)
        logging.debug("configuration_uri: " + configuration_uri)

        # create the proxies                     #

        self.workflow_engine = zro.Proxy(workflow_engine_uri)
        connection_checker = zro.Proxy(connection_checker_uri)
        self.configuration = zro.Proxy(configuration_uri)

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
    ''' ConnectionChecker runs on server side. It runs a thread which listens
        to "life signals" emitted from the client (using a ConnectionHolder
        object).

        ConnectionChecker should be contacted through the network from the
        client, so should have a network proxy.
    '''

    def __init__(self, interval=2, controlInterval=3):
        self.connected = False
        self.lock = threading.RLock()
        self.interval = timedelta(seconds=interval)
        self.controlInterval = controlInterval
        self.lastSignal = datetime.now()

        def controlLoop(self, control_interval):
            logger = logging.getLogger('engine.ConnectionChecker')
            logger.debug('ConnectionChecker: start controlLoop')
            while True:
                with self.lock:
                    last_signal = self.lastSignal
                    # print(ls)
                delta = datetime.now() - last_signal
                if delta > self.interval * 12:
                    logger.debug("Delta is too large, the client is not connected anymore: " + str(delta))
                    self.disconnectionCallback()
                    with self.lock:
                        self.connected = False
                    logger.debug('ConnectionChecker: client disconnected.')
                    break
                else:
                    with self.lock:
                        self.connected = True
                with self.lock:
                    logger.debug("ConnectionChecker: Connected? :"
                                 + str(self.connected))
                time.sleep(control_interval)
            logger.debug('ConnectionChecker: exit controlLoop')

        self.controlThread = threading.Thread(name="connectionControlThread",
                                              target=controlLoop,
                                              args=(self, controlInterval))
        self.controlThread.setDaemon(True)
        self.controlThread.start()

    def get_interval(self):
        return self.interval.seconds

    def signalConnectionExist(self):
        with self.lock:
            # print("ConnectionChecker <= a signal was received")
            logger = logging.getLogger('engine.ConnectionChecker')
            logger.debug("ConnectionChecker <= a signal was received")
            self.lastSignal = datetime.now()

    def isConnected(self):
        with self.lock:
            return self.connected

    def disconnectionCallback(self):
        pass


class ConnectionHolder(threading.Thread):
    ''' ConnectionHolder runs on client side, it runs a thread which emits a
        signal to the server every time interval, using a ConnectionChecker
        proxy.
    '''

    def __init__(self, connectionChecker):
        threading.Thread.__init__(self)
        self.setDaemon(True)
        self.name = "connectionHolderThread"
        self.connectionChecker = connectionChecker
        self.interval = self.connectionChecker.get_interval()

    def __del__(self):
        print('del ConnectionHolder')
        self.stop()

    def run(self):
        self.stopped = False
        while not self.stopped:
            # print("ConnectionHolder => signal")
            try:
                self.connectionChecker.signalConnectionExist()
                #print('life signal emitted')
            except ConnectionClosedError as e: # TBC Apparently the exception is not defined anymore
                print("Connection closed")
                break
            time.sleep(self.interval)
        print('ConnectionHolder stopped.')

    def stop(self):
        self.stopped = True


class Tunnel(threading.Thread):

    class ForwardServer (socketserver.ThreadingTCPServer):
        daemon_threads = True
        allow_reuse_address = True

    class Handler (socketserver.BaseRequestHandler):

        def setup(self):
            logging.info('Setup : %d' %(self.channel_end_port))
            if not self.ssh_transport.is_active():
                raise ConnectionError('ssh transport is closed.')
            logging.debug("Tunnel::Handler::Beginning of setup")
            try:
                # There has been quite lot of tweaks around this function
                # as it is very counter intuitive. On the destination
                # adress the host should be localhost, this is quite
                # reminiscent of the -L option of ssh.

                # print("Peername is: ", self.request.getpeername())
                self.__chan = self.ssh_transport.open_channel(
                    'direct-tcpip',
                    ('localhost', self.channel_end_port), #destination address
                    self.request.getpeername())#source address (from the Proxy object)
            except Exception as e:
                logging.exception("Hey here is an exception!: %s" % repr(e))
                self.shutdown_server()
                raise ConnectionError('Incoming request to %d failed: %s'
                    % (self.channel_end_port, repr(e)))

            if self.__chan is None:
                self.shutdown_server()
                raise ConnectionError(
                    'Incoming request to %s:%d was rejected by the SSH server.'
                    % ('localhost', self.channel_end_port))

            logging.info('Connected!  Tunnel open %r -> %r -> %r'
                  % (self.request.getpeername(),
                     self.__chan.getpeername(),
                     ('localhost', self.channel_end_port)))

        def handle(self):
            # print("Beginning of handle")
            logging.info('Handle : %s %d' %('localhost',
                                            self.channel_end_port))
            if not self.ssh_transport.is_active():
                return
            try:
                while True:
                    try:
                        r, w, x = select.select([self.request, self.__chan], [],
                                                [])
                    except Exception as e:
                        if e.args[0] == errno.EINTR:
                            # Qt modal dialogs event loop (at least in
                            # QFileDialog)
                            # can cause an interrupted system call here.
                            # It seems not to completely break the connection,
                            # we can go on.
                            continue
                        else:
                            raise
                    if self.request in r:
                        # print("Transfering from the local server handling the channel "
                        #      "to the original object")
                        data = self.request.recv(12000)

                        if len(data) == 0:
                            break
                        if len(data) == 12000:
                            logging.debug("Network data too small")
                            logging.info("Tunnel.Handler.handle: multiple receive to transfert"
                                    " the data, could potentially be a problem")
                        self.__chan.send(data)
                    if self.__chan in r:
                        # print('Transfering from the channel to the proxy')
                        data = self.__chan.recv(12000)
                        if len(data) == 0:
                            break
                        if len(data) == 12000:
                            logging.debug("Network data too small 2")
                            logging.info("Tunnel.Handler.handle: multiple receive to transfert"
                                    "the data, could potentially be a problem")
                        self.request.send(data)
            except:
                self.shutdown_server()

        def finish(self):
            print('Channel closed from %r' % (self.request.getpeername(),))
            self.__chan.close()
            self.request.close()
            del self.__chan
            self.shutdown_server()

        def shutdown_server(self):
            #Tunnel.server_instance.shutdown()
            # try to determine which socket server has called us,
            # and shut it down
            import gc
            import inspect
            frames = [x.f_back for x in gc.get_referrers(self)
                      if inspect.isframe(x)]
            servers = [x.f_locals['self'] for x in frames
                       if 'self' in x.f_locals
                          and isinstance(x.f_locals['self'],
                                         Tunnel.ForwardServer)]
            for server in servers:
                server.shutdown()

    def __init__(self, localport, hostport, transport):
        threading.Thread.__init__(self)
        self.__port = localport
        self.__hostport = hostport
        self.__transport = transport
        self.__server = None
        self.setDaemon(True)

    def serve_forever(self):
        try:
            super(Tunnel, self).serve_forever()
        except:
            print('EXCEPT')
            self.shutdown()

    def __del__(self):
        logging.info('del Tunnel')
        self.shutdown()

    def run(self):
        hostport = self.__hostport
        transport = self.__transport
        local_port = self.__port

        logging.debug("local server port: " + str(local_port) )

        class SubHandler(Tunnel.Handler):
            channel_end_port = hostport
            ssh_transport = transport

        try:
            self.__server = Tunnel.ForwardServer(('', local_port), SubHandler)
            Tunnel.server_instance = self.__server
            self.__server.serve_forever()
        except KeyboardInterrupt:
            logging.error('tunnel from local_port %d to port %d stopped !' % (local_port, hostport))
        except Exception as e:
            logging.error('Tunnel Error. %s: %s' % (type(e), e))
        logging.warning('Tunnel stopped.')

    def shutdown(self):
        if self.__server is not None:
            self.__server.shutdown()
            self.__server = None
