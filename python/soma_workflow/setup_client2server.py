#! /usr/bin/env python

"""
@author: Jinpeng LI
@contact: mr.li.jinpeng@gmail.coms
@organization: I2BM, Neurospin, Gif-sur-Yvette, France
@organization: CATI, France
@organization: U{IFR 49<http://www.ifr49.org>}

@license: U{CeCILL version 2<http://www.cecill.info/licences/
Licence_CeCILL_V2-en.html>}
"""

from __future__ import with_statement

# System import
import os
import re
import logging
try:
    import subprocess32 as subprocess
    import subprocess as _subprocess
    if hasattr(_subprocess, '_args_from_interpreter_flags'):
        # get this private function which is used somewhere in
        # multiprocessing
        subprocess._args_from_interpreter_flags \
            = _subprocess._args_from_interpreter_flags
    del _subprocess
except ImportError:
    import subprocess
from ConfigParser import SafeConfigParser

# Soma Workflow import
from soma_workflow.connection import SSHExecCmd, check_if_somawfdb_on_server
import soma_workflow.configuration as configuration
from soma_workflow.configuration import WriteOutConfiguration
from soma_workflow.client import Job, Workflow, WorkflowController


#
#                    Server Information Tools
#

def GetHostNameOnPBSTORQUE(userid,
                           ip_address_or_domain,
                           userpw=None):
    """ Return the host name

    Parameters
    ----------
    userid: str
        user name on the server side
    ip_address_or_domain: str
        the ip address or the domain of the server
    userpw: str
        user password to login the server using ssh.
        If you want to use "id_rsa.pub", just leave userpw = ""

    Returns
    -------
    hostname: str
        the hostname

    .. note::
        Raises paramiko.AuthenticationException when the authetification
        fails.
    """
    return SSHExecCmd("hostname", userid, ip_address_or_domain, userpw)[0]


def GetQueueNamesOnPBSTORQUE(userid,
                             ip_address_or_domain,
                             userpw=None):
    """ Return a list of queue names on torque pbs

    Parameters
    ----------
    userid: str
        user name on the server side
    ip_address_or_domain: str
        the ip address or the domain of the server
    userpw: str
        user password to login the server using ssh.
        If you want to use "id_rsa.pub", just leave userpw = ""

    Returns
    -------
    info_queue: list of str
        the list of queue names on torque pbs

    .. note::
        Raises paramiko.AuthenticationException when the authetification
        fails.
    """
    std_out_lines = SSHExecCmd("qstat -Q", userid, ip_address_or_domain,
                               userpw)

    info_queue = []
    # Skip the first line since it is the header
    for line in std_out_lines[2:]:
        queue_item = line.split()[0]
        # Check if the queue item start with an upper or lower character
        if re.match("^[a-zA-Z]", queue_item):
            info_queue.append(queue_item)

    return info_queue


#
#                      Client Configuration Tools
#

def SetupConfigurationFileOnClient(configuration_item_name,
                                   userid,
                                   ip_address_or_domain,
                                   userpw=None,
                                   install_swf_path_server=None,
                                   sshport=22):
    """ Setup the configuration file on the client part

    The procedure:
    * search the configuration file on the client.
    * if it exists, keep the original configuration,
    * if not, create the new configuration at $HOME/.soma-workflow.cfg

    Parameters
    ----------
    configuration_item_name: str
        the name of the configuration item (ex. "Gabriel")
    userid: str
        user name on the server side
    ip_address_or_domain: str
        the ip address or the domain of the server
    userpw: str (optional)
        user password to login the server using ssh.
        If you want to use "id_rsa.pub", just leave userpw to None
        To copy the public key on the server use ssh-copy-id -i name@server.
    install_swf_path_server: str (optional)
        soma workflow source path on server
    sshport: int (optional)
        the ssh port

    .. note::
        Raises IOError, ValueError when the procedure fails.
    """
    # Search the configuration file on the client
    config_file_path = configuration.Configuration.search_config_path()

    # If configuration file not found
    if config_file_path == None:

        # Generate configuration file path
        home_dir = configuration.Configuration.get_home_dir()
        config_file_path = os.path.join(home_dir, ".soma-workflow.cfg")
        logging.info("No configuration file on the client, "
                     "we create a new one at {0}".format(config_file_path))

        # Generate client config
        config_parser = ConfiguratePaser(configuration_item_name, userid,
                                         ip_address_or_domain, userpw, install_swf_path_server, sshport)
        WriteOutConfiguration(config_parser, config_file_path)

    # If the configuration already exists
    else:
        logging.info("Configuration file is found at: {0}".format(
            config_file_path))

        # First read the configuration file
        config_parser = read_configuration_file(config_file_path)

        # Then check if configuration item is already created
        sections = config_parser.sections()
        if not configuration_item_name in sections:
            # Add client config
            config_parser = ConfiguratePaser(configuration_item_name, userid,
                                             ip_address_or_domain, userpw, install_swf_path_server,
                                             sshport, config_parser)
            WriteOutConfiguration(config_parser, config_file_path)


def read_configuration_file(config_file_path):
    """ Read a configuration file

    Parameters
    ----------
    config_file_path: str
        the path to the configuration file we want to load

    Returns
    -------
    config_parser: SafeConfigParser
        the configuration object

    .. note::
        Raises IOError if the procedure fails.
    """
    config_parser = SafeConfigParser()
    try:
        config_parser.read(config_file_path)
    except:
        strmsg = "Cannot open {0} \n".format(config_file_path)
        raise IOError(strmsg)
    return config_parser


def ConfiguratePaser(configuration_item_name,
                     userid,
                     ip_address_or_domain,
                     userpw=None,
                     installpath=None,
                     sshport=None,
                     config_parser=None):
    """ Create or update the configuration file based on the input
    secifications.

    Parameters
    ----------
    configuration_item_name: str
        the name of the configuration item
    userid: str
        user name on the server side
    ip_address_or_domain: str
        the ip address or the domain of the server
    userpw: str
        user password to login the server using ssh.
        If you want to use "id_rsa.pub", just leave userpw to None
        To copy the public key on the server use ssh-copy-id -i name@server.
    installpath: str
        soma workflow source path on server
    sshport: int (optional)
        the ssh port
    config_parser: SafeConfigParser (optional)
        default None, an empty config parser is created
        otherwise, the config parser is updatd with the new item

    Returns
    -------
    config_parser: SafeConfigParser
        the configuration item
    """

    # Create config generator
    if not config_parser:
        config_parser = SafeConfigParser()

    # Get server info
    hostname = GetHostNameOnPBSTORQUE(userid, ip_address_or_domain,
                                      userpw)
    info_queue = GetQueueNamesOnPBSTORQUE(userid, ip_address_or_domain,
                                          userpw)

    # Add section
    config_parser.add_section(configuration_item_name)

    # Fill section
    config_parser.set(configuration_item_name,
                      configuration.CFG_CLUSTER_ADDRESS,
                      hostname)
    config_parser.set(configuration_item_name,
                      configuration.CFG_SUBMITTING_MACHINES,
                      hostname)
    config_parser.set(configuration_item_name,
                      configuration.OCFG_QUEUES,
                      " ".join(info_queue))
    config_parser.set(configuration_item_name,
                      configuration.OCFG_LOGIN,
                      userid)
    config_parser.set(configuration_item_name,
                      configuration.OCFG_SSHPort,
                      str(sshport or 22))
    if installpath:
        config_parser.set(configuration_item_name,
                          configuration.OCFG_INSTALLPATH,
                          installpath)

    return config_parser


def RemoveResNameOnConfigureFile(section_to_remove,
                                 config_file_path=None):
    """ Remove a section from a configuration file

    Parameters
    ----------
    section_to_remove: str
        the name of the section we want to remove from the configuration
        file
    config_file_path: str
        the path to the configuration file we want to update
        If None, try to find the path to the soma workflow configuration file

    Returns
    -------
    status: int
        0 no configuration file found
        1 file updated

    .. note::
        Raises IOError if the procedure fails.
    """
    # First find the configuration file if not specified
    if config_file_path is None:
        config_file_path = configuration.Configuration.search_config_path()

    # If no configuration file found exit with 0 status
    if config_file_path is None:
        return 0

    # Read the configuration file
    config_parser = read_configuration_file(config_file_path)

    # Remove the section
    config_parser.remove_section(section_to_remove)

    # Write the resulting configuration
    WriteOutConfiguration(config_parser, config_file_path)

    return 1


#
#                      Server Installation Tools
#

def InstallSomaWF2Server(userid,
                         ip_address_or_domain,
                         configuration_item_name,
                         userpw=None,
                         install_swf_path_server=None,
                         sshport=22,
                         config_options={}):
    """ Procedure to install the client somaworklow install on the server
    side.

    Parameters
    ----------
    userid: str
        user name on the server side
    ip_address_or_domain: str
        the ip address or the domain of the server
    configuration_item_name: str
        the name of the configuration item (ex. "Gabriel")
    userpw: str (optional)
        user password to login the server using ssh.
        If you want to use "id_rsa.pub", just leave userpw to None
        To copy the public key on the server use ssh-copy-id -i name@server.
    install_swf_path_server: str (optional)
        soma workflow source path on server
    sshport: int (optional)
        the ssh port
    """
    # Frist make a copy of the local install to the server
    install_swf_path_server = CopySomaWF2Server(
        userid, ip_address_or_domain, userpw, install_swf_path_server,
        sshport)

    # Set environ
    script2install = os.path.join(install_swf_path_server, "soma_workflow",
                                  "setup_server.py")
    command = "python '{0}' -r {1} ".format(script2install,
                                            configuration_item_name)
    if config_options:
        command += ' ' + ' '.join(['%s=\'%s\'' % (n, v)
                                   for n, v in config_options.items()])
    logging.info("ssh command = {0}".format(command))
    print('install command:', command)

    (std_out_lines, std_err_lines) = SSHExecCmd(command, userid,
                                                ip_address_or_domain, userpw,
                                                wait_output=False,
                                                isNeedErr=True,
                                                sshport=sshport)
    if len(std_err_lines) > 0:
        logging.error("Unable to configure the server: {0}".format(
                      std_err_lines))
        raise OSError("Unable to configure the server:  {0}".format(
                      std_err_lines))

    # Create a update the configuration file on the client side
    SetupConfigurationFileOnClient(configuration_item_name, userid,
                                   ip_address_or_domain,
                                   userpw, install_swf_path_server, sshport)

    # submit a workflow for test
    SimpleJobExample(configuration_item_name, userid, userpw)


def CopySomaWF2Server(userid,
                      ip_address_or_domain,
                      userpw=None,
                      install_swf_path_server=None,
                      sshport=22):
    """ Copy the soma workflow source from the client to the server.
    Force to have the same version on both side.

    Parameters
    ----------
    userid: str
        user name on the server side
    ip_address_or_domain: str
        the ip address or the domain of the server
    userpw: str
        user password to login the server using ssh.
        If you want to use "id_rsa.pub", just leave userpw to None
        To copy the public key on the server use ssh-copy-id -i name@server.
    install_swf_path_server: str
        soma workflow source path on server
    sshport: int (optional)
        the ssh port

    Returns
    -------
    install_swf_path_server: str
        the location where the soma workflow project has been copied
    """
    # Generate install path
    install_swf_path_server = (install_swf_path_server or
                               os.path.expanduser("~"))
    install_swf_path_server = os.path.join(install_swf_path_server,
                                           "soma_workflow_auto_remote_install")
    # Create install directory if necessary
    std_out_lines = SSHExecCmd(
        "mkdir -p '{0}'".format(install_swf_path_server),
        userid, ip_address_or_domain, userpw, sshport=sshport,
        exit_status='raise')
    logging.info("Install soma workflow on server attempt to create server "
                 "project directory. Command return {0}".format(std_out_lines))

    # Get client project paths
    path2somawf = os.path.dirname(os.path.realpath(__file__))
    path2drmaa = os.path.join(path2somawf, os.pardir, "somadrmaa")

    # Sync soma workflow to server
    sshcommand = ("rsync", "-e", "ssh", "-a", "--copy-unsafe-links",
                  "--delete-after", path2somawf,
                  "{0}@{1}:'{2}'".format(userid, ip_address_or_domain,
                                         install_swf_path_server))
    logging.info("Attempt to copy soma workflow: sshcommand = "
                 " {0}".format(sshcommand))
    subprocess.check_call(sshcommand)

    # Sync drmaa patching to server
    sshcommand = ("rsync", "-e", "ssh", "-a", "--copy-unsafe-links",
                  "--delete-after", path2drmaa,
                  "{0}@{1}:'{2}'".format(userid, ip_address_or_domain,
                                         install_swf_path_server))
    logging.info("Attempt to copy drmaa patching: sshcommand = "
                 " {0}".format(sshcommand))
    subprocess.check_call(sshcommand)

    return install_swf_path_server


def SimpleJobExample(configuration_item_name, userid, userpw=None):
    """ Dummy workflow to test the install

    Parameters
    ----------
    configuration_item_name: str
        the name of the configuration item (ex. "Gabriel")
    userid: str
        user name on the server side
    userpw: str (optional)
        user password to login the server using ssh.
        If you want to use "id_rsa.pub", just leave userpw to None
        To copy the public key on the server use ssh-copy-id -i name@server.
    """
    job_1 = Job(command=["sleep", "5"], name="job 1")
    job_2 = Job(command=["sleep", "5"], name="job 2")
    job_3 = Job(command=["sleep", "5"], name="job 3")
    job_4 = Job(command=["sleep", "5"], name="job 4")

    jobs = [job_1, job_2, job_3, job_4]
    dependencies = [(job_1, job_2),
                    (job_1, job_3),
                    (job_2, job_4),
                    (job_3, job_4)]

    workflow = Workflow(jobs=jobs,
                        dependencies=dependencies)

    controller = WorkflowController(configuration_item_name, userid, userpw)

    controller.submit_workflow(workflow=workflow,
                               name="TestConnectionExample")


def SetupSomaWF2Server(userid,
                       ip_address_or_domain,
                       configuration_item_name,
                       userpw=None,
                       install_swf_path_server=None,
                       sshport=22):
    """ Procedure to setup somaworklow on the server side.

    Parameters
    ----------
    userid: str
        user name on the server side
    ip_address_or_domain: str
        the ip address or the domain of the server
    configuration_item_name: str
        the name of the configuration item (ex. "Gabriel")
    userpw: str (optional)
        user password to login the server using ssh.
        If you want to use "id_rsa.pub", just leave userpw to None
        To copy the public key on the server use ssh-copy-id -i name@server.
    install_swf_path_server: str (optional)
        soma workflow source path on server
    sshport: int (optional)
        the ssh port
    """
    # Check if swf intalled on the server side
    if check_if_somawfdb_on_server(configuration_item_name, userid,
                                   ip_address_or_domain, userpw, sshport):
        msg = ("Cannot find the soma-workflow on the server. "
               "Please first install soma-workflow")
        raise ImportError(msg)

    # Create a update the configuration file on the client side
    SetupConfigurationFileOnClient(configuration_item_name, userid,
                                   ip_address_or_domain,
                                   userpw, install_swf_path_server, sshport)

    # submit a workflow for test
    SimpleJobExample(configuration_item_name, userid, userpw)


def RemoveSomaWF2Server(userid,
                        ip_address_or_domain,
                        configuration_item_name,
                        userpw=None,
                        install_swf_path_server=None,
                        sshport=22):
    """ Procedure to remove somaworklow on the server side.

    Parameters
    ----------
    userid: str
        user name on the server side
    ip_address_or_domain: str
        the ip address or the domain of the server
    configuration_item_name: str
        the name of the configuration item (ex. "Gabriel")
    userpw: str (optional)
        user password to login the server using ssh.
        If you want to use "id_rsa.pub", just leave userpw to None
        To copy the public key on the server use ssh-copy-id -i name@server.
    install_swf_path_server: str (optional)
        soma workflow source path on server
    sshport: int (optional)
        the ssh port
    """
    # Execute the clean script on the server side
    clean_script_server = os.path.join(install_swf_path_server,
                                       "soma_workflow", "clean_server.py")
    command = "python '{0}' -r {1}".format(clean_script_server,
                                           configuration_item_name)
    SSHExecCmd(command, userid, ip_address_or_domain,
               userpw, wait_output=False, sshport=sshport)

    # Remove the source files on the server
    command = "rm -rf '{0}'".format(install_swf_path_server)
    SSHExecCmd(command, userid, ip_address_or_domain,
               userpw, wait_output=False, sshport=sshport)

    # remove the configuration on the client
    RemoveResNameOnConfigureFile(configuration_item_name)


if __name__ == "__main__":
    InstallSomaWF2Server("ag239446", "gabriel", "Gabriel")
