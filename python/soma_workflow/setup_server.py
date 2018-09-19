#! /usr/bin/env python

"""
@author: Jinpeng LI
@contact: mr.li.jinpeng@gmail.com

@organization: I2BM, Neurospin, Gif-sur-Yvette, France
@organization: CATI, France
@organization: U{IFR 49<http://www.ifr49.org>}

@license: U{CeCILL version 2<http://www.cecill.info/licences/
Licence_CeCILL_V2-en.html>}
"""

# System import
from __future__ import print_function

import os
import logging
import sys
import argparse
import re
import getpass
import socket
import subprocess
import shutil
from ConfigParser import SafeConfigParser


#
#               Global variables
#

path2somawf_setup_server = os.path.realpath(__file__)
current_dir = os.path.dirname(os.path.realpath(__file__))
path2somawf = os.path.normpath(os.path.abspath(os.path.join(current_dir,
                                                            os.pardir)))
path2resources = "/i2bm/brainvisa/CentOS-5.11-x86_64/python"

req_version = (2, 7)


#
#               Environement variables to export
#

def SetPathToEnvVar(env_var_name, path):
    """ Set an environment variable

    Parameters
    ----------
    env_var_name: str
        the name of the environment variable
    path: str
        path to the resource

    Returns
    -------
    export: str
        the export directive
    """
    os.environ[env_var_name] = path
    return "export {0}={1}".format(env_var_name, path)


def AddPathToEnvVar(env_var_name, path):
    """ Add an environment variable

    Parameters
    ----------
    env_var_name: str
        the name of the environment variable
    path: str
        path to the resource

    Returns
    -------
    export: str
        the export directive
    """
    os.environ[env_var_name] = "{0}{1}{2}".format(env_var_name, os.pathsep,
                                                  os.environ.get(path))
    return "export %s=%s%s${%s}" % (env_var_name, path, os.pathsep,
                                    env_var_name)


def SetupServerEnvVar(path2somawf):
    """ Environment variable list

    Parameters
    ----------
    path2somawf: str
        path to the somaworkflow project

    Returns
    -------
    envlines2add: list of str
        the environement variable to export
    """
    envlines2add = []
    envlines2add.append(AddPathToEnvVar("PYTHONPATH", path2somawf))

    if socket.gethostname() == "gabriel.intra.cea.fr":
        envlines2add.append(AddPathToEnvVar(
            "PYTHONPATH",
            os.path.join(path2resources, "lib", "python2.7")))
        envlines2add.append(AddPathToEnvVar(
            "PYTHONPATH",
            os.path.join(path2resources, "lib", "python2.7", "site-packages")))
        envlines2add.append(AddPathToEnvVar(
            "PATH",
            os.path.join(path2resources, "bin")))
        envlines2add.append(AddPathToEnvVar(
            "LD_LIBRARY_PATH",
            os.path.join(path2resources, "lib")))
        envlines2add.append(AddPathToEnvVar(
            "LD_LIBRARY_PATH",
            "/i2bm/brainvisa/CentOS-5.11-x86_64/pbs_drmaa/lib"))
        envlines2add.append(AddPathToEnvVar(
            "LD_LIBRARY_PATH",
            "/usr/lib64/openmpi/lib"))
        envlines2add.append(SetPathToEnvVar(
            "DRMAA_LIBRARY_PATH",
            "/i2bm/brainvisa/CentOS-5.11-x86_64/pbs_drmaa/lib/libdrmaa.so"))

    return envlines2add


#
#                      Server Configuration Tools
#

def ensure_is_dir(d, clear_dir=False):
    """ If the directory doesn't exist, use os.makedirs

    Parameters
    ----------
    d: str
        the directory we want to create
    clear_dir: bool (optional)
        if True clean the directory if it exists
    """
    if not os.path.exists(d):
        os.makedirs(d)
    elif clear_dir:
        shutil.rmtree(d)
        os.makedirs(d)


def GetQueueNamesOnPBSTORQUEServer():
    """ Return a list of queue names on torque pbs

    Returns
    -------
    info_queue: list of str
        the list of queue names on torque pbs
    """
    std_out_lines = subprocess.check_output(["qstat", "-Q"])
    std_out_lines = std_out_lines.splitlines()

    info_queue = []
    # Skip the first line since it is the header
    for line in std_out_lines[2:]:
        line = line.strip()
        queue_item = line.split()[0]
        # Check if the queue item start with an upper or lower character
        if re.match("^[a-zA-Z]", queue_item):
            info_queue.append(queue_item)

    return info_queue


def SetupConfigurationFileOnServer(userid,
                                   ip_address_or_domain,
                                   resource_id=None, options={}):
    """ Create a configuration file on the server side


    It will create the configuration at $HOME/.soma-workflow.cfg if no
    configuration file is detected.

    Parameters
    ----------
    userid: str
        user name on the server side
    ip_address_or_domain: str
        the ip address or the domain of the server
    resource_id: str
        the name of the configuration component
    options: dict
        config options to set in the configuration

    .. note::
       Raises IOError, ValueError when the procedure fails
    """
    # Create a folder to store working items (logs, db, ...)
    home_dir = configuration.Configuration.get_home_dir()
    install_prefix = os.path.join(home_dir, ".soma-workflow")

    # Locate existing config file, create one if necessay
    config_file_path = configuration.Configuration.search_config_path()
    if config_file_path is None:
        config_parser = SafeConfigParser()
        config_file_path = os.path.join(home_dir, ".soma-workflow.cfg")
    else:
        config_parser = read_configuration_file(config_file_path)

    # Create default configuration component name
    if resource_id is None:
        resource_id = "{0}@{1}".format(userid, ip_address_or_domain)

    # Fill the configuration parser if resource id do not exists
    sections = config_parser.sections()
    if not resource_id in sections:
        config_parser.add_section(resource_id)

        db_file = os.path.join(install_prefix, "soma_workflow_{0}.db".format(
                               resource_id))
        config_parser.set(resource_id, configuration.CFG_DATABASE_FILE,
                          db_file)
        transfer_dir = os.path.join(install_prefix,
                                    "transfered_files_{0}".format(resource_id))
        config_parser.set(resource_id, configuration.CFG_TRANSFERED_FILES_DIR,
                          transfer_dir)
        ensure_is_dir(transfer_dir, clear_dir=True)
        config_parser.set(resource_id, configuration.CFG_NAME_SERVER_HOST,
                          ip_address_or_domain)
        config_parser.set(resource_id, configuration.CFG_SERVER_NAME,
                          "soma_workflow_database_" + userid)

        log_file = os.path.join(install_prefix, "logs_{0}".format(resource_id),
                                "log_server")
        config_parser.set(resource_id, configuration.OCFG_SERVER_LOG_FILE,
                          log_file)
        ensure_is_dir(os.path.dirname(log_file), clear_dir=True)
        config_parser.set(resource_id, configuration.OCFG_SERVER_LOG_FORMAT,
                          "%(asctime)s => line %(lineno)s: %(message)s")
        config_parser.set(resource_id, configuration.OCFG_SERVER_LOG_LEVEL,
                          "ERROR")
        config_parser.set(resource_id, configuration.OCFG_ENGINE_LOG_DIR,
                          os.path.join(install_prefix, "logs"))
        config_parser.set(resource_id, configuration.OCFG_ENGINE_LOG_FORMAT,
                          "%(asctime)s => %(module)s line %(lineno)s: "
                          "%(message)s %(threadName)s")
        config_parser.set(resource_id, configuration.OCFG_ENGINE_LOG_LEVEL,
                          "ERROR")

        if 'scheduler_type' not in options \
                or options['scheduler_type'] == 'drmaa':
            info_queue = GetQueueNamesOnPBSTORQUEServer()
            info_queue.insert(0, "")
            info_queue = "{15} ".join(info_queue)
            info_queue += "{15}"

            config_parser.set(resource_id, configuration.OCFG_MAX_JOB_IN_QUEUE,
                          info_queue)

        for opt, value in options.items():
            config_parser.set(resource_id, opt, value)

        WriteOutConfiguration(config_parser, config_file_path)


#
#               Start to check the requirement on the server side
#

cur_version = sys.version_info

# The default python in gabriel is too old for soma-workflow
# Use compiled 2.7 version of python for the setup setup
if (cur_version < req_version and
        socket.gethostname() == "gabriel.intra.cea.fr" and
        os.path.exists(path2resources)):
    logging.info("Use nested python to install")
    cmd = ["{0} '{1}'".format(os.path.join(path2resources, "bin", "python"),
                              path2somawf_setup_server)]
    cmd.extend(sys.argv[1:])
    os.system(" ".join(cmd))
    sys.exit(0)

# Raise Exeption otherwise
if cur_version < req_version or cur_version >= (3, 0):
    raise ImportError("This program requires a python 2 version >= 2.7. "
                      "Please update your python {0}".format(repr(cur_version)))
    sys.exit(0)


#
#               Set environment variables to the bashrc
#

sys.path.append(path2somawf)
envlines2add = SetupServerEnvVar(path2somawf)

# Soma Workflow import
from soma_workflow.configuration import (AddLineDefintions2BashrcFile,
                                         WriteOutConfiguration)
import soma_workflow.configuration as configuration
from setup_client2server import read_configuration_file

AddLineDefintions2BashrcFile(envlines2add)

#
#             Create the configuration file on the server side
#

parser = argparse.ArgumentParser()
parser.add_argument("-r", help="resource id")
parser.add_argument("options", nargs="+", default=[],
                    help="config options, in the shape option=value")

args = parser.parse_args()
if not args.r or args.r == "":
    logging.error("Please enter resource id with -r resource_id")
    sys.exit(0)

resource_id = args.r
options = {}

if len(args.options) != 0:
    for arg in args.options:
        opt, val = arg.split('=')
        options[opt] = val

userid = getpass.getuser()
ip_address_or_domain = socket.gethostname()
SetupConfigurationFileOnServer(userid, ip_address_or_domain, resource_id,
                               options=options)

# Stop running database and start the new one
lines2cmd = [
    ("kill $(ps -ef | grep 'python -m soma_workflow.start_database_server' |"
        "grep '%s' | grep -v grep | awk '{print $2}')") % (userid),
    "python -m soma_workflow.start_database_server %s & bg" % (resource_id)
]

for cmd in lines2cmd:
    print(cmd)
    # os.system("echo '%s' " % (cmd))
    os.system(cmd)
