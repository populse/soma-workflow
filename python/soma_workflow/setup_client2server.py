#! /usr/bin/env python

'''
@author: Jinpeng LI
@contact: mr.li.jinpeng@gmail.coms
@organization: I2BM, Neurospin, Gif-sur-Yvette, France
@organization: CATI, France
@organization: U{IFR 49<http://www.ifr49.org>}

@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''


from __future__ import with_statement

import os
import sys
from soma_workflow.connection import SSHExecCmd, check_if_somawfdb_on_server
import soma_workflow.configuration as configuration
from soma_workflow.configuration import WriteOutConfiguration
from soma_workflow.client import Job, Workflow, WorkflowController


def GetHostNameOnPBSTORQUE(userid, ip_address_or_domain, userpw=''):
    """To get a list of queue names on torque pbs

    Args:
       userid (str):  user name on the server side
       ip_address_or_domain (str): the ip address or the domain of the server
       userpw: user password to login the server using ssh. If you want to use "id_rsa.pub", just leave userpw = ""

    Returns:
       str.  hostname

    Raises:
       paramiko.AuthenticationException


    """
    sshcommand = "hostname"

    outlines = SSHExecCmd(sshcommand, userid, ip_address_or_domain, userpw)

    return outlines[0]


def GetQueueNamesOnPBSTORQUE(userid, ip_address_or_domain, userpw=''):
    """To get a list of queue names on torque pbs

    Args:
       userid (str):  user name on the server side
       ip_address_or_domain (str): the ip address or the domain of the server
       userpw: user password to login the server using ssh. If you want to use "id_rsa.pub", just leave userpw = ""

    Returns:
       list of str.  queue names

    Raises:
       paramiko.AuthenticationException


    """
    sshcommand = "qstat -Q"

    info_queue = []

    outlines = SSHExecCmd(sshcommand, userid, ip_address_or_domain, userpw)

    import re
    sline_idx = 0

    for sline in outlines:

        if sline_idx >= 2:  # skip the first line since it is the header
            sline = sline.strip()
            ssline = sline.split()
            if re.match("^[a-zA-Z]", ssline[0]):
                info_queue.append(ssline[0])

        sline_idx += 1

    # print repr(info_queue)
    return info_queue


def ConfiguratePaser(config_parser, resource_id, ip_address_or_domain, info_queue, userid, installpath, sshport):
    import soma_workflow.configuration as configuration

    oneline_queues = ''
    for one_q in info_queue:
        oneline_queues = oneline_queues+one_q+" "

    config_parser.add_section(resource_id)
    config_parser.set(
        resource_id, configuration.CFG_CLUSTER_ADDRESS, ip_address_or_domain)
    config_parser.set(
        resource_id, configuration.CFG_SUBMITTING_MACHINES, ip_address_or_domain)
    config_parser.set(resource_id, configuration.OCFG_QUEUES, oneline_queues)
    config_parser.set(resource_id, configuration.OCFG_LOGIN, userid)

    if sshport == None:
        sshport = 22
    config_parser.set(resource_id, configuration.OCFG_SSHPort, str(sshport))
    config_parser.set(resource_id, configuration.OCFG_INSTALLPATH, installpath)

    return config_parser


def RemoveResNameOnConfigureFile(resName, config_file_path=None):
    import sys
    import os
    import ConfigParser
    from ConfigParser import SafeConfigParser

    if config_file_path == None:
        config_file_path = configuration.Configuration.search_config_path()

    if config_file_path == None:
        return

    config_parser = SafeConfigParser()
    try:
        config_parser.read(config_file_path)
    except:
        strmsg = "Cannot open %s \n" % (config_file_path)
        sys.stderr.write(strmsg)
        raise Exception(strmsg)

    config_parser.remove_section(resName)

    WriteOutConfiguration(config_parser, config_file_path)

    return config_parser


def SetupConfigurationFileOnClient(resource_id,
                                   userid,
                                   ip_address_or_domain, userpw=None,
                                   install_swf_path_server=None,
                                   sshport=22):
    """To setup the configuration file on the client part

    Args:
       userid (str):  user name on the server side
       ip_address_or_domain (str): the ip address or the domain of the server
       userpw: user password to login the server using ssh. If you want to use "id_rsa.pub", just leave userpw = ""

    Raises:
       IOError, ValueError

    It will search the configuration file on the client.
    If it exists, it will keep the original configuration,
    if not, it will create the new configuration at $HOME/.soma-workflow.cfg

    """

    # ouput the configuration file
    import sys
    import os
    import ConfigParser
    from ConfigParser import SafeConfigParser

    config_file_path = configuration.Configuration.search_config_path()
    hostname = GetHostNameOnPBSTORQUE(userid, ip_address_or_domain, userpw)
    # resource_id="%s@%s"%(userid,hostname)
    # print "resource_id="+resource_id

    if config_file_path == None:

        home_dir = configuration.Configuration.get_home_dir()
        config_file_path = os.path.join(home_dir, ".soma-workflow.cfg")

        print "No configuration file on the client, we create a new one at %s" % (config_file_path)

        info_queue = GetQueueNamesOnPBSTORQUE(
            userid, ip_address_or_domain, userpw)
        config_parser = SafeConfigParser()

        config_parser = ConfiguratePaser(config_parser,
                                         resource_id,
                                         hostname,
                                         info_queue,
                                         userid,
                                         install_swf_path_server,
                                         sshport)
        WriteOutConfiguration(config_parser, config_file_path)

    else:
        print "Configuration file is found at: "+config_file_path
        config_parser = SafeConfigParser()
        config_parser.read(config_file_path)
        list_sections = config_parser.sections()

        if any(sec == resource_id for sec in list_sections):
            pass
        else:
            info_queue = GetQueueNamesOnPBSTORQUE(
                userid, ip_address_or_domain, userpw)
            config_parser = ConfiguratePaser(config_parser,
                                             resource_id,
                                             ip_address_or_domain,
                                             info_queue, userid,
                                             install_swf_path_server, sshport)
            WriteOutConfiguration(config_parser, config_file_path)


def CopySomaWF2Server(install_swf_path_server,
                      userid,
                      ip_address_or_domain,
                      userpw='', sshport=22):
    install_swf_path_server = os.path.join(install_swf_path_server, "python")
    install_swf_path_server_soma_workflow = os.path.join(
        install_swf_path_server, "./")
    install_swf_path_server_drmaa = os.path.join(install_swf_path_server)

    std_out_lines = SSHExecCmd(
        "mkdir -p '%s'" % (install_swf_path_server_drmaa),
        userid, ip_address_or_domain, userpw, sshport=sshport)
    print std_out_lines
    std_out_lines = SSHExecCmd(
        "mkdir -p '%s'" % (install_swf_path_server_soma_workflow),
        userid, ip_address_or_domain, userpw, sshport=sshport)
    print std_out_lines

    path2somawf = os.path.dirname(os.path.realpath(__file__))
    path2drmaa = os.path.join(path2somawf, "..", "somadrmaa")

    sshcommand = "scp -rC '%s' %s@%s:'%s'" % (path2somawf,
                                              userid,
                                              ip_address_or_domain,
                                              install_swf_path_server_soma_workflow)
    print "sshcommand="+sshcommand
    os.system(sshcommand)

    sshcommand = "scp -rC '%s' %s@%s:'%s'" % (path2drmaa,
                                              userid,
                                              ip_address_or_domain,
                                              install_swf_path_server_drmaa)
    print "sshcommand="+sshcommand
    os.system(sshcommand)

    SSHExecCmd("echo >> '%s'" % (os.path.join(
        install_swf_path_server_soma_workflow,
        "__init__.py")),
        userid,
        ip_address_or_domain,
        userpw,
        sshport=sshport)


def SimpleJobExample(resName, login, password):

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

    controller = WorkflowController(resName, login, password)

    controller.submit_workflow(workflow=workflow,
                               name="TestConnectionExample")





def InstallSomaWF2Server(install_swf_path_server, ResName, userid, ip_address_or_domain, userpw='', sshport=22):

    CopySomaWF2Server(install_swf_path_server,
                      userid, ip_address_or_domain, userpw, sshport)

    script2install = os.path.join(
        install_swf_path_server, "python", "soma_workflow", "setup_server.py")

    command = "python '%s' -r %s " % (script2install, ResName)
    print "ssh command="+command

    SSHExecCmd(command, userid, ip_address_or_domain,
               userpw, wait_output=False, sshport=sshport)

    SetupConfigurationFileOnClient(
        ResName, userid, ip_address_or_domain, userpw, install_swf_path_server, sshport)

    os.system("sleep 5")

    # submit a workflow for test
    SimpleJobExample(ResName, userid, userpw)


def SetupSomaWF2Server(install_swf_path_server, ResName, userid, ip_address_or_domain, userpw='', sshport=22):

    if check_if_somawfdb_on_server(ResName, userid, ip_address_or_domain, userpw, sshport):
        msg = "Cannot find the soma-workflow on the server. Please first install soma-workflow"
        sys.stderr.write(msg)
        raise Exception(msg)

    SetupConfigurationFileOnClient(
        ResName, userid, ip_address_or_domain, userpw, install_swf_path_server, sshport)

    # submit a workflow for test
    SimpleJobExample(ResName, userid, userpw)


def RemoveSomaWF2Server(install_swf_path_server, ResName, userid, ip_address_or_domain, userpw='', sshport=22):

#    print "install_swf_path_server="+install_swf_path_server
#    print "ResName="+ResName
#    print "userid="+userid
#    print "ip_address_or_domain="+ip_address_or_domain
#    print "userpw="+userpw
#    print "sshport="+repr(sshport)

    # clean_script_server=os.path.join(install_swf_path_server,"python","soma","workflow","clean_server.py")

    clean_script_server = install_swf_path_server + \
        "/python/soma_workflow/clean_server.py"
    command = "python '%s' -r %s " % (clean_script_server, ResName)
    print command
    SSHExecCmd(command, userid, ip_address_or_domain,
               userpw, wait_output=False, sshport=sshport)
    os.system("sleep 5")

    command = "rm -rf '%s'" % (install_swf_path_server)
    print command
    SSHExecCmd(command, userid, ip_address_or_domain,
               userpw, wait_output=False, sshport=sshport)

    RemoveResNameOnConfigureFile(ResName)

    pass
