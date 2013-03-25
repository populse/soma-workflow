#! /usr/bin/env python

'''
@author: Jinpeng LI
@contact: mr.li.jinpeng@gmail.com
@organization: CEA, I2BM, Neurospin, Gif-sur-Yvette, France

@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''

from __future__ import with_statement

import os
import sys

path2somawf = os.path.dirname(os.path.realpath(__file__))
path2somawfpy = os.path.join(path2somawf,"python")
sys.path.append(path2somawfpy)

from soma.workflow.configuration import AddLineDefintions2BashrcFile,WriteOutConfiguration

'''
Start to check the requirement on the server side
'''
req_version = (2,7)
cur_version = sys.version_info

if cur_version < req_version or cur_version >= (3,0):
    print "This program requires a python version >= 2.7 and please update your python %s \
    to latest version of python2."%(repr(cur_version))
    sys.exit(0)
'''
end to check the requirement on the server side
'''

def SSHExecCmd(sshcommand,userid,ip_address_or_domain,userpw='',wait_output=True):
    if(wait_output==True):
        tag='----xxxx=====start to exec=====xxxxx----'
        sshcommand="echo %s && %s"%(tag,sshcommand)
    
    stdin   = []
    stdout  = []
    stderr  = []
    
    std_out_lines = []
    
    import paramiko
    
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy()) 
        client.load_system_host_keys()
        client.connect(hostname=ip_address_or_domain, port=22, username=userid, password=userpw)
        stdin, stdout, stderr = client.exec_command(sshcommand)
    except paramiko.AuthenticationException, e:
        print "The authentification failed. %s. Please check your user and password. \
        You can test the connection in terminal or command: ssh %s@%s" %(e,userid,ip_address_or_domain)
        raise
    except Exception, e:
        print "Can not use ssh to log on the remote machine. Please Make sure your network is connected %s.\
             You can test the connection in terminal or command: ssh %s@%s" %(e,userid,ip_address_or_domain)
        raise 
    
    if wait_output:
        sline=stdout.readline()
        while sline and sline!=tag+'\n':
            sline=stdout.readline()
    
        sline=stdout.readline()
        while sline:
            std_out_lines.append(sline.strip())
            sline=stdout.readline()
            
    
    client.close()
    
    return std_out_lines

def GetHostNameOnPBSTORQUE(userid,ip_address_or_domain,userpw=''):
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

    outlines=SSHExecCmd(sshcommand,userid,ip_address_or_domain,userpw)
    
    return outlines[0]

def GetQueueNamesOnPBSTORQUE(userid,ip_address_or_domain,userpw=''):
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

    outlines=SSHExecCmd(sshcommand,userid,ip_address_or_domain,userpw)

    import re
    sline_idx=0
    
    for sline in outlines:
        
        if sline_idx >= 2: # skip the first line since it is the header
            sline = sline.strip()
            ssline = sline.split()
            if re.match("^[a-zA-Z]", ssline[0]):
                info_queue.append(ssline[0])
        
        sline_idx += 1


    #print repr(info_queue)

    return info_queue


def ConfiguratePaser(config_parser,resource_id,ip_address_or_domain,info_queue,userid):
    import soma.workflow.configuration as configuration
    
    oneline_queues=''
    for one_q in info_queue:
        oneline_queues=oneline_queues+one_q+" "
                    
    config_parser.add_section(resource_id)
    config_parser.set(resource_id,configuration.CFG_CLUSTER_ADDRESS,ip_address_or_domain)
    config_parser.set(resource_id,configuration.CFG_SUBMITTING_MACHINES,ip_address_or_domain)
    config_parser.set(resource_id,configuration.OCFG_QUEUES,oneline_queues)
    config_parser.set(resource_id,configuration.OCFG_LOGIN,userid)
    return config_parser



def SetupConfigurationFileOnClient(userid,ip_address_or_domain,userpw=""):
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
    #ouput the configuration file 
    import sys
    import os
    import ConfigParser
    from ConfigParser import SafeConfigParser
    

    
    import soma.workflow.configuration as configuration


    config_file_path = configuration.Configuration.search_config_path()
    hostname=GetHostNameOnPBSTORQUE(userid,ip_address_or_domain,userpw)
    resource_id="%s@%s"%(userid,hostname)
    #print "resource_id="+resource_id
    
    
    if not config_file_path:
        print "No configuration file on the client"
        
        info_queue=GetQueueNamesOnPBSTORQUE(userid, ip_address_or_domain,userpw)
        config_parser = SafeConfigParser()
        home_dir = configuration.Configuration.get_home_dir() 
        config_path = os.path.join(home_dir, ".soma-workflow.cfg")
        
        config_parser=ConfiguratePaser(config_parser,resource_id,hostname,info_queue,userid)
        WriteOutConfiguration(config_parser,config_path)


    else :
        print "Configuration file is found at: "+config_file_path
        config_parser = SafeConfigParser()
        config_parser.read(config_file_path)
        list_sections=config_parser.sections()

        if any( sec ==  resource_id for sec in list_sections):
            pass
        else:
            info_queue=GetQueueNamesOnPBSTORQUE(userid, ip_address_or_domain,userpw)
            config_parser=ConfiguratePaser(config_parser,resource_id,hostname,info_queue,userid)
            WriteOutConfiguration(config_parser,config_file_path)


lines2add = [
             "SOMAWF_PATH=%s"%(path2somawf),
             'export PATH=$SOMAWF_PATH/bin:$PATH',
             'export PYTHONPATH=$SOMAWF_PATH/python:$PYTHONPATH',
             'export SOMA_WORKFLOW_EXAMPLES=$SOMAWF_PATH/test/jobExamples/',
             'export SOMA_WORKFLOW_EXAMPLES_OUT=$SOMAWF_PATH/test/jobExamples_out/']

AddLineDefintions2BashrcFile(lines2add)


print "Enviroment variable configuration is done."

print "Start to configuration file on client side:"
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("-u", help="User name on the server using ssh connection")
parser.add_argument("-p", help="Password on the server using ssh connection. \
                                If you want to use 'id_rsa.pub' in $HOME/.ssh, just don't set any value.")
parser.add_argument("-s", help="Server's ip address or domain using ssh connection")
parser.add_argument("-i", help="If you need install soma-workflow on the remote server, you can set a path, for example: -i ~/soma-workflow\
                                If you don't need to install soma-workflow on the remote server, just don't set any value.")

args = parser.parse_args()



if not args.u or args.u == '':
    print "Please give the user name on the server:"
    args.u=sys.stdin.readline()
    args.u=args.u.strip()
    if not args.u or args.u == '':
        sys.exit(0)

if not args.s or args.s == '':
    print "Please give the ip address or the domain of the server:"
    args.s=sys.stdin.readline()
    args.s=args.s.strip()
    if not args.s or args.s == '':
        sys.exit(0)

#if not args.i or args.i=='':
#    print "If you want to install soma-workflow on the server, \
#please entre a path, for example: -i ~/soma-workflow. \
#Otherwise, just press 'entre' to leave it empty:"
#    args.i=sys.stdin.readline()
#    args.i=args.i.strip()


userid=args.u
ip_address_or_domain=args.s
userpw=''
if args.p and args.p!="":
    userpw=args.p


SetupConfigurationFileOnClient(userid,ip_address_or_domain,userpw)


#if args.i and args.i!='':
#    install_swf_path_server=args.i
#    print "Start to copy soma-workflow file to the server path=%s."%(args.i)
#    sshcommand="scp -rC '%s' %s@%s:'%s'"%(path2somawf, userid,ip_address_or_domain,install_swf_path_server)
#    os.system(sshcommand)
#    install_swf_path_server_setup=os.path.join(install_swf_path_server,"setup_server.py")
#    sshcommand="python '%s'"%(install_swf_path_server_setup)
#    print 'ssh command='+sshcommand
#    SSHExecCmd(sshcommand,userid,ip_address_or_domain,userpw, False)

print "Configuration Done ! Close terminal and then start terminal to use $ soma_workflow_gui"
