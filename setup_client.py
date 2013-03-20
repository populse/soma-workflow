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

path2somawf = os.path.join(os.getenv("PWD"),"python")
sys.path.append(path2somawf)

import soma.workflow.configuration.AddLineDefintions2BashrcFile as AddLineDefintions2BashrcFile




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

    stdin   = []
    stdout  = []
    stderr  = []
    
    info_queue = []

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


    import re
    sline_idx=0
    
    sline=stdout.readline()
    sline_idx += 1
    while (sline):
        
        if sline_idx >= 2: # skip the first line since it is the header
            sline = sline.strip()
            ssline = sline.split()
            if re.match("^[a-zA-Z]", ssline[0]):
                #print repr(ssline)
                info_queue.append(ssline[0])
            
        sline=stdout.readline()
        sline_idx += 1

    #print repr(info_queue)

    client.close()
    
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

def WriteOutConfiguration(config_parser,config_path):
    try:
        with open(config_path,'w') as cfgfile:
            config_parser.write(cfgfile)
    except IOError as e:
        print "I/O error({0}): {1}".format(e.errno, e.strerror)
        print "The system cannot write the file %s. Please make sure that it can be written. "% (config_path)
        raise e
    except ValueError:
        print "Could not convert data to an integer.%s" % (config_path)
        raise ValueError
    except:
        print "Unexpected error:", sys.exc_info()[0]
        raise

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
    resource_id="%s@%s"%(userid,ip_address_or_domain)
    #print "resource_id="+resource_id
    
    
    if not config_file_path:
        print "No configuration file on the client"
        
        info_queue=GetQueueNamesOnPBSTORQUE(userid, ip_address_or_domain,userpw)
        config_parser = SafeConfigParser()
        home_dir = configuration.Configuration.get_home_dir() 
        config_path = os.path.join(home_dir, ".soma-workflow.cfg")
        config_parser=ConfiguratePaser(config_parser,resource_id,ip_address_or_domain,info_queue,userid)
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
            config_parser=ConfiguratePaser(config_parser,resource_id,ip_address_or_domain,info_queue,userid)
            WriteOutConfiguration(config_parser,config_file_path)



lines2add = [
             "SOMAWF_PATH=%s/github/soma-workflow"%(os.getenv("HOME")),
             'export PATH=$SOMAWF_PATH/bin:$PATH',
             'export PYTHONPATH=$SOMAWF_PATH/python:$PYTHONPATH',
             'export SOMA_WORKFLOW_EXAMPLES=$SOMAWF_PATH/test/jobExamples/',
             'export SOMA_WORKFLOW_EXAMPLES_OUT=$SOMAWF_PATH/test/jobExamples_out/']

# AddLineDefintions2BashrcFile(lines2add)


userid='ed203246'
ip_address_or_domain='gabriel.intra.cea.fr'
userpw='xxxx'

#info_queue=GetQueueNamesOnPBSTORQUE(userid, ip_address_or_domain,userpw)

SetupConfigurationFileOnClient(userid,ip_address_or_domain,userpw)

install_swf_path_server = "/home/ed203246/soma_wf_tmp"



