#! /usr/bin/env python

'''
@author: Jinpeng LI
@contact: mr.li.jinpeng@gmail.com

@organization: I2BM, Neurospin, Gif-sur-Yvette, France
@organization: CATI, France
@organization: U{IFR 49<http://www.ifr49.org>}

@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''

'''
start to check the requirement on the server side
'''
import os
import sys
import socket
import subprocess
import platform

path2somawf = os.path.dirname(os.path.realpath(__file__))
path2somawfpy = os.path.join(path2somawf,"..","..")
path2somawf_setup_server = os.path.realpath(__file__)
sys.path.append(path2somawfpy)


def SetPathToEnvVar(KeyPath,NewPath):
    os.environ[KeyPath] = NewPath
    res = ""
    if platform.system()=='Windows':
        res = "set %s=%s"%(KeyPath,NewPath)
    else:
        res = "export %s=%s"%(KeyPath,NewPath)
    return res
        
def AddPathToEnvVar(KeyPath,NewPath):
    os.environ[KeyPath] = "%s%s%s"%(NewPath,os.pathsep,os.environ.get(KeyPath))
    res = ""
    
    if platform.system()=='Windows':
        res = "set %s=%s%s%%%s%%"%(KeyPath,NewPath,os.pathsep,KeyPath)
    else:
        res = "export %s=%s%s${%s}"%(KeyPath,NewPath,os.pathsep,KeyPath)
        
    return res
        


def SetupServerEnvVar(path2somawfpy):
    '''
    Configurate the environment variable configuration
    '''
    envlines2add = [ ]
    
    envlines2add.append(AddPathToEnvVar("PYTHONPATH",path2somawfpy))

    import socket
    if socket.gethostname()=="gabriel.intra.cea.fr":
        envlines2add.append(AddPathToEnvVar("PYTHONPATH","/i2bm/brainvisa/CentOS-5.3-x86_64/python-2.7.3/lib/python2.7"))
        envlines2add.append(AddPathToEnvVar("PYTHONPATH","/i2bm/brainvisa/CentOS-5.3-x86_64/python-2.7.3/lib/python2.7/site-packages"))
        envlines2add.append(AddPathToEnvVar("PATH","/i2bm/brainvisa/CentOS-5.3-x86_64/python-2.7.3/bin"))
        envlines2add.append(AddPathToEnvVar("LD_LIBRARY_PATH","/i2bm/brainvisa/CentOS-5.3-x86_64/python-2.7.3/lib"))
        envlines2add.append(AddPathToEnvVar("LD_LIBRARY_PATH","/i2bm/brainvisa/CentOS-5.3-x86_64/pbs_drmaa-1.0.13/lib"))
        envlines2add.append(AddPathToEnvVar("LD_LIBRARY_PATH","/usr/lib64/openmpi/lib"))
        envlines2add.append(SetPathToEnvVar("DRMAA_LIBRARY_PATH","/i2bm/brainvisa/CentOS-5.3-x86_64/pbs_drmaa-1.0.13/lib/libdrmaa.so"))
    
    return envlines2add


envlines2add=SetupServerEnvVar(path2somawfpy)

'''
Start to check the requirement on the server side
'''
req_version = (2,7)
cur_version = sys.version_info

if (cur_version < req_version 
and socket.gethostname()=="gabriel.intra.cea.fr" 
and os.path.exists("/i2bm/brainvisa/CentOS-5.3-x86_64/python-2.7.3/bin")):
    '''
    The default python in gabriel is too old to soma-workflow
    Use new version of python to setup since we have defined some enviroment in SetupServerEnvVar for gabriel.intra.cea.fr
    '''
    print "Use nested python to install"

    strcmd="/i2bm/brainvisa/CentOS-5.3-x86_64/python-2.7.3/bin/python '%s'"%(path2somawf_setup_server)

    i=1
    while i < len(sys.argv):
      # print sys.argv[i]
      strcmd=strcmd+" %s"%(sys.argv[i])
      i=i+1

    #print strcmd

    os.system(strcmd)
    sys.exit(0)
    

if cur_version < req_version or cur_version >= (3,0):
    print "This program requires a python version >= 2.7 and please update your python %s \
    to latest version from python2."%(repr(cur_version))
    sys.exit(0)
'''
end to check the requirement on the server side
'''


from soma_workflow.configuration import AddLineDefintions2BashrcFile,WriteOutConfiguration
import soma_workflow.configuration as configuration


def GetQueueNamesOnPBSTORQUEServer():
    import re
    
    info_queue =[]
    
    info_queue_out=subprocess.check_output(['qstat', '-Q'])
    
    info_queue_lines=info_queue_out.split('\n')
    
    sline_idx=0
    for info_queue_line in info_queue_lines:
        if sline_idx >= 2: # skip the first line since it is the header
            sline = info_queue_line.strip()
            ssline = sline.split()
            if len(ssline)>=1 :
                if re.match("^[a-zA-Z]", ssline[0]):
                    #print repr(ssline)
                    info_queue.append(ssline[0])
        sline_idx+=1
    
    return info_queue

def SetupConfigurationFileOnServer(userid,  ip_address_or_domain,   resource_id=None):
    """To setup the configuration file on the client part
     
    Args:
       userid (str):  user name on the server side
       ip_address_or_domain (str): the ip address or the domain of the server

    Raises:
       IOError, ValueError

    It will create the configuration at $HOME/.soma-workflow.cfg

    """
    #ouput the configuration file 
    import sys
    import os
    from ConfigParser import SafeConfigParser

    config_file_path = configuration.Configuration.search_config_path()
    
    if resource_id==None:
        resource_id="%s@%s"%(userid,ip_address_or_domain)
        
    
    home_dir = configuration.Configuration.get_home_dir() 
    install_prefix=os.path.join(home_dir,".soma-workflow")
    
    if config_file_path==None:
        config_file_path=os.path.join(home_dir,".soma-workflow.cfg")
    
#    print "config_file_path="+config_file_path
#    print "resource_id="+resource_id
#    print "home_dir="+home_dir
#    print "config_path="+config_path
#    print "install_prefix="+install_prefix
    
    config_parser = SafeConfigParser()
    config_parser.add_section(resource_id)
    
    config_parser.set(resource_id,configuration.CFG_DATABASE_FILE,          os.path.join(install_prefix,"soma_workflow.db"))
    config_parser.set(resource_id,configuration.CFG_TRANSFERED_FILES_DIR,   os.path.join(install_prefix,"transfered-files"))
    config_parser.set(resource_id,configuration.CFG_NAME_SERVER_HOST,       ip_address_or_domain)
    config_parser.set(resource_id,configuration.CFG_SERVER_NAME,            "soma_workflow_database_"+userid)
    
    config_parser.set(resource_id,configuration.OCFG_SERVER_LOG_FILE,       os.path.join(install_prefix,"logs","log_server"))
    config_parser.set(resource_id,configuration.OCFG_SERVER_LOG_FORMAT,     "%(asctime)s => line %(lineno)s: %(message)s")
    config_parser.set(resource_id,configuration.OCFG_SERVER_LOG_LEVEL,      "ERROR")
    config_parser.set(resource_id,configuration.OCFG_ENGINE_LOG_DIR,        os.path.join(install_prefix,"logs"))
    config_parser.set(resource_id,configuration.OCFG_ENGINE_LOG_FORMAT,     "%(asctime)s => %(module)s line %(lineno)s: %(message)s                 %(threadName)s")
    config_parser.set(resource_id,configuration.OCFG_ENGINE_LOG_LEVEL,      "ERROR")

    info_queue=GetQueueNamesOnPBSTORQUEServer()
    
    str_info_q="{15} "
    
    for e_info_queue in info_queue:
        str_info_q=str_info_q+e_info_queue+"{15} "
    str_info_q=str_info_q.strip()
    
    config_parser.set(resource_id,configuration.OCFG_MAX_JOB_IN_QUEUE,      str_info_q)
    
    WriteOutConfiguration(config_parser,config_file_path)


AddLineDefintions2BashrcFile(envlines2add)


import argparse
parser = argparse.ArgumentParser()
parser.add_argument("-r", help="resource id")

args = parser.parse_args()
if not args.r or args.r == '':
    print "Please enter resource id with -r resource_id"
    sys.exit(0)

resource_id=args.r


import getpass
userid=getpass.getuser()
ip_address_or_domain=socket.gethostname()
#resource_id="%s@%s"%(userid,ip_address_or_domain)

SetupConfigurationFileOnServer(userid,ip_address_or_domain,resource_id)

 
lines2cmd = [
             "kill $(ps -ef | grep 'python -m soma_workflow.start_database_server' | grep '%s' \
| grep -v grep | awk '{print $2}')"%(userid),
            "rm ~/.soma-workflow/*.db",
            "rm -rf ~/.soma-workflow/transfered-files/*",
            "rm -rf ~/.soma-workflow/logs/*",
            "mkdir ~/.soma-workflow/transfered-files",
            "mkdir ~/.soma-workflow/logs",
            "python -m soma_workflow.start_database_server %s & bg"%(resource_id)
             ]


for line2cmd in lines2cmd:
    os.system("echo '%s' "%(line2cmd))
    os.system(line2cmd)

