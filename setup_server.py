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
import pexpect
import subprocess


path2somawf = os.path.dirname(os.path.realpath(__file__))
path2somawfpy = os.path.join(path2somawf,"python")
sys.path.append(path2somawfpy)

from soma.workflow.configuration import AddLineDefintions2BashrcFile,WriteOutConfiguration
import soma.workflow.configuration as configuration

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

def SetupConfigurationFileOnServer(userid,ip_address_or_domain):
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
    resource_id="%s@%s"%(userid,ip_address_or_domain)
    home_dir = configuration.Configuration.get_home_dir() 
    config_path = os.path.join(home_dir, ".soma-workflow.cfg")
    install_prefix=os.path.join(home_dir,".soma-workflow")
    
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
    
    
    



lines2add = [
            "SOMAWF_PATH=%s"%(path2somawf),
            'export PATH=$SOMAWF_PATH/bin:$PATH',
            'export PYTHONPATH=$SOMAWF_PATH/python:$PYTHONPATH',
            'export LD_LIBRARY_PATH=$SOMAWF_PATH/lib:${LD_LIBRARY_PATH}'
            'export SOMA_WORKFLOW_EXAMPLES=$SOMAWF_PATH/test/jobExamples/',
            'export SOMA_WORKFLOW_EXAMPLES_OUT=$SOMAWF_PATH/test/jobExamples_out/'
             ]

import socket
if socket.gethostname()=="gabriel.intra.cea.fr":
    lines2add.append("export PYTHONPATH=/i2bm/brainvisa/CentOS-5.3-x86_64/python-2.7.3/lib/python2.7:$PYTHONPATH")
    lines2add.append("export PYTHONPATH=/i2bm/brainvisa/CentOS-5.3-x86_64/python-2.7.3/lib/python2.7/site-packages:$PYTHONPATH")
    lines2add.append("export PATH=/i2bm/brainvisa/CentOS-5.3-x86_64/python-2.7.3/bin:$PATH")
    lines2add.append("export LD_LIBRARY_PATH=/i2bm/brainvisa/CentOS-5.3-x86_64/python-2.7.3/lib:$LD_LIBRARY_PATH")
    lines2add.append("export LD_LIBRARY_PATH=/i2bm/brainvisa/CentOS-5.3-x86_64/pbs_drmaa-1.0.13/lib/:$LD_LIBRARY_PATH")
    lines2add.append("export LD_LIBRARY_PATH=/usr/lib64/openmpi/lib/:$LD_LIBRARY_PATH")
    lines2add.append("export DRMAA_LIBRARY_PATH=/i2bm/brainvisa/CentOS-5.3-x86_64/pbs_drmaa-1.0.13/lib/libdrmaa.so")

AddLineDefintions2BashrcFile(lines2add)

for line2add in lines2add:
    os.system(line2add)


userid="ed203246"
ip_address_or_domain=socket.gethostname()
resource_id="%s@%s"%(userid,ip_address_or_domain)

SetupConfigurationFileOnServer(userid,ip_address_or_domain)


lines2cmd = [
             "rm -rf %s/build && \
             mkdir %s/build && \
             cd %s/build && \
             cmake -DCMAKE_INSTALL_PREFIX:PATH=%s %s && \
             make && \
             make install "%(path2somawf,path2somawf,path2somawf,path2somawf,path2somawf),
             "mkdir ~/.soma-workflow && mkdir ~/.soma-workflow/transfered-files && mkdir ~/.soma-workflow/logs ",
             "python -m soma.workflow.start_database_server %s && bg"%(resource_id)
             ]

for line2cmd in lines2cmd:
    os.system(line2cmd)

