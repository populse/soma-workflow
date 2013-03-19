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


def AddLineDefintions2BashrcFile(lines2add,path2bashrc=""):
    """Add line defintions to the ~/.bashrc file 
    
    Removing the lines2add which are already exsting in the ~/.bashrc
    Adding the lines2add at the end of ~/.bashrc
    
    Args:
        lines2add (string):  a list of line definitions. Line defintion like export PATH=~/mylocal/bin:${PATH}
        path2bashrc (string, optional): path to the ./bashrc file. It could be another path. Default paht is "~/.bashrc"

    Raises:
       IOError, ValueError
       
    Example:
        lines2add = ["SOMAWF_PATH=%s/soma-workflow"%(os.getenv("HOME")),
        'export PATH=$SOMAWF_PATH/bin:$PATH',
        'export PYTHONPATH=$SOMAWF_PATH/python:$PYTHONPATH',
        'export SOMA_WORKFLOW_EXAMPLES=$SOMAWF_PATH/test/jobExamples/',
        'export SOMA_WORKFLOW_EXAMPLES_OUT=$SOMAWF_PATH/test/jobExamples_out/']
        >>> print AddVariables2BashrcFile(lines2add, "~/.bashrc")
    """
    import os
    import sys
    if path2bashrc=="" :
        
        path2bashrc = os.path.join(os.getenv("HOME"),".bashrc")
        
    lines2rm = []
    content=[]

    try:
        with open(path2bashrc) as f:
            content = f.readlines()
    except IOError as e:
        print "I/O error({0}): {1}".format(e.errno, e.strerror)
        print "%s does not exist, the system will create the new file"% (path2bashrc)
    except ValueError:
        print "Could not convert data to an integer."
    except:
        print "Unexpected error:", sys.exc_info()[0]
        raise
    
    
    for i in range(len(content)):
        content[i]=content[i].strip()

    #try to find the duplicated paths and remove them
    for line2add in lines2add:
        isfound= any(line2add.strip()==cline.strip() for cline in content)
        while isfound:
            content.remove(line2add.strip())
            isfound= any(line2add.strip()==cline.strip() for cline in content)

    for line2add in lines2add:
        content.append(line2add)

    try:
        with open(path2bashrc,'w') as f:
            for cline in content:
                f.write(cline+"\n")
    except IOError as e:
        print "I/O error({0}): {1}".format(e.errno, e.strerror)
        print "The system cannot write the file %s. Please make sure it can be wrote. "% (path2bashrc)
        raise e
    except ValueError:
        print "Could not convert data to an integer."
        raise ValueError
    except:
        print "Unexpected error:", sys.exc_info()[0]
        raise


def GetQueueNamesOnPBSTORQUE(userid,ip_address_or_domain,userpw=''):


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
        print "The authentification failed. %s. Please check your user and password. " %(e)
        raise
    except Exception, e:
        print "Can not use ssh to log on the remote machine. Please Make sure your network is connected %s" %(e)
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



lines2add = [
             "SOMAWF_PATH=%s/soma-workflow"%(os.getenv("HOME")),
             'export PATH=$SOMAWF_PATH/bin:$PATH',
             'export PYTHONPATH=$SOMAWF_PATH/python:$PYTHONPATH',
             'export SOMA_WORKFLOW_EXAMPLES=$SOMAWF_PATH/test/jobExamples/',
             'export SOMA_WORKFLOW_EXAMPLES_OUT=$SOMAWF_PATH/test/jobExamples_out/']

# AddLineDefintions2BashrcFile(lines2add)


userid='ed203246'
ip_address_or_domain='gabriel.intra.cea.fr'
userpw='0Scard99'


info_queue=GetQueueNamesOnPBSTORQUE(userid, ip_address_or_domain,userpw)
# print repr(info_queue)

