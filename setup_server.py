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
import socket

print "hostname="+socket.gethostname()

path2somawf = os.getenv("PWD")
path2somawfpy = os.path.join(path2somawf,"python")
sys.path.append(path2somawfpy)

import soma.workflow.configuration as configuration


lines2add = [
            "SOMAWF_PATH=%s"%(path2somawf),
            'export PATH=$SOMAWF_PATH/bin:$PATH',
            'export PYTHONPATH=$SOMAWF_PATH/python:$PYTHONPATH',
            'export SOMA_WORKFLOW_EXAMPLES=$SOMAWF_PATH/test/jobExamples/',
            'export SOMA_WORKFLOW_EXAMPLES_OUT=$SOMAWF_PATH/test/jobExamples_out/',
            
            "export PYTHONPATH=/i2bm/brainvisa/CentOS-5.3-x86_64/python-2.7.3/lib/python2.7:$PYTHONPATH",
            "export PYTHONPATH=/i2bm/brainvisa/CentOS-5.3-x86_64/python-2.7.3/lib/python2.7/site-packages:$PYTHONPATH",
            "export PATH=/i2bm/brainvisa/CentOS-5.3-x86_64/python-2.7.3/bin:$PATH",
            "export LD_LIBRARY_PATH=/i2bm/brainvisa/CentOS-5.3-x86_64/python-2.7.3/lib:$LD_LIBRARY_PATH",
            "export LD_LIBRARY_PATH=/i2bm/brainvisa/CentOS-5.3-x86_64/pbs_drmaa-1.0.13/lib/:$LD_LIBRARY_PATH",
            "export LD_LIBRARY_PATH=/usr/lib64/openmpi/lib/:$LD_LIBRARY_PATH",
            "export DRMAA_LIBRARY_PATH=/i2bm/brainvisa/CentOS-5.3-x86_64/pbs_drmaa-1.0.13/lib/libdrmaa.so"
             ]

configuration.AddLineDefintions2BashrcFile(lines2add)






