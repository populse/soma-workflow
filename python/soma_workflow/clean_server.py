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



resName= None

i=0
while i < len(sys.argv):
  if sys.argv[i] == "-r" :
    resName=sys.argv[i+1]
    break
  i=i+1
 
lines2cmd = [
             "kill $(ps -ef | grep 'python -m soma_workflow.start_database_server' | grep '%s' \
| grep -v grep | awk '{print $2}')"%(resName),
            "rm ~/.soma-workflow.cfg"
             ]


for line2cmd in lines2cmd:
    os.system("echo '%s' "%(line2cmd))
    os.system(line2cmd)

