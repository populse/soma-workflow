'''
The L{JobScheduler} allows to submit jobs to predefined sets of machines
linked together via a distributed resource management systems (DRMS) like 
Condor, SGE, LSF, etc. It requires a instance of L{JobServer} to be available.

@author: Yann Cointepas
@author: Soizic Laguitton
@organization: U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''
__docformat__ = "epytext en"

import shutil

class FileCopier( object ):
  def copyRemoteToLocal(self, remote_file, local_file_path):
    pass 
    
  def copyLocalToRemote(self, local_file, remote_file_path):
    pass 
  
  
class LocalFileCopier(FileCopier):
  
  def copyRemoteToLocal(self, remote_file, local_file_path):
    shutil.copy(remote_file,local_file_path)
    
  def copyLocalToRemote(self, local_file, remote_file_path):
    shutil.copy(local_file,remote_file_path)
  