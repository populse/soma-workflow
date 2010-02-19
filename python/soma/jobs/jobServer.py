'''
The class L{JobServer} is the data server used by the L{JobScheduler} to 
save all information related to submitted jobs to predefined sets of machines
linked together via a distributed resource management systems (DRMS).


@author: Yann Cointepas
@author: Soizic Laguitton
@organization: U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''

import sqlite3


__docformat__ = "epytext en"


class JobServer ( object ):
  '''
  
  
  '''
  
  def registerUser( self, login )
    '''
    Register a user so that he can submit job.
    
    @rtype: C{UserIdentifier}
    @return: user identifier
    '''
  
  def getLocalFilePath(self, user_id, remote_file_path=None):
    '''
    Generates free local file path for transfers. 
    
    @type  user_id: C{UserIdentifier}
    @param user_id: user identifier
    @type  remote_file_path: string
    @param remote_file_path: the generated name can derivate from 
    this path.
    @rtype: string
    @return: free local file path
    '''
    
  def addTransfer(local_file_path, remote_file_path, expiration_date, user_id):
    '''
    Adds a transfer to the database.
    
    @type local_file_path: string
    @type remote_file_path: string
    @type expiration_date: date
    @type user_id:  C{UserIdentifier}
    '''

  def getRemoteFile(local_file_path)
    '''
    Returns the remote file path associated to a local file path in a transfer.
    
    @type local_file_path:string
    @rtype: string
    @returns: the associated remote file path
    '''

  def removeTransferASAP(local_file_path)
    '''
    Set the expiration date of the transfer associated to the local file path 
    to today (yesterday?). That way it will be disposed as soon as no job will need it.
    
    @type  local_file_path: string
    @param local_file_path: local file path to identifying the transfer 
    record to delete.
    '''

  def isUserTransfer(local_file_path, user_id)
    '''
    Check that a local file path match with a transfer and that the transfer 
    is owned by the user.
    
    @type local_file_path: string
    @type user_id: C{UserIdentifier}
    @rtype: bool
    @returns: the local file path match with a transfer owned by the user
    '''





