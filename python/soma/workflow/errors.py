'''
@author: Soizic Laguitton

@organization: I2BM, Neurospin, Gif-sur-Yvette, France
@organization: CATI, France
@organization: U{IFR 49<http://www.ifr49.org>}

@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''

class SomaWorkflowError(Exception):
  '''
  Wrong usage of Soma-workflow.
  '''
  pass


class ConfigurationError(Exception): 
  '''
  Raised when the configuration is not correct or when configuration
  items are missing.
  '''
  pass 

class ConnectionError(Exception):   
  '''
  Raised when the connection could not be set up with the computing 
  resource and/or the database server.
  '''
  pass

class DRMError(Exception):          
  '''
  DRMAA session error or DRMS error.
  '''
  pass

class JobError(Exception):           
  '''
  Raised when an incorrect Job is submitted to soma-workflow.
  '''
  pass

class WorkflowError(Exception):      
  '''
  Raised when an incorrect Workflow is submitted to soma-workflow.
  '''
  pass

class TransferError(Exception):
  '''
  Raised when an incorrect Transfer is submitted to soma_workflow, or when an error occurs during a file transfer.
  '''
  pass

class UnknownObjectError(Exception):
  '''
  Raised when an operation is attempted on Jobs, Workflows or Transfers which
  do not exist or do not belong to the user.
  '''
  pass


class EngineError(Exception):
  '''
  Raised if an error occurs while starting the workflow engine. 
  '''
  pass


class DatabaseError(Exception):
  '''
  Raised if an error occurs while reading or writing to the database.
  '''
  pass


class SerializationError(Exception):
  pass


class NoDrmaaLibError(Exception):
  '''
  Raised if no drmaa libary is found
  '''
  pass

