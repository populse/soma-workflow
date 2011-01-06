from __future__ import with_statement 

'''
@author: Yann Cointepas
@author: Soizic Laguitton
@organization: U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''

'''
soma-workflow database server

The WorkflowDatabaseServer stores all the information related to workflows, jobs 
and file transfer in a sqlite database file. The database stores the workflows,
jobs and file transfer objects but also the status of the actions which can be
done on the objects (submission of a job or a workflow, transfering a file...). 
'''

#-------------------------------------------------------------------------------
# Imports
#-------------------------------------------------------------------------------

import sqlite3
import threading
import os
import shutil
import logging
import pickle
from datetime import date
from datetime import timedelta
from datetime import datetime

import soma.workflow.constants as constants

__docformat__ = "epytext en"

#-----------------------------------------------------------------------------
# Globals and constants
#-----------------------------------------------------------------------------

strtime_format = '%Y-%m-%d %H:%M:%S'
file_separator = ', '

#-----------------------------------------------------------------------------
# Local utilities
#-----------------------------------------------------------------------------

def adapt_datetime(ts):
    return ts.strftime(strtime_format)

sqlite3.register_adapter(datetime, adapt_datetime)


#-----------------------------------------------------------------------------
# Classes and functions
#-----------------------------------------------------------------------------


'''
Job server database tables:
  Users
    id
    login or other userId

  Jobs
    => identification:
      id      : int
      user_id : int
    
    => used by the job system (DrmaaWorkflowEngine, WorkflowDatabaseServer)
      drmaa_id           : string, None if not submitted
      expiration_date    : date
      status             : string
      last_status_update : date
      workflowId         : int, optional
      stdout_file        : file path
      stderr_file        : file path, optional
      
    => used to submit the job
      command            : string
      stdin_file         : file path, optional 
      join_errout        : boolean
      (stdout_file        : file path)
      (stderr_file        : file path, optional)
      working_directory  : dir path, optional
      custom_submission  : boolean 
      parallel_config_name : string, optional
      max_node_number      : int, optional
      queue               : string, optional

    => for user and administrator usage
      name_description   : string, optional 
      submission_date    : date 
      execution_date     : date
      ending_date        : date
      exit_status        : string, optional
      exit_value         : int, optional
      terminating_signal : string, optional
      resource_usage_file  : string, optional
     
  
  Transfer
    local file path
    remote file path (optional)
    transfer date
    expiration date
    user_id
    workflow_id (optional)
    status
    remote_paths
    transfer_action_info

  Input/Ouput junction table
    job_id 
    local file path (transferid)
    input or output
    
  Workflows
    id,
    user_id,
    pickled_workflow,
    expiration_date,
    name,
    ended_transfered, 
    status
'''

def create_database(database_file):
  connection = sqlite3.connect(database_file, timeout=5, isolation_level="EXCLUSIVE")
  cursor = connection.cursor()
  cursor.execute('''CREATE TABLE users (id    INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                                      login VARCHAR(255) NOT NULL UNIQUE)''')
  cursor.execute('''CREATE TABLE jobs (
                                       id                   INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                                       user_id              INTEGER NOT NULL CONSTRAINT known_user REFERENCES users (id),
                                       
                                       drmaa_id             VARCHAR(255),
                                       expiration_date      DATE NOT NULL,
                                       status               VARCHAR(255) NOT NULL,
                                       last_status_update   DATE NOT NULL,
                                       workflow_id          INTEGER CONSTRAINT known_workflow REFERENCES workflows (id),
                                       
                                       command              TEXT,
                                       stdin_file           TEXT,
                                       join_errout          BOOLEAN NOT NULL,
                                       stdout_file          TEXT NOT NULL,
                                       stderr_file          TEXT,
                                       working_directory    TEXT,
                                       custom_submission    BOOLEAN NOT NULL,
                                       parallel_config_name TEXT,
                                       max_node_number      INTEGER,
                                       queue                TEXT,
                                       
                                       name_description     TEXT,
                                       submission_date      DATE,
                                       execution_date       DATE,
                                       ending_date          DATE,
                                       exit_status          VARCHAR(255),
                                       exit_value           INTEGER,
                                       terminating_signal   VARCHAR(255),
                                       resource_usage       TEXT
                                       )''')

  cursor.execute('''CREATE TABLE transfers (local_file_path  TEXT PRIMARY KEY NOT NULL, 
                                            remote_file_path TEXT,
                                            transfer_date    DATE,
                                            expiration_date  DATE NOT NULL,
                                            user_id          INTEGER NOT NULL CONSTRAINT known_user REFERENCES users (id),
                                            workflow_id      INTEGER CONSTRAINT known_workflow REFERENCES workflows (id),
                                            status           VARCHAR(255) NOT NULL,
                                            remote_paths     TEXT,
                                            transfer_action_info  TEXT)''')

  cursor.execute('''CREATE TABLE ios (job_id           INTEGER NOT NULL CONSTRAINT known_job REFERENCES jobs(id),
                                      local_file_path  TEXT NOT NULL CONSTRAINT known_local_file REFERENCES transfers (local_file_path),
                                      is_input         BOOLEAN NOT NULL,
                                      PRIMARY KEY (job_id, local_file_path))''')
                                      
  cursor.execute('''CREATE TABLE fileCounter (count INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                                              foo INTEGER)''') #!!! FIND A CLEANER WAY !!!
                                              
  cursor.execute('''CREATE TABLE workflows (id               INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                                           user_id           INTEGER NOT NULL CONSTRAINT known_user REFERENCES users (id),
                                           pickled_workflow   TEXT,
                                           expiration_date    DATE NOT NULL,
                                           name               TEXT,
                                           ended_transfers    TEXT,
                                           status             TEXT,
                                           last_status_update DATE NOT NULL) ''')
  
  cursor.close()
  connection.commit()
  connection.close()

def print_job_status(database_file):
  connection = sqlite3.connect(database_file, timeout = 5, isolation_level = "EXCLUSIVE")
  cursor = connection.cursor()

  for row in cursor.execute('SELECT id, status, queue FROM jobs'):
    job_id, status, queue = row
    print "job_id: " + repr(job_id) + " status: " + repr(status) + " queue " + repr(queue)

  cursor.close()
  connection.close()

def print_tables(database_file):
  
  connection = sqlite3.connect(database_file, timeout = 5, isolation_level = "EXCLUSIVE")
  cursor = connection.cursor()
  
  print "==== users table: ========"
  for row in cursor.execute('SELECT * FROM users'):
    id, login = row
    print 'id=', repr(id).rjust(2), 'login=', repr(login).rjust(7)
    
  print "==== transfers table: ===="
  for row in cursor.execute('SELECT * FROM transfers'):
    print row
    #local_file_path, remote_file_path, transfer_date, expiration_date, user_id = row
    #print '| local_file_path', repr(local_file_path).ljust(25), '| remote_file_path=', repr(remote_file_path).ljust(25) , '| transfer_date=', repr(transfer_date).ljust(7), '| expiration_date=', repr(expiration_date).ljust(7), '| user_id=', repr(user_id).rjust(2), ' |'
  
  print "==== workflows table: ========"
  for row in cursor.execute('SELECT * FROM workflows'):
    print row
    #id, submission_date, user_id, expiration_date, stdout_file, stderr_file, join_errout, stdin_file, name_description, drmaa_id,     working_directory = row
    #print 'id=', repr(id).rjust(3), 'submission_date=', repr(submission_date).rjust(7), 'user_id=', repr(user_id).rjust(3), 'expiration_date' , repr(expiration_date).rjust(7), 'stdout_file', repr(stdout_file).rjust(10), 'stderr_file', repr(stderr_file).rjust(10), 'join_errout', repr(join_errout).rjust(5), 'stdin_file', repr(stdin_file).rjust(10), 'name_description', repr(name_description).rjust(10), 'drmaa_id', repr(drmaa_id).rjust(10), 'working_directory', repr(working_directory).rjust(10)
  
  print "==== jobs table: ========"
  for row in cursor.execute('SELECT * FROM jobs'):
    print row
  
  print "==== ios table: ========="
  for row in cursor.execute('SELECT * FROM ios'):
    job_id, local_file_path, is_input = row
    print '| job_id=', repr(job_id).rjust(2), '| local_file_path=', repr(local_file_path).ljust(25), '| is_input=', repr(is_input).rjust(2), ' |'
  
  
  #print "==== file counter table: ========="
  #for row in cursor.execute('SELECT * FROM fileCounter'):
    #count, foo = row
    #print '| count=', repr(count).rjust(2), '| foo=', repr(foo).ljust(2), ' |'
  
  cursor.close()
  connection.close()



class DBJob(object):
  '''
  Job as defined in the DB.
  '''
  
  def __init__(self,
              user_id,
    
              expiration_date,
              join_errout,
              stdout_file,
              custom_submission,
    
              drmaa_id = None,
              status = constants.NOT_SUBMITTED,
              last_status_update=None,
              workflow_id=None,
              
              command=None,
              stdin_file=None,
              stderr_file=None,
              working_directory=None,
              parallel_config_name=None,
              max_node_number=1,
              queue=None,
              
              name_description=None,
              submission_date=None,
              execution_date=None,
              ending_date=None,
              exit_status=None,
              exit_value=None,
              terminating_signal=None,
              resource_usage=None):
              
    '''
    @type user_id: C{UserIdentifier}
    @type expiration_date: date
    @type  drmaa_id: string or None
    @param drmaa_id: submitted job DRMAA identifier
    @type  status: string
    @param status: job status as defined in constants.JOB_STATUS
    @type  last_status_update: date
    @param last_status_update: last status update date
    @type  workflow_id: int
    @param workflow_id: id of the workflow the job belongs to. 
                        None if it doesn't belong to any.
                        
    @type  command: string
    @param command: job command
    @type  stdin_file: string
    @param stdin_file: job's standard input as a path to a file. 
                       C{None} if the job doesn't require an input stream.
    @type  join_stderrout: bool
    @param join_stderrout: C{True} if the standard error should be 
                           redirect in the same file as the standard output.
    @type  stdout_file: string
    @param stdout_file: job's standard output as a path to a file
    @type  stderr_file: string
    @param stderr_file: job's standard output as a path to a file
    @type   working_directory: string
    @param  working_directory: path of the job working directory.
    @type custom_submission: Boolean
    @type custom_submission: C{True} if it was a custom submission. 
                             If C{True} the standard output files won't 
                             be deleted with the job.
    @type  parallel_config_name: None or string 
    @param parallel_config_name: if the job is made to run on several nodes: 
                                 name of the paralle configuration as defined 
                                 in WorkflowDatabaseServer.
    @type  max_node_number: int 
    @param max_node_number: maximum of node requested by the job to run
    @type  queue: str or None
    @param queue: name of the queue used to submit the job.
    
    @type  name_description: string
    @param name_description: optional description of the job.  
    @type  exit_status: string 
    @param exit_status: exit status string as defined in L{WorkflowDatabaseServer}
    @type  exit_value: int or None
    @param exit_value: if the status is FINISHED_REGULARLY, it contains the operating 
    system exit code of the job.
    @type  terminating_signal: string or None
    @param terminating_signal: if the status is FINISHED_TERM_SIG, it contain a representation 
    of the signal that caused the termination of the job.
    @type  resource_usage: string 
    @param resource_usage: contain the resource usage information of
    the job.
    
    @rtype: C{JobIdentifier}
    @return: the identifier of the job
    '''
              
              
    self.user_id = user_id
    
    self.drmaa_id = drmaa_id
    self.expiration_date = expiration_date
    self.status = status
    self.last_status_update = last_status_update
    self.workflow_id = workflow_id
    
    self.command = command
    self.stdin_file = stdin_file
    self.join_errout = join_errout
    self.stdout_file = stdout_file
    self.stderr_file = stderr_file
    self.working_directory = working_directory
    self.custom_submission = custom_submission
    self.parallel_config_name = parallel_config_name
    self.max_node_number = max_node_number
    self.queue = queue
    
    self.name_description = name_description
    self.submission_date = submission_date
    self.execution_date = execution_date
    self.ending_date = ending_date
    
    self.exit_status = exit_status
    self.exit_value = exit_value
    self.terminating_signal = terminating_signal
    self.resource_usage = resource_usage



class WorkflowDatabaseServerError( Exception ):
  def __init__(self, msg, logger=None):
    self.args = (msg,)
    if logger: 
      logger.critical('EXCEPTION ' + msg)


class WorkflowDatabaseServer( object ):
  
  def __init__(self, database_file, tmp_file_dir_path):
    '''
    The constructor gets as parameter the database information.
    
    @type  database_file: string
    @param database_file: the SQLite database file 
    @type  tmp_file_dir_path: string
    @param tmp_file_dir_path: place on the resource file system where
    the files will be transfered
    '''
        
    self._tmp_file_dir_path = tmp_file_dir_path
    self._database_file = database_file
     
    self._lock = threading.RLock()
   
    self.logger = logging.getLogger('jobServer')
    
    with self._lock:
      if not os.path.isfile(database_file):
        print "Database creation " + database_file
        self.logger.info("Database creation " + database_file)
        create_database(database_file)
      
      
      
  def __del__(self):
   pass 
    
    
  def _connect(self):
    try:
      connection = sqlite3.connect(self._database_file, timeout = 10, isolation_level = "EXCLUSIVE")
    except Exception, e:
        raise WorkflowDatabaseServerError('Database connection error %s: %s \n' %(type(e), e), self.logger) 
    return connection
  
  def _user_transfer_dir_path(self, login, user_id):
    path = os.path.join(self._tmp_file_dir_path,login+"_"+repr(user_id))
    return path# supposes simple logins. Or use only the user id ? 
  
  
  def register_user(self, login):
    '''
    Register a user so that he can submit job.
    
    @rtype: C{UserIdentifier}
    @return: user identifier
    '''
    with self._lock:
      connection = self._connect()
      cursor = connection.cursor()
      try:
        count = cursor.execute('SELECT count(*) FROM users WHERE login=?', [login]).next()[0]
        if count==0:
          cursor.execute('INSERT INTO users (login) VALUES (?)', [login])          
        user_id = cursor.execute('SELECT id FROM users WHERE login=?', [login]).next()[0]
      except Exception, e:
        connection.rollback()
        cursor.close()
        connection.close()
        raise WorkflowDatabaseServerError('Error register_user %s: %s \n' %(type(e), e), self.logger) 
      connection.commit()
      cursor.close()
      connection.close()
      
      personal_path = self._user_transfer_dir_path(login, user_id)
      if not os.path.isdir(personal_path):
        os.mkdir(personal_path)
        os.chmod(personal_path, 0775)
      
      return user_id
  
  
  def clean(self) :
    '''
    Delete all expired jobs, transfers and workflows, except transfers which are requested 
    by valid job.
    '''
    self.remove_non_registered_files()
    with self._lock:
      connection = self._connect()
      cursor = connection.cursor()
        
      try:
        #########################################################################
        # Jobs and associated files (std out, std err and ressouce usage file)
        jobsToDelete = []
        for row in cursor.execute('SELECT id FROM jobs WHERE expiration_date < ?', [date.today()]):
          jobsToDelete.append(row[0])
        
        for job_id in jobsToDelete:
          cursor.execute('DELETE FROM ios WHERE job_id=?', [job_id])
          stdof, stdef, rusage, custom = cursor.execute('''
                                                          SELECT 
                                                          stdout_file, 
                                                          stderr_file, 
                                                          resource_usage,
                                                          custom_submission
                                                          FROM jobs 
                                                          WHERE id=?''', 
                                                          [job_id]).next()
          if not custom:
            self.__removeFile(self._string_conversion(stdof))
            self.__removeFile(self._string_conversion(stdef))
        
        cursor.execute('DELETE FROM jobs WHERE expiration_date < ?', [date.today()])
        
        #########################################################################
        # Transfers 
        
        # get back the expired transfers
        expiredTransfers = []
        for row in cursor.execute('SELECT local_file_path FROM transfers WHERE expiration_date < ?', [date.today()]):
          expiredTransfers.append(row[0])
        
        # check that they are not currently used (as an input of output of a job)
        transfersToDelete = []
        for local_file_path in expiredTransfers:
          count = cursor.execute('SELECT count(*) FROM ios WHERE local_file_path=?', [local_file_path]).next()[0]
          if count == 0 :
            transfersToDelete.append(local_file_path)
      
        # delete transfers data and associated local file
        for local_file_path in transfersToDelete:
          cursor.execute('DELETE FROM transfers WHERE local_file_path=?', [local_file_path])
          self.__removeFile(local_file_path)
        
        ############################################################################
        # Workflows
          
        cursor.execute('DELETE FROM workflows WHERE expiration_date < ?', [date.today()])
        
        
      except Exception, e:
        connection.rollback()
        cursor.close()
        connection.close()
        raise WorkflowDatabaseServerError('Error clean %s: %s \n' %(type(e), e), self.logger) 
      
      cursor.close()
      connection.commit()
      connection.close()
     
  def remove_non_registered_files(self):
    registered_local_paths = []
    registered_users = []
    with self._lock:
      connection = self._connect()
      cursor = connection.cursor()
      try:
        for row in cursor.execute('SELECT local_file_path FROM transfers'):
          local_path = row[0]
          registered_local_paths.append(self._string_conversion(local_path))
        for row in cursor.execute('SELECT stdout_file FROM jobs'):
          stdout_file = row[0]
          if stdout_file:   
            registered_local_paths.append(self._string_conversion(stdout_file))
        for row in cursor.execute('SELECT stderr_file FROM jobs'):
          stderr_file = row[0]
          if stderr_file:
            registered_local_paths.append(self._string_conversion(stderr_file))
        for row in cursor.execute('SELECT id, login FROM users'):
          user_id, login = row
          registered_users.append((user_id, login))
      except Exception, e:
        cursor.close()
        connection.close()
        raise WorkflowDatabaseServerError('Error remove_non_registered_files %s: %s \n' %(type(e), e), self.logger) 
      cursor.close()
      connection.close()
      
    
    for user_info in registered_users:
      user_id, login = user_info
      directory_path = self._user_transfer_dir_path(login, user_id) 
      for name in os.listdir(directory_path):
        local_path = os.path.join(directory_path,name)
        if not local_path in registered_local_paths:
          self.logger.debug("remove_non_registered_files, not registered " + local_path + " to delete!")
          self.__removeFile(local_path)
  
     
     
  def generate_local_file_path(self, user_id, remote_file_path=None):
    '''
    Generates free local file path for transfers. 
    The user_id must be valid.
    
    @type  user_id: C{UserIdentifier}
    @param user_id: user identifier
    @type  remote_file_path: string
    @param remote_file_path: the generated name can derivate from 
    this path.
    @rtype: string
    @return: free local file path
    '''
    with self._lock:
      connection = self._connect()
      cursor = connection.cursor()
      try:
        login = cursor.execute('SELECT login FROM users WHERE id=?',  [user_id]).next()[0]#supposes that the user_id is valid
        login = self._string_conversion(login)
        cursor.execute('INSERT INTO fileCounter (foo) VALUES (?)', [0])
        file_num = cursor.lastrowid
      except Exception, e:
        connection.rollback()
        cursor.close()
        connection.close()
        raise WorkflowDatabaseServerError('Error generate_local_file_path %s: %s \n' %(type(e), e), self.logger) 
      
      userDirPath = self._user_transfer_dir_path(login, user_id) 
      if remote_file_path == None:
        newFilePath = os.path.join(userDirPath, repr(file_num))
        #newFilePath += repr(file_num)
      else:
        remote_base_name  = os.path.basename(remote_file_path)
        iextention = remote_base_name.find(".")
        if iextention == -1 :
          newFilePath = os.path.join(userDirPath, remote_base_name + '_' + repr(file_num))
          #newFilePath += remote_file_path[remote_file_path.rfind("/")+1:] + '_' + repr(file_num) 
        else: 
          newFilePath = os.path.join(userDirPath, remote_base_name[0:iextention] + '_' + repr(file_num) + remote_base_name[iextention:])
          #newFilePath += remote_file_path[remote_file_path.rfind("/")+1:iextention] + '_' + repr(file_num) + remote_file_path[iextention:]
      cursor.close()
      connection.commit()
      connection.close()
      return newFilePath
  
  
  def __removeFile(self, file_path):
    if file_path and os.path.isdir(file_path):
      try :
        shutil.rmtree(file_path)
      except Exception, e:
        self.logger.debug("Could not remove file %s, error %s: %s \n" %(file_path, type(e), e))  
        
    elif file_path and os.path.isfile(file_path):
      try:
        os.remove(file_path)
      except Exception, e:
        self.logger.debug("Could not remove file %s, error %s: %s \n" %(file_path, type(e), e))
    
    
  #####################################"
  # TRANSFERS 
  
  def add_transfer(self, local_file_path, remote_file_path, expiration_date, user_id, status = constants.READY_TO_TRANSFER, workflow_id = -1, remote_paths = None):
    '''
    Adds a transfer to the database.
    
    @type local_file_path: string
    @type  remote_file_path: string or None
    @param remote_file_path: C{None} for job standard output or error only.
    @type expiration_date: date
    @type user_id:  C{UserIdentifier}
    @type  status: string as defined in constants.FILE_TRANSFER_STATUS
    @param status: job transfer status (used when the transfer belong to a workflow)
    @type  workflow_id: C{WorkflowIdentifier}
    @param workflow_id: None or identifier of the workflow the transfer belongs to
    @type  remote_paths: sequence of string or None
    @param remote_paths: sequence of remote file or directory if transfering a 
    file serie or if the file format involve serveral files of directories.
    '''
    if remote_paths:
      remote_path_std = file_separator.join(remote_paths)
    else:
      remote_path_std = None
     
    with self._lock:
      connection = self._connect()
      cursor = connection.cursor()
      try:
        cursor.execute('''INSERT INTO transfers 
                        (local_file_path, remote_file_path, transfer_date, expiration_date, user_id, workflow_id, status, remote_paths) VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                        (local_file_path, remote_file_path, date.today(), expiration_date, user_id, workflow_id, status, remote_path_std))
      except Exception, e:
        connection.rollback()
        cursor.close()
        connection.close()
        raise WorkflowDatabaseServerError('Error add_transfer %s: %s \n' %(type(e), e), self.logger) 
      cursor.close()
      connection.commit()
      connection.close()


  def remove_transfer(self, local_file_path):
    '''
    Set the expiration date of the transfer associated to the local file path 
    to today (yesterday?). That way it will be disposed as soon as no job will need it.
    
    @type  local_file_path: string
    @param local_file_path: local file path to identifying the transfer 
    record to delete.
    '''
    with self._lock:
      connection = self._connect()
      cursor = connection.cursor()
      yesterday = date.today() - timedelta(days=1)
      try:
        cursor.execute('UPDATE transfers SET expiration_date=? WHERE local_file_path=?', (yesterday, local_file_path))
      except Exception, e:
        connection.rollback()
        cursor.close()
        connection.close()
        raise WorkflowDatabaseServerError('Error remove_transfer %s: %s \n' %(type(e), e), self.logger) 
      connection.commit()
      cursor.close()
      connection.close()
      self.clean()
        
  def get_transfer_information(self, local_file_path):
    '''
    Returns the information related to the transfer associated to the local file path.
    The local_file_path must be associated to a transfer.
    Returns (None, None, None, -1, None) if the local_file_path is not associated to a transfer.
    
    @type local_file_path: string
    @rtype: tuple
    @returns: (local_file_path, remote_file_path, expiration_date, workflow_id, remote_paths)
    '''
    with self._lock:
      connection = self._connect()
      cursor = connection.cursor()
      try:
        count = cursor.execute('SELECT count(*) FROM transfers WHERE local_file_path=?', [local_file_path]).next()[0]
        if not count == 0 :
          info = cursor.execute('''SELECT 
                                   local_file_path, 
                                   remote_file_path, 
                                   expiration_date, 
                                   workflow_id,
                                   remote_paths 
                                   FROM transfers 
                                   WHERE local_file_path=?''', 
                                   [local_file_path]).next() #supposes that the local_file_path is associated to a transfer
        else: 
          info = None
      except Exception, e:
        cursor.close()
        connection.close()
        raise WorkflowDatabaseServerError('Error get_transfer_information %s: %s \n' %(type(e), e), self.logger) 
      if info:
        if info[4]:
          remote_paths = self._string_conversion(info[4]).split(file_separator)
        else: 
          remote_paths = None
        result = (self._string_conversion(info[0]), self._string_conversion(info[1]), self._str_to_date_conversion(info[2]), info[3], remote_paths)
      else:
        result = (None, None, None, -1, None)
      cursor.close()
      connection.close()
    return result
  
  def get_transfer_status(self, local_file_path):
    '''
    Returns the transferstatus sored in the database.
    Returns None if the local_file_path is not associated to a transfer.
    '''
    with self._lock:
      connection = self._connect()
      cursor = connection.cursor()
      try:
        count = cursor.execute('SELECT count(*) FROM transfers WHERE local_file_path=?', [local_file_path]).next()[0]
        if not count == 0 :
          status = cursor.execute('SELECT status FROM transfers WHERE local_file_path=?', [local_file_path]).next()[0]#supposes that the local_file_path is associated to a transfer
        else:
          status = None
      except Exception, e:
        cursor.close()
        connection.close()
        raise WorkflowDatabaseServerError('Error get_transfer_status %s: %s \n' %(type(e), e), self.logger) 
      if status:
        status = self._string_conversion(status)
      cursor.close()
      connection.close()
 
    return status
      
  
  def set_transfer_status(self, local_file_path, status):
    '''
    Updates the transfer status in the database.
    The status must be valid (ie a string among the transfer status 
    string defined in constants.FILE_TRANSFER_STATUS
    
    @type  status: string
    @param status: transfer status as defined in constants.FILE_TRANSFER_STATUS
    '''
    with self._lock:
      # TBI if the status is not valid raise an exception ??
      connection = self._connect()
      cursor = connection.cursor()
      try:
        count = cursor.execute('SELECT count(*) FROM transfers WHERE local_file_path=?', [local_file_path]).next()[0]
        if not count == 0 :
          cursor.execute('UPDATE transfers SET status=? WHERE local_file_path=?', (status, local_file_path))
      except Exception, e:
        connection.rollback()
        cursor.close()
        connection.close()
        raise WorkflowDatabaseServerError('Error set_transfer_status %s: %s \n' %(type(e), e), self.logger) 
      connection.commit()
      cursor.close()
      connection.close()

  def set_transfer_action_info(self, local_file_path, info):
    '''
    Save data necessary for the transfer itself.
    In the case of a file transfer, info is a tuple (file size, md5 hash)
    In the case of a directory transfer, info is a tuple (cumulated file size, file transfer info)
    '''
    with self._lock:
      connection = self._connect()
      cursor = connection.cursor()
      try:
        count = cursor.execute('SELECT count(*) FROM transfers WHERE local_file_path=?', [local_file_path]).next()[0]
        if not count == 0 :
          pickled_info = pickle.dumps(info)
          cursor.execute('UPDATE transfers SET transfer_action_info=? WHERE local_file_path=?', (pickled_info, local_file_path))
      except Exception, e:
        connection.rollback()
        cursor.close()
        connection.close()
        raise WorkflowDatabaseServerError('Error set_transfer_action_info %s: %s \n' %(type(e), e), self.logger) 
      connection.commit()
      cursor.close()
      connection.close()
      
      
  def get_transfer_action_info(self, local_file_path):
    '''
    Returns data necessary to the transfer itself.
    '''
    with self._lock:
      connection = self._connect()
      cursor = connection.cursor()
      try:
        count = cursor.execute('SELECT count(*) FROM transfers WHERE local_file_path=?', [local_file_path]).next()[0]
        if not count == 0 :
          pickled_info = cursor.execute('SELECT transfer_action_info FROM transfers WHERE local_file_path=?', [local_file_path]).next()[0]#supposes that the local_file_path is associated to a transfer
        else:
          pickled_info = None
      except Exception, e:
        cursor.close()
        connection.close()
        raise WorkflowDatabaseServerError('Error get_transfer_action_info %s: %s \n' %(type(e), e), self.logger) 
      
      pickled_info = self._string_conversion(pickled_info)
      if pickled_info:
        info = pickle.loads(pickled_info)
      else:
        info = None
      cursor.close()
      connection.close()
 
    return info

  
  def add_workflow_ended_transfer(self, workflow_id, local_file_path):
    '''
    To signal that a transfer belonging to a workflow finished.
    '''
    separator = ", "
    with self._lock:
      connection = self._connect()
      cursor = connection.cursor()
      try:
        count = cursor.execute('SELECT count(*) FROM workflows WHERE id=?', [workflow_id]).next()[0]
        if not count == 0 :
          str_ended_transfers = cursor.execute('SELECT ended_transfers FROM workflows WHERE id=?', [workflow_id]).next()[0] 
          if str_ended_transfers != None:
            ended_transfers = self._string_conversion(str_ended_transfers).split(separator)
            ended_transfers.append(local_file_path)
            str_ended_transfers = separator.join(ended_transfers)
          else:
            str_ended_transfers = local_file_path
          cursor.execute('UPDATE workflows SET ended_transfers=? WHERE id=?', (str_ended_transfers, workflow_id))
      except Exception, e:
        connection.rollback()
        cursor.close()
        connection.close()
        raise WorkflowDatabaseServerError('Error add_workflow_ended_transfer %s: %s \n' %(type(e), e), self.logger) 
      connection.commit()
      cursor.close()
      connection.close()
    

  def pop_workflow_ended_transfer(self, workflow_id):
    '''
    Returns the ended transfers for a workflow and clear the ended transfer list.
    '''
    separator = ", "
    ended_transfers = []
    with self._lock:
      connection = self._connect()
      cursor = connection.cursor()
      try:
        count = cursor.execute('SELECT count(*) FROM workflows WHERE id=?', [workflow_id]).next()[0]
        if not count == 0 :
          str_ended_transfers = cursor.execute('SELECT ended_transfers FROM workflows WHERE id=?', [workflow_id]).next()[0] 
          if str_ended_transfers != None:
            ended_transfers = self._string_conversion(str_ended_transfers).split(separator)
          cursor.execute('UPDATE workflows SET ended_transfers=? WHERE id=?', (None, workflow_id))
      except Exception, e:
        connection.rollback()
        cursor.close()
        connection.close()
        raise WorkflowDatabaseServerError('Error set_transfer_status %s: %s \n' %(type(e), e), self.logger) 
      connection.commit()
      cursor.close()
      connection.close()
    return ended_transfers

  ###############################################
  # WORKFLOWS
  
  def add_workflow(self, user_id, expiration_date, name = None):
    '''
    Adds a job to the database and returns its identifier.
    
    @type user_id: C{UserIdentifier}
    @type expiration_date: date
    
    @rtype: C{WorkflowIdentifier}
    @return: the identifier of the workflow
    '''
    with self._lock:
      connection = self._connect()
      cursor = connection.cursor()
      try:
        cursor.execute('''INSERT INTO workflows 
                         (user_id,
                          pickled_workflow,
                          expiration_date,
                          name,
                          status, 
                          last_status_update)
                          VALUES (?, ?, ?, ?, ?, ?)''',
                         (user_id,
                          None, 
                          expiration_date,
                          name,
                          constants.WORKFLOW_NOT_STARTED,
                          datetime.now()))
      except Exception, e:
        connection.rollback()
        cursor.close()
        connection.close()
        raise WorkflowDatabaseServerError('Error add_workflow %s: %s \n' %(type(e), e), self.logger) 
      connection.commit()
      wf_id = cursor.lastrowid
      cursor.close()
      connection.close()

    return wf_id
  
  def delete_workflow(self, wf_id):
    '''
    Remove the workflow from the database. Remove all associated jobs and transfers.
    
    @type wf_id: C{WorkflowIdentifier}
    '''
    with self._lock:
      # set expiration date to yesterday + clean() ?
      connection = self._connect()
      cursor = connection.cursor()
      
      yesterday = date.today() - timedelta(days=1)
    
      try:
        cursor.execute('UPDATE workflows SET expiration_date=? WHERE id=?', (yesterday, wf_id))
        cursor.execute('UPDATE jobs SET expiration_date=? WHERE workflow_id=?', (yesterday, wf_id))
        cursor.execute('UPDATE transfers SET expiration_date=? WHERE workflow_id=?', (yesterday, wf_id))
      except Exception, e:
        connection.rollback()
        cursor.close()
        connection.close()
        raise WorkflowDatabaseServerError('Error delete_job %s: %s \n' %(type(e), e), self.logger) 
        
      cursor.close()
      connection.commit()
      connection.close()
      self.clean()

  def set_workflow(self, wf_id, workflow, user_id):
    '''
    Saves the workflow in a file and register the file path in the database.
    
    @type user_id: C{WorkflowIdentifier}
    @type  workflow: L{Workflow}
    '''
    pickled_workflow = pickle.dumps(workflow)
    with self._lock:
      connection = self._connect()
      cursor = connection.cursor()
      try:
        cursor.execute('UPDATE workflows SET pickled_workflow=? WHERE id=?', (pickled_workflow, wf_id))
      except Exception, e:
        connection.rollback()
        cursor.close()
        connection.close()
        raise WorkflowDatabaseServerError('Error set_workflow %s: %s \n' %(type(e), e), self.logger) 
      connection.commit()
      cursor.close()
      connection.close()
      
  def change_workflow_expiration_date(self, wf_id, new_date):
    '''
    Change the workflow expiration date.
    
    @type wf_id: C{WorflowIdentifier}
    @type new_date: datetime.datetime
    '''
    with self._lock:
      connection = self._connect()
      cursor = connection.cursor()
      try:
        cursor.execute('UPDATE workflows SET expiration_date=? WHERE id=?', (new_date, wf_id))
      except Exception, e:
        connection.rollback()
        cursor.close()
        connection.close()
        raise WorkflowDatabaseServerError('Error change_workflow_expiration_date %s: %s \n' %(type(e), e), self.logger) 
      connection.commit()
      cursor.close()
      connection.close()
      
  def get_workflow(self, wf_id):
    '''
    Returns the workflow object.
    The wf_id must be valid.
    
    @type wf_id: C{WorflowIdentifier}
    @rtype: C{Workflow}
    @return: workflow object
    '''
    with self._lock:
      connection = self._connect()
      cursor = connection.cursor()
      try:
        pickled_workflow = cursor.execute('''SELECT  
                                              pickled_workflow
                                              FROM workflows WHERE id=?''', [wf_id]).next()[0]#supposes that the wf_id is valid
      except Exception, e:
        cursor.close()
        connection.close()
        raise WorkflowDatabaseServerError('Error get_workflow %s: %s \n' %(type(e), e), self.logger) 
      cursor.close()
      connection.close()
      
    pickled_workflow = self._string_conversion(pickled_workflow)
    if pickled_workflow:
      workflow = pickle.loads(pickled_workflow)
    else:
      workflow = None
 
    return workflow
  
  def get_workflow_info(self, wf_id):
    '''
    Returns a tuple with the workflow expiration date and name
    The wf_id must be valid.
    
    @type wf_id: C{WorflowIdentifier}
    @rtype: (date, string)
    @return: (expiration_date, name)
    '''
    with self._lock:
      connection = self._connect()
      cursor = connection.cursor()
      try:
        expiration_date, name = cursor.execute('''SELECT  
                                                  expiration_date,
                                                  name
                                                  FROM workflows WHERE id=?''', [wf_id]).next()#supposes that the wf_id is valid
      except Exception, e:
        cursor.close()
        connection.close()
        raise WorkflowDatabaseServerError('Error get_workflow_info %s: %s \n' %(type(e), e), self.logger) 
      cursor.close()
      connection.close()
      
    expiration_date = self._str_to_date_conversion(expiration_date)
    name = self._string_conversion(name)
    return (expiration_date, name)
  
  
  def set_workflow_status(self, wf_id, status, force = False):
    '''
    Updates the workflow status in the database.
    The status must be valid (ie a string among the workflow status 
    string defined in constants.WORKFLOW_STATUS)
    
    @type  status: string
    @param status: workflow status as defined in constants.WORKFLOW_STATUS
    '''
    with self._lock:
      # TBI if the status is not valid raise an exception ??
      connection = self._connect()
      cursor = connection.cursor()
      try:
        count = cursor.execute('''SELECT count(*) 
                                  FROM workflows 
                                  WHERE id=?''', [wf_id]).next()[0]
        if not count == 0 :
          prev_status = cursor.execute('''SELECT status 
                                          FROM workflows WHERE id=?''',
                                          [wf_id]).next()[0]
          prev_status = self._string_conversion(prev_status)
          if force or \
             (prev_status != constants.DELETE_PENDING and \
              prev_status != constants.KILL_PENDING):
            cursor.execute('''UPDATE workflows 
                            SET status=?, 
                            last_status_update=? 
                            WHERE id=?''', 
                            (status, 
                            datetime.now(), 
                            wf_id))
      except Exception, e:
        connection.rollback()
        cursor.close()
        connection.close()
        raise WorkflowDatabaseServerError('Error set_workflow_status %s: %s \n' %(type(e), e), 
                              self.logger) 
      connection.commit()
      cursor.close()
      connection.close()
    
  def get_workflow_status(self, wf_id):
    '''
    Returns the workflow status stored in the database 
    (updated by L{DrmaaWorkflowEngine}) and the date of its last update.
    Returns (None, None) if the wf_id is not valid.
    '''
    with self._lock:
      connection = self._connect()
      cursor = connection.cursor()
      try:
        count = cursor.execute('''SELECT count(*) 
                                  FROM workflows 
                                  WHERE id=?''', [wf_id]).next()[0]
        if not count == 0 :
          (status, strdate) = cursor.execute('''SELECT status, 
                                                      last_status_update 
                                                FROM workflows WHERE id=?''', 
                                                [wf_id]).next()
        else:
          (status, strdate) = (None, None)
      except Exception, e:
        cursor.close()
        connection.close()
        raise WorkflowDatabaseServerError('Error get_workflow_status %s: %s \n' %(type(e), e), 
                              self.logger) 
      status = self._string_conversion(status)
      date = self._str_to_date_conversion(strdate)
      cursor.close()
      connection.close()
    return (status, date)
    
  
  
  def get_detailed_workflow_status(self, wf_id):
    '''
    Gets back the status of all the workflow elements at once, minimizing the
    requests to the database.
    
    @type wf_id: C{WorflowIdentifier}
    @rtype: tuple (sequence of tuple (job_id, status, 
                                      exit_info, 
                                      (submission_date, 
                                       execution_date, 
                                       ending_date)), 
                   sequence of tuple (transfer_id, status), 
                   workflow_status)
    '''
    with self._lock:
      connection = self._connect()
      cursor = connection.cursor()
    
      try:
        # workflow status 
        wf_status = cursor.execute('''SELECT  
                                      status
                                      FROM workflows WHERE id=?''',
                                      [wf_id]).next()[0]#supposes that the wf_id is valid
                                      
        workflow_status = ([],[], wf_status)
        # jobs
        for row in cursor.execute('''SELECT id,
                                            status,
                                            exit_status,
                                            exit_value,
                                            terminating_signal,
                                            resource_usage,
                                            submission_date,
                                            execution_date,
                                            ending_date
                                     FROM jobs WHERE workflow_id=?''', [wf_id]):
          job_id, status, exit_status, exit_value, term_signal, resource_usage, submission_date, execution_date, ending_date = row
          
          submission_date = self._str_to_date_conversion(submission_date)
          execution_date = self._str_to_date_conversion(execution_date)
          ending_date = self._str_to_date_conversion(ending_date)
          
          
          workflow_status[0].append((job_id, status, (exit_status, exit_value, term_signal, resource_usage), (submission_date, execution_date, ending_date)))
                          
        # transfers 
        for row in cursor.execute('''SELECT local_file_path, 
                                            remote_file_path,
                                            status,
                                            transfer_action_info
                                     FROM transfers WHERE workflow_id=?''', [wf_id]):
          local_file_path, remote_file_path, status, pickled_info = row
            
          local_file_path = self._string_conversion(local_file_path)
          remote_file_path = self._string_conversion(remote_file_path)
          status = self._string_conversion(status)
          
          pickled_info = self._string_conversion(pickled_info)
          if pickled_info:
            transfer_action_info = pickle.loads(pickled_info)
          else:
            transfer_action_info = None
            
          workflow_status[1].append((local_file_path, remote_file_path, status, transfer_action_info))
          
      except Exception, e:
        cursor.close()
        connection.close()
        raise WorkflowDatabaseServerError('Error get_workflow_status %s: %s \n' %(type(e), e), self.logger) 
      cursor.close()
      connection.close()
      
    return workflow_status
    
      
  ###########################################
  # JOBS 
  
  def add_job( self, dbJob):
    '''
    Adds a job to the database and returns its identifier.
    
    @type  dbJob: L{DBJob}
    @param dbJob: Job information.
    
    @rtype: C{JobIdentifier}
    @return: the identifier of the job
    '''
    with self._lock:
      connection = self._connect()
      cursor = connection.cursor()
      try:
        cursor.execute('''INSERT INTO jobs 
                         (user_id,
                
                          drmaa_id,
                          expiration_date,
                          status,
                          last_status_update,
                          workflow_id,
                                       
                          command,
                          stdin_file,
                          join_errout,
                          stdout_file,
                          stderr_file,
                          working_directory,
                          custom_submission,
                          parallel_config_name,
                          max_node_number,
                          queue,
                                       
                          name_description,
                          submission_date,
                          execution_date,
                          ending_date,
                          
                          exit_status,
                          exit_value,
                          terminating_signal,
                          resource_usage)
                          VALUES (?, ?, ?, ?, ?,
                                  ?, ?, ?, ?, ?, 
                                  ?, ?, ?, ?, ?, 
                                  ?, ?, ?, ?, ?,
                                  ?, ?, ?, ?)''',
                         (dbJob.user_id,
                        
                          dbJob.drmaa_id, 
                          dbJob.expiration_date, 
                          dbJob.status,
                          datetime.now(), #last_status_update
                          dbJob.workflow_id,
                        
                          dbJob.command,
                          dbJob.stdin_file,
                          dbJob.join_errout,
                          dbJob.stdout_file,
                          dbJob.stderr_file,
                          dbJob.working_directory,
                          dbJob.custom_submission,
                          dbJob.parallel_config_name,
                          dbJob.max_node_number,
                          dbJob.queue,
                          
                          dbJob.name_description,
                          dbJob.submission_date,
                          dbJob.execution_date,
                          dbJob.ending_date,
                          dbJob.exit_status,
                          dbJob.exit_value,
                          dbJob.terminating_signal,
                          dbJob.resource_usage
                          ))
      except Exception, e:
        connection.rollback()
        cursor.close()
        connection.close()
        raise WorkflowDatabaseServerError('Error add_job %s: %s \n' %(type(e), e), self.logger) 
      connection.commit()
      job_id = cursor.lastrowid
      cursor.close()
      connection.close()

    return job_id
   
   
  def delete_job(self, job_id):
    '''
    Remove the job from the database. Remove all associated transfered files if
    their expiration date passed and they are not used by any other job.
    
    @type job_id: 
    '''
    with self._lock:
      # set expiration date to yesterday + clean() ?
      connection = self._connect()
      cursor = connection.cursor()
      
      yesterday = date.today() - timedelta(days=1)
    
      try:
        cursor.execute('UPDATE jobs SET expiration_date=? WHERE id=?', (yesterday, job_id))
          
      except Exception, e:
        connection.rollback()
        cursor.close()
        connection.close()
        raise WorkflowDatabaseServerError('Error delete_job %s: %s \n' %(type(e), e), self.logger) 
        
      cursor.close()
      connection.commit()
      connection.close()
      self.clean()



  def set_job_status(self, job_id, status, force = False):
    '''
    Updates the job status in the database.
    The status must be valid (ie a string among the job status 
    string defined in constants.JOB_STATUS
    
    @type  status: string
    @param status: job status as defined in constants.JOB_STATUS
    '''
    with self._lock:
      # TBI if the status is not valid raise an exception ??
      connection = self._connect()
      cursor = connection.cursor()
      try:
        count = cursor.execute('SELECT count(*) FROM jobs WHERE id=?', [job_id]).next()[0]
        if not count == 0 :
          (previous_status, 
           execution_date, 
           ending_date) = cursor.execute(''' SELECT status, 
                                                    execution_date, 
                                                    ending_date 
                                             FROM jobs WHERE id=?''',
                                             [job_id]).next()#supposes that the job_id is valid
          previous_status = self._string_conversion(previous_status)
          execution_date = self._str_to_date_conversion(execution_date)
          ending_date = self._str_to_date_conversion(ending_date)
          if previous_status != status:
            if not execution_date and status == constants.RUNNING:
              execution_date = datetime.now()
            if not ending_date and status == constants.DONE or \
               status == constants.FAILED:
              ending_date = datetime.now()
              if not execution_date :
                execution_date = datetime.now()
          if force or \
             (previous_status != constants.DELETE_PENDING and \
              previous_status != constants.KILL_PENDING):
            cursor.execute('''UPDATE jobs SET status=?, 
                                              last_status_update=?,
                                              execution_date=?, 
                                              ending_date=? WHERE id=?''',
                                              (status, datetime.now(),
                                               execution_date, ending_date,
                                               job_id))
      except Exception, e:
        connection.rollback()
        cursor.close()
        connection.close()
        raise WorkflowDatabaseServerError("Error set_job_status " 
                                           "%s: %s \n" %(type(e), e),
                                           self.logger) 
      connection.commit()
      cursor.close()
      connection.close()
      
  def get_job_status(self, job_id):
    '''
    Returns the job status stored in the database (updated by L{DrmaaWorkflowEngine}) and 
    the date of its last update.
    Returns (None, None) if the job_id is not valid.
    '''
    with self._lock:
      connection = self._connect()
      cursor = connection.cursor()
      try:
        count = cursor.execute('SELECT count(*) FROM jobs WHERE id=?', [job_id]).next()[0]
        if not count == 0 :
          (status, strdate) = cursor.execute('SELECT status, last_status_update FROM jobs WHERE id=?', [job_id]).next()#supposes that the job_id is valid
        else:
          (status, strdate) = (None, None)
      except Exception, e:
        cursor.close()
        connection.close()
        raise WorkflowDatabaseServerError('Error get_job_status %s: %s \n' %(type(e), e), self.logger) 
      status = self._string_conversion(status)
      date = self._str_to_date_conversion(strdate)
      cursor.close()
      connection.close()
 
    return (status, date)

  def set_submission_information(self, job_id, drmaa_id, submission_date):
    '''
    Set the submission information of the job and reset information 
    related to the job submission (execution_date, ending_date, 
    exit_status, exit_value, terminating_signal, resource_usage) .
    
    @type  drmaa_id: string
    @param drmaa_id: job identifier on DRMS if submitted via DRMAA
    @type  submission_date: date or None
    @param submission_date: submission date if the job was submitted
    '''
    with self._lock:
      connection = self._connect()
      cursor = connection.cursor()
      try:
        cursor.execute('''UPDATE jobs 
                          SET drmaa_id=?, 
                              submission_date=?, 
                              status=?, 
                              last_status_update=?,
                              exit_status=?,
                              exit_value=?,
                              terminating_signal=?,
                              resource_usage=?, 
                              execution_date=?,
                              ending_date=?
                              WHERE id=?''', 
                              (drmaa_id, 
                              submission_date, 
                              constants.UNDETERMINED, 
                              datetime.now(),
                              None,
                              None,
                              None,
                              None,
                              None,
                              None,
                              job_id))
      except Exception, e:
        connection.rollback()
        cursor.close()
        connection.close()
        raise WorkflowDatabaseServerError('Error set_submission_information %s: %s \n' %(type(e), e), self.logger) 
      connection.commit()
      cursor.close()
      connection.close()
      
  def get_drmaa_job_id(self, job_id):
    '''
    Returns the DRMAA job id associated with the job.
    Returns None if the job_id is not valid.
    
    @type job_id: C{JobIdentifier}
    @rtype: string
    @return: DRMAA job identifier (job identifier on DRMS if submitted via DRMAA)
    '''
    with self._lock:
      connection = self._connect()
      cursor = connection.cursor()
      try:
        count = cursor.execute('SELECT count(*) FROM jobs WHERE id=?', [job_id]).next()[0]
        if not count == 0 :
          drmaa_id = cursor.execute('SELECT drmaa_id FROM jobs WHERE id=?', [job_id]).next()[0] #supposes that the job_id is valid
        else:
          drmaa_id = None
      except Exception, e:
        cursor.close()
        connection.close()
        raise WorkflowDatabaseServerError('Error get_drmaa_job_id %s: %s \n' %(type(e), e), self.logger) 
      cursor.close()
      connection.close()
      return drmaa_id

  def get_std_out_err_file_path(self, job_id):
    '''
    Returns the path of the standard output and error files.
    The job_id must be valid.
    
    @type job_id: C{JobIdentifier}
    @rtype: tuple
    @return: (stdout_file_path, stderr_file_path)
    '''
    with self._lock:
      connection = self._connect()
      cursor = connection.cursor()
      try:
        result = cursor.execute('SELECT stdout_file, stderr_file FROM jobs WHERE id=?', [job_id]).next()#supposes that the job_id is valid
      except Exception, e:
        cursor.close()
        connection.close()
        raise WorkflowDatabaseServerError('Error %s: %s \n' %(type(e), e), self.logger) 
      cursor.close()
      connection.close()
    stdout_file_path = self._string_conversion(result[0])
    stderr_file_path = self._string_conversion(result[1])
    return (stdout_file_path, stderr_file_path)

  def set_job_exit_info(self, job_id, exit_status, exit_value, terminating_signal, resource_usage):
    '''
    Record the job exit status in the database.
    The status must be valid (ie a string among the exit job status 
    string defined in L{WorkflowDatabaseServer}.

    @type  job_id: C{JobIdentifier}
    @param job_id: job identifier
    @type  exit_status: string 
    @param exit_status: exit status string as defined in L{WorkflowDatabaseServer}
    @type  exit_value: int or None
    @param exit_value: if the status is FINISHED_REGULARLY, it contains the operating 
    system exit code of the job.
    @type  terminating_signal: string or None
    @param terminating_signal: if the status is FINISHED_TERM_SIG, it contain a representation 
    of the signal that caused the termination of the job.
    @type  resource_usage: string 
    @param resource_usage: contain the resource usage information of
    the job.
    '''
    with self._lock:
      # TBI if the status is not valid raise an exception ??
      connection = self._connect()
      cursor = connection.cursor()
      try:
        count = cursor.execute('SELECT count(*) FROM jobs WHERE id=?', [job_id]).next()[0]
        if not count == 0 :
          cursor.execute('''UPDATE jobs SET exit_status=?, 
                                          exit_value=?,    
                                          terminating_signal=?, 
                                          resource_usage=? 
                                          WHERE id=?''', 
                          (exit_status, 
                          exit_value,
                          terminating_signal,
                          resource_usage,
                          job_id)
                        )
      except Exception, e:
        connection.rollback()
        cursor.close()
        connection.close()
        raise WorkflowDatabaseServerError('Error set_job_exit_info %s: %s \n' %(type(e), e), self.logger) 
      connection.commit()
      cursor.close()
      connection.close()

  def get_job(self, job_id):
    '''
    returns the job information stored in the database.
    The job_id must be valid.
    @rtype: L{DBJob}
    '''
    with self._lock:
      connection = self._connect()
      cursor = connection.cursor()
      try:
        user_id,             \
                             \
        drmaa_id,            \
        expiration_date,     \
        status,              \
        last_status_update,  \
        workflow_id,         \
                             \
        command,             \
        stdin_file,          \
        join_errout,         \
        stdout_file,         \
        stderr_file,         \
        working_directory,   \
        custom_submission,   \
        parallel_config_name,\
        max_node_number,     \
        queue,               \
                             \
        name_description,    \
        submission_date,     \
        execution_date,      \
        ending_date,         \
        exit_status,         \
        exit_value,          \
        terminating_signal,  \
        resource_usage = cursor.execute('''SELECT user_id,
              
                                          drmaa_id,
                                          expiration_date,
                                          status,
                                          last_status_update,
                                          workflow_id,
                                          
                                          command,
                                          stdin_file,
                                          join_errout,
                                          stdout_file,
                                          stderr_file,
                                          working_directory,
                                          custom_submission,
                                          parallel_config_name,
                                          max_node_number,
                                          queue,
                                          
                                          name_description,
                                          submission_date,
                                          execution_date,
                                          ending_date,
                                          exit_status,
                                          exit_value,
                                          terminating_signal,
                                          resource_usage FROM jobs WHERE id=?''', [job_id]).next()#supposes that the job_id is valid
      except Exception, e:
        cursor.close()
        connection.close()
        raise WorkflowDatabaseServerError('Error get_job %s: %s \n' %(type(e), e), self.logger) 
      cursor.close()
      connection.close()
    
    dbJob = DBJob(user_id,
    
                  self._str_to_date_conversion(expiration_date),
                  join_errout,
                  self._string_conversion(stdout_file),
                  custom_submission,
                  
                  drmaa_id,
                  self._string_conversion(status),
                  self._str_to_date_conversion(last_status_update),
                  workflow_id,
                  
                  self._string_conversion(command),
                  self._string_conversion(stdin_file),
                  self._string_conversion(stderr_file),
                  self._string_conversion(working_directory),
                  self._string_conversion(parallel_config_name),
                  max_node_number,
                  self._string_conversion(queue),
                  
                  self._string_conversion(name_description),
                  self._str_to_date_conversion(submission_date),
                  self._str_to_date_conversion(execution_date),
                  self._str_to_date_conversion(ending_date),
                  self._string_conversion(exit_status),
                  exit_value,
                  self._string_conversion(terminating_signal),
                  self._string_conversion(resource_usage))
    return dbJob


  def _string_conversion(self, string):
    if string: 
      return string.encode('utf-8')
    else: 
      return string
  
  def _str_to_date_conversion(self,strdate):
    if strdate:
      date = datetime.strptime(strdate.encode('utf-8'), strtime_format)
    else:
      date = None
    return date

  ###############################################
  # INPUTS/OUTPUTS

  def register_inputs(self, job_id, local_input_files):
    '''
    Register associations between a job and input file path.
    
    @type job_id: C{JobIdentifier}
    @type  local_input_files: sequence of string
    @param local_input_files: local input file paths 
    '''
    with self._lock:
      connection = self._connect()
      cursor = connection.cursor()
      for file in local_input_files:
        try:
          cursor.execute('INSERT INTO ios (job_id, local_file_path, is_input) VALUES (?, ?, ?)',
                        (job_id, file, True))
        except Exception, e:
          connection.rollback()
          cursor.close()
          connection.close()
          raise WorkflowDatabaseServerError('Error register_inputs %s: %s \n' %(type(e), e), self.logger) 
      cursor.close()
      connection.commit()
      connection.close()
    
    
  def register_outputs(self, job_id, local_output_files):
    '''
    Register associations between a job and output file path.
    
    @type job_id: C{JobIdentifier}
    @type  local_input_files: sequence of string
    @param local_input_files: local output file paths 
    '''
    with self._lock:
      connection = self._connect()
      cursor = connection.cursor()
      for file in local_output_files:
        try:
          cursor.execute('INSERT INTO ios (job_id, local_file_path, is_input) VALUES (?, ?, ?)',
                        (job_id, file, False))
        except Exception, e:
          connection.rollback()
          cursor.close()
          connection.close()
          raise WorkflowDatabaseServerError('Error register_outputs %s: %s \n' %(type(e), e), self.logger) 
      cursor.close()
      connection.commit()
      connection.close()


     
  ################### DATABASE QUERYING ##############################
  
  #JOBS
  def is_user_job(self, job_id, user_id):
    '''
    Check that the job exists and is own by the user.
    
    @type job_id: C{JobIdentifier}
    @type user_id: C{UserIdentifier}
    @rtype: bool
    @returns: the job exists and is owned by the user
    '''
    with self._lock:
      connection = self._connect()
      cursor = connection.cursor()
      result = False
      try:
        count = cursor.execute('SELECT count(*) FROM jobs WHERE id=?', [job_id]).next()[0]
        if not count == 0 :
          owner_id = cursor.execute('SELECT user_id FROM jobs WHERE id=?',  [job_id]).next()[0] 
          result = (owner_id==user_id)
      except Exception, e:
        cursor.close()
        connection.close()
        raise WorkflowDatabaseServerError('Error is_user_job jobid=%s, userid=%s. Error %s: %s \n' %(repr(job_id), repr(user_id), type(e), e), self.logger) 
      cursor.close()
      connection.close()
    return result
  
  def get_jobs(self, user_id):
    '''
    Returns the jobs owned by the user.
    
    @type user_id: C{UserIdentifier}
    @rtype: sequence of C{JobIdentifier}
    @returns: jobs owned by the user
    '''
    with self._lock:
      connection = self._connect()
      cursor = connection.cursor()
      job_ids = []
      try:
        for row in cursor.execute('SELECT id FROM jobs WHERE user_id=?', [user_id]):
          jid = row[0]
          job_ids.append(jid)
      except Exception, e:
        cursor.close()
        connection.close()
        raise WorkflowDatabaseServerError('Error get_jobs %s: %s \n' %(type(e), e), self.logger) 
          
      cursor.close()
      connection.close()
      return job_ids
  
  
  def nb_queued_jobs(self, user_id, queue_name):
    '''
    Returns the number of job of the user with the status 
    constants.QUEUED_ACTIVE in the queue queue_name.
    
    @type user_id: C{UserIdentifier}
    @type queue_name: str
    @rtype: int
    '''
    with self._lock:
      connection = self._connect()
      cursor = connection.cursor()
      try:
        #print "queue_name " + repr(queue_name)
        if queue_name != None:
          count = cursor.execute("SELECT count(*) FROM jobs WHERE " 
                                 "user_id=? and ( status=? or status=?) " 
                                 "and queue=?", 
                                 [user_id, 
                                  constants.QUEUED_ACTIVE,
                                  constants.UNDETERMINED, 
                                  queue_name]).next()[0]
        else:
          count = cursor.execute("SELECT count(*) FROM jobs WHERE " 
                                 "user_id=? and ( status=? or status=?) " 
                                 "and queue ISNULL", 
                                 [user_id, 
                                  constants.QUEUED_ACTIVE,
                                  constants.UNDETERMINED]).next()[0]
        #print "count " + repr(count)
      except Exception, e:
        cursor.close()
        connection.close()
        raise WorkflowDatabaseServerError('Error nb_queued_jobs %s: %s \n' %(type(e), e), self.logger) 
          
      cursor.close()
      connection.close()
      return count


  def jobs_to_delete_and_kill(self, user_id):
    '''
    Returns the id of the job with the status constants.DELETE_PENDING

    @type user_id: C{UserIdentifier}
    @rtype: sequence of C{JobIdentifier}
    @returns: job with status constants.DELETE_PENDING
    '''
    with self._lock:
      connection = self._connect()
      cursor = connection.cursor()
      job_to_delete_ids = []
      job_to_kill_ids = []
      try:
        for row in cursor.execute("SELECT id FROM jobs " 
                                  "WHERE user_id=? AND status=?",
                                  [user_id, constants.DELETE_PENDING]):
          jid = row[0]
          job_to_delete_ids.append(jid)
        for row in cursor.execute("SELECT id FROM jobs " 
                                  "WHERE user_id=? AND status=?",
                                  [user_id, constants.KILL_PENDING]):
          jid = row[0]
          job_to_kill_ids.append(jid)
      except Exception, e:
        cursor.close()
        connection.close()
        raise WorkflowDatabaseServerError('Error get_jobs %s: %s \n' %(type(e), e), self.logger) 
          
      cursor.close()
      connection.close()
      return (job_to_delete_ids, job_to_kill_ids)



  #TRANSFERS
  
  def is_user_transfer(self, local_file_path, user_id):
    '''
    Check that a local file path match with a transfer and that the transfer 
    is owned by the user.
    
    @type local_file_path: string
    @type user_id: C{UserIdentifier}
    @rtype: bool
    @returns: the local file path match with a transfer owned by the user
    '''
    with self._lock:
      connection = self._connect()
      cursor = connection.cursor()
      result = False
      try:
        count = cursor.execute('SELECT count(*) FROM transfers WHERE local_file_path=?', [local_file_path]).next()[0]
        if not count == 0 :
          owner_id = cursor.execute('SELECT user_id FROM transfers WHERE local_file_path=?',  [local_file_path]).next()[0] 
          result = (owner_id==user_id)
      except Exception, e:
        cursor.close()
        connection.close()
        raise WorkflowDatabaseServerError('Error is_user_transfer %s: %s \n' %(type(e), e), self.logger) 
      cursor.close()
      connection.close()
    return result
    
    
  def get_transfers(self, user_id):
    '''
    Returns the transfers owned by the user.
    
    @type user_id: C{UserIdentifier}
    @rtype: sequence of local file path
    @returns: local file path associated with a transfer owned by the user
    '''
    with self._lock:
      connection = self._connect()
      cursor = connection.cursor()
      local_file_paths = []
      try:
        for row in cursor.execute('SELECT local_file_path FROM transfers WHERE user_id=?', [user_id]):
          local_file = row[0]
          local_file = self._string_conversion(local_file)
          local_file_paths.append(local_file)
      except Exception, e:
        cursor.close()
        connection.close()
        raise WorkflowDatabaseServerError('Error get_transfers %s: %s \n' %(type(e), e), self.logger) 
      cursor.close()
      connection.close()
    return local_file_paths
   

  #WORKFLOWS
  
  def is_user_workflow(self, wf_id, user_id):
    '''
    Check that the workflows exists and is own by the user.
    
    @type wf_id: C{WorflowIdentifier}
    @type user_id: C{UserIdentifier}
    @rtype: bool
    @returns: the workflow exists and is owned by the user
    '''
    with self._lock:
      connection = self._connect()
      cursor = connection.cursor()
      result = False
      try:
        count = cursor.execute('SELECT count(*) FROM workflows WHERE id=?', [wf_id]).next()[0]
        if not count == 0 :
          owner_id = cursor.execute('SELECT user_id FROM workflows WHERE id=?',  [wf_id]).next()[0] 
          result = (owner_id==user_id)
      except Exception, e:
        cursor.close()
        connection.close()
        raise WorkflowDatabaseServerError('Error is_user_workflow wfid=%d, userid=%s. Error %s: %s \n' %(repr(wf_id), repr(user_id), type(e), e), self.logger) 
      cursor.close()
      connection.close()
    return result
  
  def get_workflows(self, user_id):
    '''
    Returns the workflows owned by the user.
    
    @type user_id: C{UserIdentifier}
    @rtype: sequence of workflows id
    '''
    with self._lock:
      connection = self._connect()
      cursor = connection.cursor()
      wf_ids = []
      try:
        for row in cursor.execute("SELECT id FROM workflows " 
                                  "WHERE user_id=?", [user_id]):
          wf_id = row[0]
          wf_ids.append(wf_id)
      except Exception, e:
        cursor.close()
        connection.close()
        raise WorkflowDatabaseServerError('Error get_workflows %s: %s \n' %(type(e), e), self.logger) 
      cursor.close()
      connection.close()
    return wf_ids
  

  def workflows_to_delete_and_kill(self, user_id):
    '''
    Returns the id of the workfows with the status constants.DELETE_PENDING

    @type user_id: C{UserIdentifier}
    @rtype: sequence of C{WorkflowIdentifier}
    @returns: workflows with status constants.DELETE_PENDING
    '''
    with self._lock:
      connection = self._connect()
      cursor = connection.cursor()
      wf_to_delete_ids = []
      wf_to_kill_ids = []
      try:
        for row in cursor.execute("SELECT id FROM workflows " 
                                  "WHERE user_id=? AND status=?",
                                  [user_id, constants.DELETE_PENDING]):
          wf_id = row[0]
          wf_to_delete_ids.append(wf_id)
        for row in cursor.execute("SELECT id FROM workflows " 
                                  "WHERE user_id=? AND status=?",
                                  [user_id, constants.KILL_PENDING]):
          wf_id = row[0]
          wf_to_kill_ids.append(wf_id)
      except Exception, e:
        cursor.close()
        connection.close()
        raise WorkflowDatabaseServerError('Error get_jobs %s: %s \n' %(type(e), e), self.logger) 
          
      cursor.close()
      connection.close()
      return (wf_to_delete_ids, wf_to_kill_ids)


  #######################################################################

