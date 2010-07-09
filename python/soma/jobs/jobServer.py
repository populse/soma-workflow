'''
The class L{JobServer} is the data server used by the L{JobScheduler} to 
save all information related to submitted jobs to predefined sets of machines
linked together via a distributed resource management systems (DRMS).


@author: Yann Cointepas
@author: Soizic Laguitton
@organization: U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''
from __future__ import with_statement 
from sqlite3 import *
from datetime import date
from datetime import timedelta
from datetime import datetime
import threading
import os
import logging
import soma.jobs.constants as constants
import pickle

__docformat__ = "epytext en"


strtime_format = '%Y-%m-%d %H:%M:%S'





def adapt_datetime(ts):
    return ts.strftime(strtime_format)

register_adapter(datetime, adapt_datetime)

'''
Job server database tables:
  Users
    id
    login or other userId

  Jobs
    => identification:
      id      : int
      user_id : int
    
    => used by the job system (DrmaaJobScheduler, JobServer)
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

    => for user and administrator usage
      name_description   : string, optional 
      submission_date    : date 
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

  Input/Ouput junction table
    job_id 
    local file path (transferid)
    input or output
    
  Workflows
    id,
    user_id,
    pickle_file_path,
    expiration_date,
    
'''

def createDatabase(database_file):
  connection = connect(database_file, timeout = 5, isolation_level = "EXCLUSIVE")
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
                                       workflow_id          INTEGER CONSTRAINT known_workflow REFERENCES workflow (id),
                                       
                                       command              TEXT,
                                       stdin_file           TEXT,
                                       join_errout          BOOLEAN NOT NULL,
                                       stdout_file          TEXT NOT NULL,
                                       stderr_file          TEXT,
                                       working_directory    TEXT,
                                       custom_submission    BOOLEAN NOT NULL,
                                       parallel_config_name TEXT,
                                       max_node_number      INTEGER,
                                       
                                       name_description     TEXT,
                                       submission_date      DATE,
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
                                            workflow_id      INTEGER CONSTRAINT known_workflow REFERENCES workflow (id),
                                            status           VARCHAR(255) NOT NULL)''')

  cursor.execute('''CREATE TABLE ios (job_id           INTEGER NOT NULL CONSTRAINT known_job REFERENCES jobs(id),
                                      local_file_path  TEXT NOT NULL CONSTRAINT known_local_file REFERENCES transfers (local_file_path),
                                      is_input         BOOLEAN NOT NULL,
                                      PRIMARY KEY (job_id, local_file_path))''')
                                      
  cursor.execute('''CREATE TABLE fileCounter (count INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                                              foo INTEGER)''') #!!! FIND A CLEANER WAY !!!
                                              
  cursor.execute('''CREATE TABLE workflows (id               INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                                           user_id          INTEGER NOT NULL CONSTRAINT known_user REFERENCES users (id),
                                           pickle_file_path TEXT,
                                           expiration_date  DATE NOT NULL) ''')
  
  cursor.close()
  connection.commit()
  connection.close()



def printTables(database_file):
  
  connection = connect(database_file, timeout = 5, isolation_level = "EXCLUSIVE")
  cursor = connection.cursor()
  
  print "==== users table: ========"
  for row in cursor.execute('SELECT * FROM users'):
    id, login = row
    print 'id=', repr(id).rjust(2), 'login=', repr(login).rjust(7)
    
  print "==== transfers table: ===="
  for row in cursor.execute('SELECT * FROM transfers'):
    local_file_path, remote_file_path, transfer_date, expiration_date, user_id = row
    print '| local_file_path', repr(local_file_path).ljust(25), '| remote_file_path=', repr(remote_file_path).ljust(25) , '| transfer_date=', repr(transfer_date).ljust(7), '| expiration_date=', repr(expiration_date).ljust(7), '| user_id=', repr(user_id).rjust(2), ' |'
  
  print "==== jobs table: ========"
  for row in cursor.execute('SELECT * FROM jobs'):
    print row
    #id, submission_date, user_id, expiration_date, stdout_file, stderr_file, join_errout, stdin_file, name_description, drmaa_id,     working_directory = row
    #print 'id=', repr(id).rjust(3), 'submission_date=', repr(submission_date).rjust(7), 'user_id=', repr(user_id).rjust(3), 'expiration_date' , repr(expiration_date).rjust(7), 'stdout_file', repr(stdout_file).rjust(10), 'stderr_file', repr(stderr_file).rjust(10), 'join_errout', repr(join_errout).rjust(5), 'stdin_file', repr(stdin_file).rjust(10), 'name_description', repr(name_description).rjust(10), 'drmaa_id', repr(drmaa_id).rjust(10), 'working_directory', repr(working_directory).rjust(10)
  
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
              last_status_update = None,
              workflow_id = None,
              
              command = None,
              stdin_file = None,
              stderr_file = None,
              working_directory = None,
              parallel_config_name = None,
              max_node_number = 1,
              
              name_description = None,
              submission_date = None,
              exit_status = None,
              exit_value = None,
              terminating_signal = None,
              resource_usage = None):
              
    '''
    Adds a job to the database and returns its identifier.
    
    @type user_id: C{UserIdentifier}
    @type expiration_date: date
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
                                 in JobServer.
    @type  max_node_number: int 
    @param max_node_number: maximum of node requested by the job to run
    
    @type  name_description: string
    @param name_description: optional description of the job.  
    @type  exit_status: string 
    @param exit_status: exit status string as defined in L{JobServer}
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
    
    self.name_description = name_description
    self.submission_date = submission_date
    self.exit_status = exit_status
    self.exit_value = exit_value
    self.terminating_signal = terminating_signal
    self.resource_usage = resource_usage



class JobServerError( Exception):
  def __init__(self, msg, logger=None):
    self.args = (msg,)
    if logger: 
      logger.critical('EXCEPTION ' + msg)


class JobServer ( object ):
  
  def __init__(self, database_file, tmp_file_dir_path):
    '''
    The constructor gets as parameter the database information.
    
    @type  database_file: string
    @param database_file: the SQLite database file 
    @type  tmp_file_dir_path: string
    @param tmp_file_dir_path: place on the file system shared by all the machine of the pool 
    used to stored temporary transfered files.
    '''
        
    self.__tmp_file_dir_path = tmp_file_dir_path
    self.__database_file = database_file
     
    self.__lock = threading.RLock()
   
    self.logger = logging.getLogger('jobServer')
    
    with self.__lock:
      if not os.path.isfile(database_file):
        print "Database creation " + database_file
        self.logger.info("Database creation " + database_file)
        createDatabase(database_file)
      
      
      
  def __del__(self):
   pass 
    
    
  def __connect(self):
    try:
      connection = connect(self.__database_file, timeout = 10, isolation_level = "EXCLUSIVE")
    except Exception, e:
        raise JobServerError('Database connection error %s: %s \n' %(type(e), e), self.logger) 
    return connection
  
  def registerUser(self, login):
    '''
    Register a user so that he can submit job.
    
    @rtype: C{UserIdentifier}
    @return: user identifier
    '''
    with self.__lock:
      connection = self.__connect()
      cursor = connection.cursor()
      try:
        count = cursor.execute('SELECT count(*) FROM users WHERE login=?', [login]).next()[0]
        if count==0:
          cursor.execute('INSERT INTO users (login) VALUES (?)', [login])          
          personal_path = self.__tmp_file_dir_path + "/" + login
          if not os.path.isdir(personal_path):
            os.mkdir(personal_path)
        user_id = cursor.execute('SELECT id FROM users WHERE login=?', [login]).next()[0]
      except Exception, e:
        connection.rollback()
        cursor.close()
        connection.close()
        raise JobServerError('Error registerUser %s: %s \n' %(type(e), e), self.logger) 
      connection.commit()
      cursor.close()
      connection.close()
      return user_id
  
  
  def clean(self) :
    '''
    Delete all expired jobs, transfers and workflows, except transfers which are requested 
    by valid job.
    '''
    with self.__lock:
      connection = self.__connect()
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
            self.__removeFile(stdof)
            self.__removeFile(stdef)
        
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
        
        for row in cursor.execute('SELECT pickle_file_path FROM workflows WHERE expiration_date < ?', [date.today()]):
          pickle_file_path = row[0]
          self.__removeFile(pickle_file_path)
          
        cursor.execute('DELETE FROM workflows WHERE expiration_date < ?', [date.today()])
        
        
      except Exception, e:
        connection.rollback()
        cursor.close()
        connection.close()
        raise JobServerError('Error clean %s: %s \n' %(type(e), e), self.logger) 
      
      cursor.close()
      connection.commit()
      connection.close()
     
     
  def generateLocalFilePath(self, user_id, remote_file_path=None):
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
    with self.__lock:
      connection = self.__connect()
      cursor = connection.cursor()
      try:
        login = cursor.execute('SELECT login FROM users WHERE id=?',  [user_id]).next()[0]#supposes that the user_id is valid
        login = login.encode('utf-8')
        cursor.execute('INSERT INTO fileCounter (foo) VALUES (?)', [0])
        file_num = cursor.lastrowid
      except Exception, e:
        connection.rollback()
        cursor.close()
        connection.close()
        raise JobServerError('Error generateLocalFilePath %s: %s \n' %(type(e), e), self.logger) 
      
      newFilePath = self.__tmp_file_dir_path + "/" + login + '/'
      if remote_file_path == None:
        newFilePath += repr(file_num)
      else:
        iextention = remote_file_path.rfind(".")
        if iextention == -1 :
          newFilePath += remote_file_path[remote_file_path.rfind("/")+1:] + '_' + repr(file_num) 
        else: 
          newFilePath += remote_file_path[remote_file_path.rfind("/")+1:iextention] + '_' + repr(file_num) + remote_file_path[iextention:]
      cursor.close()
      connection.commit()
      connection.close()
      return newFilePath
        
  def __removeFile(self, file_path):
    if file_path and os.path.isfile(file_path):
      try:
        os.remove(file_path)
      except OSError,e:
        print "Could not remove file %s, error %s: %s \n" %(file_path, type(e), e) 
    
    
  #####################################"
  # TRANSFERS 
  
  def addTransfer(self, local_file_path, remote_file_path, expiration_date, user_id, status = constants.READY_TO_TRANSFER, workflow_id = -1):
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
    '''
    with self.__lock:
      connection = self.__connect()
      cursor = connection.cursor()
      try:
        cursor.execute('''INSERT INTO transfers 
                        (local_file_path, remote_file_path, transfer_date, expiration_date, user_id, workflow_id, status) VALUES (?, ?, ?, ?, ?, ?, ?)''',
                        (local_file_path, remote_file_path, date.today(), expiration_date, user_id, workflow_id, status))
      except Exception, e:
        connection.rollback()
        cursor.close()
        connection.close()
        raise JobServerError('Error addTransfer %s: %s \n' %(type(e), e), self.logger) 
      cursor.close()
      connection.commit()
      connection.close()


  def removeTransfer(self, local_file_path):
    '''
    Set the expiration date of the transfer associated to the local file path 
    to today (yesterday?). That way it will be disposed as soon as no job will need it.
    
    @type  local_file_path: string
    @param local_file_path: local file path to identifying the transfer 
    record to delete.
    '''
    with self.__lock:
      connection = self.__connect()
      cursor = connection.cursor()
      yesterday = date.today() - timedelta(days=1)
      try:
        cursor.execute('UPDATE transfers SET expiration_date=? WHERE local_file_path=?', (yesterday, local_file_path))
      except Exception, e:
        connection.rollback()
        cursor.close()
        connection.close()
        raise JobServerError('Error removeTransfer %s: %s \n' %(type(e), e), self.logger) 
      connection.commit()
      cursor.close()
      connection.close()
      self.clean()
        
  def getTransferInformation(self, local_file_path):
    '''
    Returns the information related to the transfer associated to the local file path.
    The local_file_path must be associated to a transfer.
    Returns (None, None, None, -1) if the local_file_path is not associated to a transfer.
    
    @type local_file_path: string
    @rtype: tuple
    @returns: (local_file_path, remote_file_path, expiration_date)
    '''
    with self.__lock:
      connection = self.__connect()
      cursor = connection.cursor()
      try:
        count = cursor.execute('SELECT count(*) FROM transfers WHERE local_file_path=?', [local_file_path]).next()[0]
        if not count == 0 :
          info = cursor.execute('''SELECT 
                                   local_file_path, 
                                   remote_file_path, 
                                   expiration_date, 
                                   workflow_id 
                                   FROM transfers 
                                   WHERE local_file_path=?''', 
                                   [local_file_path]).next() #supposes that the local_file_path is associated to a transfer
        else: 
          info = None
      except Exception, e:
        cursor.close()
        connection.close()
        raise JobServerError('Error getTransferInformation %s: %s \n' %(type(e), e), self.logger) 
      if info:
        result = (info[0].encode('utf-8'), info[1].encode('utf-8'), info[2].encode('utf-8'), info[3])
      else:
        result = (None, None, None, -1)
      cursor.close()
      connection.close()
    return result
  
  def getTransferStatus(self, local_file_path):
    '''
    Returns the transferstatus sored in the database.
    Returns None if the local_file_path is not associated to a transfer.
    '''
    with self.__lock:
      connection = self.__connect()
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
        raise JobServerError('Error getTransferStatus %s: %s \n' %(type(e), e), self.logger) 
      if status:
        status = status.encode('utf-8')
      cursor.close()
      connection.close()
 
    return status
  
  def setTransferStatus(self, local_file_path, status):
    '''
    Updates the transfer status in the database.
    The status must be valid (ie a string among the transfer status 
    string defined in constants.FILE_TRANSFER_STATUS
    
    @type  status: string
    @param status: transfer status as defined in constants.FILE_TRANSFER_STATUS
    '''
    with self.__lock:
      # TBI if the status is not valid raise an exception ??
      connection = self.__connect()
      cursor = connection.cursor()
      try:
        count = cursor.execute('SELECT count(*) FROM transfers WHERE local_file_path=?', [local_file_path]).next()[0]
        if not count == 0 :
          cursor.execute('UPDATE transfers SET status=? WHERE local_file_path=?', (status, local_file_path))
      except Exception, e:
        connection.rollback()
        cursor.close()
        connection.close()
        raise JobServerError('Error setTransferStatus %s: %s \n' %(type(e), e), self.logger) 
      connection.commit()
      cursor.close()
      connection.close()

  
  ###############################################
  # WORKFLOWS
  
  def addWorkflow(self, user_id, expiration_date):
    '''
    Adds a job to the database and returns its identifier.
    
    @type user_id: C{UserIdentifier}
    @type expiration_date: date
    
    @rtype: C{WorkflowIdentifier}
    @return: the identifier of the workflow
    '''
    with self.__lock:
      connection = self.__connect()
      cursor = connection.cursor()
      try:
        cursor.execute('''INSERT INTO workflows 
                         (user_id,
                          pickle_file_path,
                          expiration_date)
                          VALUES (?, ?, ?)''',
                         (user_id,
                          None, 
                          expiration_date))
      except Exception, e:
        connection.rollback()
        cursor.close()
        connection.close()
        raise JobServerError('Error addWorkflow %s: %s \n' %(type(e), e), self.logger) 
      connection.commit()
      wf_id = cursor.lastrowid
      cursor.close()
      connection.close()

    return wf_id
  
  def deleteWorkflow(self, wf_id):
    '''
    Remove the workflow from the database. Remove all associated jobs and transfers.
    
    @type wf_id: C{WorkflowIdentifier}
    '''
    with self.__lock:
      # set expiration date to yesterday + clean() ?
      connection = self.__connect()
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
        raise JobServerError('Error deleteJob %s: %s \n' %(type(e), e), self.logger) 
        
      cursor.close()
      connection.commit()
      connection.close()
      self.clean()

  def setWorkflow(self, wf_id, workflow, user_id):
    '''
    Saves the workflow in a file and register the file path in the database.
    
    @type user_id: C{WorkflowIdentifier}
    @type  workflow: L{Workflow}
    '''
    pickle_file_path = self.generateLocalFilePath(user_id)
    file = open(pickle_file_path, "w")
    pickle.dump(workflow, file)
    file.close()
    with self.__lock:
      connection = self.__connect()
      cursor = connection.cursor()
      try:
        cursor.execute('UPDATE workflows SET pickle_file_path=? WHERE id=?', (pickle_file_path, wf_id))
      except Exception, e:
        connection.rollback()
        cursor.close()
        connection.close()
        raise JobServerError('Error setWorkflow %s: %s \n' %(type(e), e), self.logger) 
      connection.commit()
      cursor.close()
      connection.close()
      
  def getWorkflow(self, wf_id):
    '''
    Return the workflow object
    The wf_id must be valid.
    
    @type wf_id: C{WorflowIdentifier}
    @rtype: C{Workflow}
    '''
    with self.__lock:
      connection = self.__connect()
      cursor = connection.cursor()
      try:
        pickle_file_path = cursor.execute('''SELECT  
                                              pickle_file_path
                                              FROM workflows WHERE id=?''', [wf_id]).next()[0]#supposes that the wf_id is valid
      except Exception, e:
        cursor.close()
        connection.close()
        raise JobServerError('Error getWorkflow %s: %s \n' %(type(e), e), self.logger) 
      cursor.close()
      connection.close()

    file = open(pickle_file_path, "r")
    workflow = pickle.load(file)
    file.close()
    
    return workflow
    
  
      
  ###########################################
  # JOBS 
  
  def addJob( self, dbJob):
    '''
    Adds a job to the database and returns its identifier.
    
    @type  dbJob: L{DBJob}
    @param dbJob: Job information.
    
    @rtype: C{JobIdentifier}
    @return: the identifier of the job
    '''
    with self.__lock:
      connection = self.__connect()
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
                                       
                          name_description,
                          submission_date,
                          exit_status,
                          exit_value,
                          terminating_signal,
                          resource_usage)
                          VALUES (?, ?, ?, ?, ?,
                                  ?, ?, ?, ?, ?, 
                                  ?, ?, ?, ?, ?, 
                                  ?, ?, ?, ?, ?,
                                  ?)''',
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
                          
                          dbJob.name_description,
                          dbJob.submission_date,
                          dbJob.exit_status,
                          dbJob.exit_value,
                          dbJob.terminating_signal,
                          dbJob.resource_usage
                          ))
      except Exception, e:
        connection.rollback()
        cursor.close()
        connection.close()
        raise JobServerError('Error addJob %s: %s \n' %(type(e), e), self.logger) 
      connection.commit()
      job_id = cursor.lastrowid
      cursor.close()
      connection.close()

    # create standard output files (not mandatory but useful for "tail -f" kind of function)
    try:  
      tmp = open(dbJob.stdout_file, 'w')
      tmp.close()
    except IOError, e:
      raise JobServerError("Could not create the standard output file %s: %s \n"  %(type(e), e), self.logger)
    if dbJob.stderr_file:
      try:
        tmp = open(dbJob.stderr_file, 'w')
        tmp.close()
      except IOError, e:
        raise JobServerError("Could not create the standard error file %s: %s \n"  %(type(e), e), self.logger)

    return job_id
   
   
  def deleteJob(self, job_id):
    '''
    Remove the job from the database. Remove all associated transfered files if
    their expiration date passed and they are not used by any other job.
    
    @type job_id: 
    '''
    with self.__lock:
      # set expiration date to yesterday + clean() ?
      connection = self.__connect()
      cursor = connection.cursor()
      
      yesterday = date.today() - timedelta(days=1)
    
      try:
        cursor.execute('UPDATE jobs SET expiration_date=? WHERE id=?', (yesterday, job_id))
          
      except Exception, e:
        connection.rollback()
        cursor.close()
        connection.close()
        raise JobServerError('Error deleteJob %s: %s \n' %(type(e), e), self.logger) 
        
      cursor.close()
      connection.commit()
      connection.close()
      self.clean()



  def setJobStatus(self, job_id, status):
    '''
    Updates the job status in the database.
    The status must be valid (ie a string among the job status 
    string defined in constants.JOB_STATUS
    
    @type  status: string
    @param status: job status as defined in constants.JOB_STATUS
    '''
    with self.__lock:
      # TBI if the status is not valid raise an exception ??
      connection = self.__connect()
      cursor = connection.cursor()
      try:
        count = cursor.execute('SELECT count(*) FROM jobs WHERE id=?', [job_id]).next()[0]
        if not count == 0 :
          cursor.execute('UPDATE jobs SET status=?, last_status_update=? WHERE id=?', (status, datetime.now(), job_id))
      except Exception, e:
        connection.rollback()
        cursor.close()
        connection.close()
        raise JobServerError('Error setJobStatus %s: %s \n' %(type(e), e), self.logger) 
      connection.commit()
      cursor.close()
      connection.close()
      
  def getJobStatus(self, job_id):
    '''
    Returns the job status sored in the database (updated by L{DrmaaJobScheduler}) and 
    the date of its last update.
    Returns (None, None) if the job_id is not valid.
    '''
    with self.__lock:
      connection = self.__connect()
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
        raise JobServerError('Error getJobStatus %s: %s \n' %(type(e), e), self.logger) 
      if status: status = status.encode('utf-8')
      if strdate:
        date = datetime.strptime(strdate.encode('utf-8'), strtime_format)
      else:
        date = None
      cursor.close()
      connection.close()
 
    return (status, date)

  def setSubmissionInformation(self, job_id, drmaa_id, submission_date):
    '''
    Set the submission information of the job.
    
    @type  drmaa_id: string
    @param drmaa_id: job identifier on DRMS if submitted via DRMAA
    @type  submission_date: date or None
    @param submission_date: submission date if the job was submitted
    '''
    with self.__lock:
      connection = self.__connect()
      cursor = connection.cursor()
      try:
        cursor.execute('UPDATE jobs SET drmaa_id=?, submission_date=?, status=? WHERE id=?', (drmaa_id, submission_date, constants.UNDETERMINED, job_id))
      except Exception, e:
        connection.rollback()
        cursor.close()
        connection.close()
        raise JobServerError('Error setSubmissionInformation %s: %s \n' %(type(e), e), self.logger) 
      connection.commit()
      cursor.close()
      connection.close()
      
  def getDrmaaJobId(self, job_id):
    '''
    Returns the DRMAA job id associated with the job.
    Returns None if the job_id is not valid.
    
    @type job_id: C{JobIdentifier}
    @rtype: string
    @return: DRMAA job identifier (job identifier on DRMS if submitted via DRMAA)
    '''
    with self.__lock:
      connection = self.__connect()
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
        raise JobServerError('Error getDrmaaJobId %s: %s \n' %(type(e), e), self.logger) 
      cursor.close()
      connection.close()
      return drmaa_id

  def getStdOutErrFilePath(self, job_id):
    '''
    Returns the path of the standard output and error files.
    The job_id must be valid.
    
    @type job_id: C{JobIdentifier}
    @rtype: tuple
    @return: (stdout_file_path, stderr_file_path)
    '''
    with self.__lock:
      connection = self.__connect()
      cursor = connection.cursor()
      try:
        result = cursor.execute('SELECT stdout_file, stderr_file FROM jobs WHERE id=?', [job_id]).next()#supposes that the job_id is valid
      except Exception, e:
        cursor.close()
        connection.close()
        raise JobServerError('Error %s: %s \n' %(type(e), e), self.logger) 
      cursor.close()
      connection.close()
    return result

  def setJobExitInfo(self, job_id, exit_status, exit_value, terminating_signal, resource_usage):
    '''
    Record the job exit status in the database.
    The status must be valid (ie a string among the exit job status 
    string defined in L{JobServer}.

    @type  job_id: C{JobIdentifier}
    @param job_id: job identifier
    @type  exit_status: string 
    @param exit_status: exit status string as defined in L{JobServer}
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
    with self.__lock:
      # TBI if the status is not valid raise an exception ??
      connection = self.__connect()
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
        raise JobServerError('Error setJobExitInfo %s: %s \n' %(type(e), e), self.logger) 
      connection.commit()
      cursor.close()
      connection.close()

  def getJob(self, job_id):
    '''
    returns the job information stored in the database.
    The job_id must be valid.
    @rtype: L{DBJob}
    '''
    with self.__lock:
      connection = self.__connect()
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
                             \
        name_description,    \
        submission_date,     \
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
                                          
                                          name_description,
                                          submission_date,
                                          exit_status,
                                          exit_value,
                                          terminating_signal,
                                          resource_usage FROM jobs WHERE id=?''', [job_id]).next()#supposes that the job_id is valid
      except Exception, e:
        cursor.close()
        connection.close()
        raise JobServerError('Error getJob %s: %s \n' %(type(e), e), self.logger) 
      cursor.close()
      connection.close()
    dbJob = DBJob(user_id,
    
                  expiration_date,
                  join_errout,
                  stdout_file,
                  custom_submission,
                  
                  drmaa_id,
                  status,
                  last_status_update,
                  workflow_id,
                  
                  command,
                  stdin_file,
                  stderr_file,
                  working_directory,
                  parallel_config_name,
                  max_node_number,
                  
                  name_description,
                  submission_date,
                  exit_status,
                  exit_value,
                  terminating_signal,
                  resource_usage)
    return dbJob


  ###############################################
  # INPUTS/OUTPUTS

  def registerInputs(self, job_id, local_input_files):
    '''
    Register associations between a job and input file path.
    
    @type job_id: C{JobIdentifier}
    @type  local_input_files: sequence of string
    @param local_input_files: local input file paths 
    '''
    with self.__lock:
      connection = self.__connect()
      cursor = connection.cursor()
      for file in local_input_files:
        try:
          cursor.execute('INSERT INTO ios (job_id, local_file_path, is_input) VALUES (?, ?, ?)',
                        (job_id, file, True))
        except Exception, e:
          connection.rollback()
          cursor.close()
          connection.close()
          raise JobServerError('Error registerInputs %s: %s \n' %(type(e), e), self.logger) 
      cursor.close()
      connection.commit()
      connection.close()
    
    
  def registerOutputs(self, job_id, local_output_files):
    '''
    Register associations between a job and output file path.
    
    @type job_id: C{JobIdentifier}
    @type  local_input_files: sequence of string
    @param local_input_files: local output file paths 
    '''
    with self.__lock:
      connection = self.__connect()
      cursor = connection.cursor()
      for file in local_output_files:
        try:
          cursor.execute('INSERT INTO ios (job_id, local_file_path, is_input) VALUES (?, ?, ?)',
                        (job_id, file, False))
        except Exception, e:
          connection.rollback()
          cursor.close()
          connection.close()
          raise JobServerError('Error registerOutputs %s: %s \n' %(type(e), e), self.logger) 
      cursor.close()
      connection.commit()
      connection.close()


     
  ################### DATABASE QUERYING ##############################
  
  #JOBS
  def isUserJob(self, job_id, user_id):
    '''
    Check that the job exists and is own by the user.
    
    @type job_id: C{JobIdentifier}
    @type user_id: C{UserIdentifier}
    @rtype: bool
    @returns: the job exists and is owned by the user
    '''
    with self.__lock:
      connection = self.__connect()
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
        raise JobServerError('Error isUserJob jobid=%s, userid=%s. Error %s: %s \n' %(repr(job_id), repr(user_id), type(e), e), self.logger) 
      cursor.close()
      connection.close()
    return result
  
  def getJobs(self, user_id):
    '''
    Returns the jobs owned by the user.
    
    @type user_id: C{UserIdentifier}
    @rtype: sequence of C{JobIdentifier}
    @returns: jobs owned by the user
    '''
    with self.__lock:
      connection = self.__connect()
      cursor = connection.cursor()
      job_ids = []
      try:
        for row in cursor.execute('SELECT id FROM jobs WHERE user_id=?', [user_id]):
          jid = row[0]
          job_ids.append(jid)
      except Exception, e:
        cursor.close()
        connection.close()
        raise JobServerError('Error getJobs %s: %s \n' %(type(e), e), self.logger) 
          
      cursor.close()
      connection.close()
      return job_ids
  
  
  #TRANSFERS
  
  def isUserTransfer(self, local_file_path, user_id):
    '''
    Check that a local file path match with a transfer and that the transfer 
    is owned by the user.
    
    @type local_file_path: string
    @type user_id: C{UserIdentifier}
    @rtype: bool
    @returns: the local file path match with a transfer owned by the user
    '''
    with self.__lock:
      connection = self.__connect()
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
        raise JobServerError('Error isUserTransfer %s: %s \n' %(type(e), e), self.logger) 
      cursor.close()
      connection.close()
    return result
    
    
  def getTransfers(self, user_id):
    '''
    Returns the transfers owned by the user.
    
    @type user_id: C{UserIdentifier}
    @rtype: sequence of local file path
    @returns: local file path associated with a transfer owned by the user
    '''
    with self.__lock:
      connection = self.__connect()
      cursor = connection.cursor()
      local_file_paths = []
      try:
        for row in cursor.execute('SELECT local_file_path FROM transfers WHERE user_id=?', [user_id]):
          local_file = row[0]
          local_file = local_file.encode('utf-8')
          local_file_paths.append(local_file)
      except Exception, e:
        cursor.close()
        connection.close()
        raise JobServerError('Error getTransfers %s: %s \n' %(type(e), e), self.logger) 
      cursor.close()
      connection.close()
    return local_file_paths
   

  #WORKFLOWS
  
  def isUserWorkflow(self, wf_id, user_id):
    '''
    Check that the workflows exists and is own by the user.
    
    @type wf_id: C{WorflowIdentifier}
    @type user_id: C{UserIdentifier}
    @rtype: bool
    @returns: the workflow exists and is owned by the user
    '''
    with self.__lock:
      connection = self.__connect()
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
        raise JobServerError('Error isUserWorkflow wfid=%d, userid=%s. Error %s: %s \n' %(repr(wf_id), repr(user_id), type(e), e), self.logger) 
      cursor.close()
      connection.close()
    return result
  
  def getWorkflows(self, user_id):
    '''
    Returns the workflows owned by the user.
    
    @type user_id: C{UserIdentifier}
    @rtype: sequence of workflows id
    '''
    with self.__lock:
      connection = self.__connect()
      cursor = connection.cursor()
      wf_ids = []
      try:
        for row in cursor.execute('SELECT id FROM workflows WHERE user_id=?', [user_id]):
          wf_id = row[0]
          
          wf_ids.append(wf_id)
      except Exception, e:
        cursor.close()
        connection.close()
        raise JobServerError('Error getWorkflows %s: %s \n' %(type(e), e), self.logger) 
      cursor.close()
      connection.close()
    return wf_ids
  


  #######################################################################

