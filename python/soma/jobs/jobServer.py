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
import soma.jobs.jobDatabase 
import threading

import os
import logging

__docformat__ = "epytext en"

class JobServerError( Exception):
  def __init__(self, msg):
    self.args = (msg,)
    logger = logging.getLogger('jobServer')
    logger.critical('EXCEPTION ' + msg)


class JobServer ( object ):

  '''
  Job status:
  '''
  UNDETERMINED="undetermined"
  QUEUED_ACTIVE="queued_active"
  SYSTEM_ON_HOLD="system_on_hold"
  USER_ON_HOLD="user_on_hold"
  USER_SYSTEM_ON_HOLD="user_system_on_hold"
  RUNNING="running"
  SYSTEM_SUSPENDED="system_suspended"
  USER_SUSPENDED="user_suspended"
  USER_SYSTEM_SUSPENDED="user_system_suspended"
  DONE="done"
  FAILED="failed"
  '''
  Exit job status:
  '''
  EXIT_UNDETERMINED="exit_status_undetermined"
  EXIT_ABORTED="aborted"
  FINISHED_REGULARLY="finished_regularly"
  FINISHED_TERM_SIG="finished_signal"
  FINISHED_UNCLEAR_CONDITIONS="finished_unclear_condition"
  USER_KILLED="killed_by_user"
    
  def __init__(self, database_file, tmp_file_dir_path):
    '''
    The constructor gets as parameter the database information.
    
    @type  database_file: string
    @param database_file: the SQLite database file 
    @type  tmp_file_dir_path: string
    @param tmp_file_dir_path: place on the file system shared by all the machine of the pool 
    used to stored temporary transfered files.
    '''
    
    logging.basicConfig(
      filename = "/volatile/laguitton/log_jobServer",
      format = "%(asctime)s => line %(lineno)s: %(message)s",
      level = logging.DEBUG)
     
    self.__tmp_file_dir_path = tmp_file_dir_path
    self.__database_file = database_file
     
    self.__lock = threading.RLock()
   
    
    with self.__lock:
      if not os.path.isfile(database_file):
        print "Database creation " + database_file
        soma.jobs.jobDatabase.create(database_file)
      
      
      
  def __del__(self):
   pass 
    
    
  def __connect(self):
    try:
      connection = connect(self.__database_file, timeout = 10, isolation_level = "EXCLUSIVE")
    except Exception, e:
        raise JobServerError('Database connection error %s: %s \n' %(type(e), e)) 
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
        raise JobServerError('Error registerUser %s: %s \n' %(type(e), e)) 
      connection.commit()
      cursor.close()
      connection.close()
      return user_id
  
  
  
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
        raise JobServerError('Error generateLocalFilePath %s: %s \n' %(type(e), e)) 
      
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
    
    
    
    
  def addTransfer(self, local_file_path, remote_file_path, expiration_date, user_id):
    '''
    Adds a transfer to the database.
    
    @type local_file_path: string
    @type  remote_file_path: string or None
    @param remote_file_path: C{None} for job standard output or error only.
    @type expiration_date: date
    @type user_id:  C{UserIdentifier}
    '''
    with self.__lock:
      connection = self.__connect()
      cursor = connection.cursor()
      try:
        cursor.execute('''INSERT INTO transfers 
                        (local_file_path, remote_file_path, transfer_date, expiration_date, user_id) VALUES (?, ?, ?, ?, ?)''',
                        (local_file_path, remote_file_path, date.today(), expiration_date, user_id))
      except Exception, e:
        connection.rollback()
        cursor.close()
        connection.close()
        raise JobServerError('Error addTransfer %s: %s \n' %(type(e), e)) 
      cursor.close()
      connection.commit()
      connection.close()


  def removeTransferASAP(self, local_file_path):
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
        raise JobServerError('Error removeTransferASAP %s: %s \n' %(type(e), e)) 
      connection.commit()
      cursor.close()
      connection.close()
      self.clean()

  def addJob(self, 
               user_id,
               custom_submission,
               expiration_date,
               stdout_file,
               stderr_file,
               join_stderrout=False,
               stdin_file=None,
               name_description=None,
               drmaa_id=None,
               working_directory=None, 
               command_info=None):
    '''
    Adds a job to the database and returns its identifier.
    
    @type user_id: C{UserIdentifier}
    @type expiration_date: date
    @type custom_submission: Boolean
    @type custom_submission: C{True} if it was a custom submission. If C{True} 
    the standard output files won't be deleted with the job.
    @type  stdout_file: string
    @type  stderr_file: string
    @type  join_stderrout: bool
    @param join_stderrout: C{True} if the standard error should be redirect in the 
    same file as the standard output.
    @type  stdin_file: string
    @param stdin_file: job's standard input as a path to a file. C{None} if the 
    job doesn't require an input stream.
    @type  name_description: string
    @param name_description: optional description of the job.
    @type  drmaa_id: string
    @param drmaa_id: job identifier on DRMS if submitted via DRMAA
    @type  working_directory: string
    @type  command_info: string
    @param command_info: job command for user information only
    @rtype: C{JobIdentifier}
    @return: the identifier of the job
    '''
    with self.__lock:
      connection = self.__connect()
      cursor = connection.cursor()
      try:
        cursor.execute('''INSERT INTO jobs 
                        (
                         user_id,
                         
                         drmaa_id,
                         expiration_date,
                         status,
                         stdin_file,
                         join_errout,
                         stdout_file,
                         stderr_file,
                         working_directory,
                         custom_submission,
                  
                         name_description,
                         command,
                         submission_date, 
                         exit_status)
                        VALUES (?, ?, ?, ?, ?,
                                ?, ?, ?, ?, ?, 
                                ?, ?, ?, ?)''',
                        (user_id,
                        
                        drmaa_id,
                        expiration_date, 
                        JobServer.UNDETERMINED,
                        stdin_file,
                        join_stderrout,
                        stdout_file,
                        stderr_file,
                        working_directory,
                        custom_submission,

                        name_description,
                        command_info,
                        date.today(), 
                        JobServer.EXIT_UNDETERMINED))
      except Exception, e:
        connection.rollback()
        cursor.close()
        connection.close()
        raise JobServerError('Error addJob %s: %s \n' %(type(e), e)) 
      connection.commit()
      job_id = cursor.lastrowid
      cursor.close()
      connection.close()

    # create standard output files (not mandatory but useful for "tail -f" kind of function)
    try:  
      tmp = open(stdout_file, 'w')
      tmp.close()
    except IOError, e:
      raise JobServerError("Could not create the standard output file %s: %s \n"  %(type(e), e))
    if stderr_file:
      try:
        tmp = open(stderr_file, 'w')
        tmp.close()
      except IOError, e:
        raise JobServerError("Could not create the standard error file %s: %s \n"  %(type(e), e))

    return job_id
   

  def setJobStatus(self, job_id, status):
    '''
    Updates the job status in the database.
    The status must be valid (ie a string among the job status 
    string defined in L{JobServer}.

    @type job_id: C{JobIdentifier}
    @type status: status string as defined in L{JobServer}
    '''
    with self.__lock:
      # TBI if the status is not valid raise an exception ??
      connection = self.__connect()
      cursor = connection.cursor()
      try:
        cursor.execute('UPDATE jobs SET status=? WHERE id=?', (status, job_id))
      except Exception, e:
        connection.rollback()
        cursor.close()
        connection.close()
        raise JobServerError('Error setJobStatus %s: %s \n' %(type(e), e)) 
      connection.commit()
      cursor.close()
      connection.close()


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
        raise JobServerError('Error setJobExitInfo %s: %s \n' %(type(e), e)) 
      connection.commit()
      cursor.close()
      connection.close()



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
          raise JobServerError('Error registerInputs %s: %s \n' %(type(e), e)) 
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
          raise JobServerError('Error registerOutputs %s: %s \n' %(type(e), e)) 
      cursor.close()
      connection.commit()
      connection.close()



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
        raise JobServerError('Error deleteJob %s: %s \n' %(type(e), e)) 
        
      cursor.close()
      connection.commit()
      connection.close()
      self.clean()


  def clean(self) :
    '''
    Delete all expired jobs and transfers, except transfers which are requested 
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
        
        
        
      except Exception, e:
        connection.rollback()
        cursor.close()
        connection.close()
        raise JobServerError('Error clean %s: %s \n' %(type(e), e)) 
      
      cursor.close()
      connection.commit()
      connection.close()
     
     
  def __removeFile(self, file_path):
    if file_path and os.path.isfile(file_path):
      try:
        os.remove(file_path)
      except OSError,e:
        print "Could not remove file %s, error %s: %s \n" %(file_path, type(e), e) 
  
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
        raise JobServerError('Error isUserJob jobid=%s, userid=%s. Error %s: %s \n' %(repr(job_id), repr(user_id), type(e), e)) 
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
        raise JobServerError('Error getJobs %s: %s \n' %(type(e), e)) 
          
      cursor.close()
      connection.close()
      return job_ids
    
    
    
  def getDrmaaJobId(self, job_id):
    '''
    Returns the DRMAA job id associated with the job.
    The job_id must be valid.
    
    @type job_id: C{JobIdentifier}
    @rtype: string
    @return: DRMAA job identifier (job identifier on DRMS if submitted via DRMAA)
    '''
    with self.__lock:
      connection = self.__connect()
      cursor = connection.cursor()
      try:
        drmaa_id = cursor.execute('SELECT drmaa_id FROM jobs WHERE id=?', [job_id]).next()[0] #supposes that the job_id is valid
      except Exception, e:
        cursor.close()
        connection.close()
        raise JobServerError('Error getDrmaaJobId %s: %s \n' %(type(e), e)) 
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
        raise JobServerError('Error %s: %s \n' %(type(e), e)) 
      cursor.close()
      connection.close()
    return result
  
  def areErrOutJoined(self, job_id):
    '''
    Tells if the standard error and output are joined in the same file.
    The job_id must be valid.
    
    @type job_id: C{JobIdentifier}
    @rtype: boolean
    @return: value of join_errout 
    '''
    with self.__lock:
      connection = self.__connect()
      cursor = connection.cursor()
      try:
        join_errout = cursor.execute('SELECT join_errout FROM jobs WHERE id=?', [job_id]).next()[0]#supposes that the job_id is valid
      except Exception, e:
        cursor.close()
        connection.close()
        raise JobServerError('Error areErrOutJoined %s: %s \n' %(type(e), e)) 
      cursor.close()
      connection.close()
    return join_errout
  

  def getJobStatus(self, job_id):
    '''
    Returns the job status sored in the database (updated by L{DrmaaJobScheduler}).
    The job_id must be valid.
    '''
    with self.__lock:
      connection = self.__connect()
      cursor = connection.cursor()
      try:
        status = cursor.execute('SELECT status FROM jobs WHERE id=?', [job_id]).next()[0]#supposes that the job_id is valid
      except Exception, e:
        cursor.close()
        connection.close()
        raise JobServerError('Error getJobStatus %s: %s \n' %(type(e), e)) 
      status = status.encode('utf-8')
      cursor.close()
      connection.close()
    return status
  

  def getExitInformation(self, job_id):
    '''
    Returns the job exit information stored in the database (by L{DrmaaJobScheduler}).
    The job_id must be valid.
    '''
    with self.__lock:
      connection = self.__connect()
      cursor = connection.cursor()
      try:
        exit_st, exit_v, term_sig, rusage = cursor.execute('''SELECT  
                                              exit_status, 
                                              exit_value,    
                                              terminating_signal, 
                                              resource_usage 
                                    FROM jobs WHERE id=?''', [job_id]).next()#supposes that the job_id is valid
      except Exception, e:
        cursor.close()
        connection.close()
        raise JobServerError('Error getExitInformation %s: %s \n' %(type(e), e)) 
      cursor.close()
      connection.close()
    
    exit_st = exit_st.encode('utf-8')
    if term_sig: term_sig = term_sig.encode('utf-8') 
    if rusage: rusage = rusage.encode('utf-8')
    return (exit_st, exit_v, term_sig, rusage)


  def getGeneralInformation(self, job_id):
    '''
    Returns the job general information stored in the database,
    that is: name/description, command and submission date
    The job_id must be valid.
    '''
    with self.__lock:
      connection = self.__connect()
      cursor = connection.cursor()
      try:
        desc, cmd, sub_date = cursor.execute('''SELECT  
                                              name_description, 
                                              command,    
                                              submission_date
                                    FROM jobs WHERE id=?''', [job_id]).next()#supposes that the job_id is valid
      except Exception, e:
        cursor.close()
        connection.close()
        raise JobServerError('Error getGeneralInformation %s: %s \n' %(type(e), e)) 
      cursor.close()
      connection.close()
      
    if desc: desc = desc.encode('utf-8')
    if cmd: cmd = cmd.encode('utf-8') 
    if sub_date: sub_date = sub_date.encode('utf-8') 
    return (desc, cmd, sub_date)





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
        raise JobServerError('Error isUserTransfer %s: %s \n' %(type(e), e)) 
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
        raise JobServerError('Error getTransfers %s: %s \n' %(type(e), e)) 
      cursor.close()
      connection.close()
    return local_file_paths
    
    

  def getTransferInformation(self, local_file_path):
    '''
    Returns the information related to the transfer associated to the local file path.
    The local_file_path must be associated to a transfer.
    
    @type local_file_path: string
    @rtype: tuple
    @returns: (local_file_path, remote_file_path, expiration_date)
    '''
    with self.__lock:
      connection = self.__connect()
      cursor = connection.cursor()
      try:
        info = cursor.execute('SELECT local_file_path, remote_file_path, expiration_date FROM transfers WHERE local_file_path=?', [local_file_path]).next() #supposes that the local_file_path is associated to a transfer
      except Exception, e:
        cursor.close()
        connection.close()
        raise JobServerError('Error getTransferInformation %s: %s \n' %(type(e), e)) 
      result = (info[0].encode('utf-8'), info[1].encode('utf-8'), info[2].encode('utf-8'))
      cursor.close()
      connection.close()
    return result