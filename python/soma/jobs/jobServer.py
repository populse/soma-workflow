'''
The class L{JobServer} is the data server used by the L{JobScheduler} to 
save all information related to submitted jobs to predefined sets of machines
linked together via a distributed resource management systems (DRMS).


@author: Yann Cointepas
@author: Soizic Laguitton
@organization: U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''

from sqlite3 import *
from datetime import date
from datetime import timedelta
import soma.jobs.jobDatabase 

import os

__docformat__ = "epytext en"


class JobServer ( object ):
  '''
  
  
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
     
    if not os.path.isfile(database_file):
      soma.jobs.jobDatabase.create(database_file)
    
    #connection = connect(self.__database_file) # Pb with Pyro ??? #Question: Is that better to do a connect in each JobServer method using the database ??
 
    
    
  def __del__(self):
    #connection.close()
   pass 
    
    
  
  def registerUser(self, login):
    '''
    Register a user so that he can submit job.
    
    @rtype: C{UserIdentifier}
    @return: user identifier
    '''
    connection = connect(self.__database_file)
    cursor = connection.cursor()

    count = cursor.execute('SELECT count(*) FROM users WHERE login=?', [login]).next()[0]
    try:
      if count==0:
        cursor.execute('INSERT INTO users (login) VALUES (?)', [login])
        personal_path = self.__tmp_file_dir_path + "/" + login
        if os.path.isdir(personal_path):
          #TO DO remove everything in the existing personal_path?
          pass
        else:
          os.mkdir(personal_path)
        
      id = cursor.execute('SELECT id FROM users WHERE login=?', [login]).next()[0]
    except:
      connection.rollback()
    else:
      connection.commit()
    cursor.close()
    connection.close()
    return id
  
  
  
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
    connection = connect(self.__database_file)
    cursor = connection.cursor()
    login = cursor.execute('SELECT login FROM users WHERE id=?',  [user_id]).next()[0]#supposes that the user_id is valid
    login = login.encode('utf-8')
    
    cursor.execute('INSERT INTO fileCounter (foo) VALUES (?)', [0])
    file_num = cursor.lastrowid
    
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
    connection = connect(self.__database_file)
    cursor = connection.cursor()
    cursor.execute('''INSERT INTO transfers 
                    (local_file_path, remote_file_path, transfer_date, expiration_date, user_id) VALUES (?, ?, ?, ?, ?)''',
                    (local_file_path, remote_file_path, date.today(), expiration_date, user_id))
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
    connection = connect(self.__database_file)
    cursor = connection.cursor()
    yesterday = date.today() - timedelta(days=1)
    cursor.execute('UPDATE transfers SET expiration_date=? WHERE local_file_path=?', (yesterday, local_file_path))
    connection.commit()
    cursor.close()
    connection.close()
    self.clean()

  def addJob(self, 
               user_id,
               expiration_date,
               stdout_file,
               stderr_file,
               join_stderrout=False,
               stdin_file=None,
               name_description=None,
               drmaa_id=None,
               working_directory=None):
    '''
    Adds a job to the database and returns its identifier.
    
    @type user_id: C{UserIdentifier}
    @type expiration_date: date
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
    @rtype: C{JobIdentifier}
    @return: the identifier of the job
    '''
    connection = connect(self.__database_file)
    cursor = connection.cursor()
    cursor.execute('''INSERT INTO jobs 
                    (submission_date, user_id, expiration_date, stdout_file, stderr_file, join_errout, stdin_file, name_description, drmaa_id, working_directory, status) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                    (date.today(), 
                     user_id,
                     expiration_date,
                     stdout_file,
                     stderr_file,
                     join_stderrout,
                     stdin_file,
                     name_description,
                     drmaa_id, 
                     working_directory,
                     JobServer.UNDETERMINED))
    connection.commit()
    id = cursor.lastrowid
    cursor.close()
    connection.close()
    return id
   

  def setJobStatus(self, job_id, status):
    '''
    Updates the job status in the database.
    The status must be valid (ie a string among the job status 
    string defined in L{JobServer}.

    @type job_id: C{JobIdentifier}
    @type status: status string as defined in L{JobServer}
    '''

    # TBI if the status is not valid raise an exception ??
    connection = connect(self.__database_file)
    cursor = connection.cursor()
    try:
      cursor.execute('UPDATE jobs SET status=? WHERE id=?', (status, job_id))
    except:
      connection.rollback()
    else:
      connection.commit()
    cursor.close()
    connection.commit()
    connection.close()


  def registerInputs(self, job_id, local_input_files):
    '''
    Register associations between a job and input file path.
    
    @type job_id: C{JobIdentifier}
    @type  local_input_files: sequence of string
    @param local_input_files: local input file paths 
    '''
    connection = connect(self.__database_file)
    cursor = connection.cursor()
    for file in local_input_files:
      cursor.execute('INSERT INTO ios (job_id, local_file_path, is_input) VALUES (?, ?, ?)',
                    (job_id, file, True))
    
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
    connection = connect(self.__database_file)

    cursor = connection.cursor()
    for file in local_output_files:
      cursor.execute('INSERT INTO ios (job_id, local_file_path, is_input) VALUES (?, ?, ?)',
                    (job_id, file, False))
                    
    cursor.close()
    connection.commit()
    connection.close()


  def deleteJob(self, job_id):
    '''
    Remove the job from the database. Remove all associated transfered files if
    their expiration date passed and they are not used by any other job.
    
    @type job_id: 
    '''
    # set expiration date to yesterday + clean() ?
    connection = connect(self.__database_file)
    cursor = connection.cursor()
    cursor2 = connection.cursor()
    
    yesterday = date.today() - timedelta(days=1)
    cursor.execute('UPDATE jobs SET expiration_date=? WHERE id=?', (yesterday, job_id))
    
    for row in cursor.execute('SELECT local_file_path FROM ios WHERE job_id=?', [job_id]):
      local_file_path, = row
      remote_file_path = cursor2.execute('SELECT remote_file_path FROM transfers WHERE local_file_path=?', [local_file_path]).next()[0] #supposes that all local_file_path of ios correspond to a transfer
      if remote_file_path == None :
        cursor2.execute('UPDATE transfers SET expiration_date=? WHERE local_file_path=?', (yesterday, local_file_path))
    
    cursor.close()
    cursor2.close()
    connection.commit()
    connection.close()
    self.clean()


  def clean(self) :
    '''
    Delete all expired jobs and transfers, except transfers which are requested 
    by valid job.
    '''
    connection = connect(self.__database_file)
    cursor = connection.cursor()
    cursor2 = connection.cursor()
    
    for row in cursor.execute('SELECT id FROM jobs WHERE expiration_date < ?', [date.today()]):
      job_id, = row
      cursor2.execute('DELETE FROM ios WHERE job_id=?', [job_id])
    
    for row in cursor.execute('SELECT local_file_path FROM transfers WHERE expiration_date < ?', [date.today()]):
      local_file_path, = row
      count = cursor2.execute('SELECT count(*) FROM ios WHERE local_file_path=?', [local_file_path]).next()[0]
      if count == 0 :
        cursor2.execute('DELETE FROM transfers WHERE local_file_path=?', [local_file_path])
        if os.path.isfile(local_file_path):
          os.remove(local_file_path)
      
    cursor.execute('DELETE FROM jobs WHERE expiration_date < ?', [date.today()])
    
    cursor.close()
    cursor2.close()
    connection.commit()
    connection.close()
      
    
  ################### DATABASE QUERYING ##############################
  
  
  #JOBS
 
  def isUserJob(self, job_id, user_id):
    '''
    Check that a job is own by a user.
    The job_id must be valid.
    
    @type job_id: C{JobIdentifier}
    @type user_id: C{UserIdentifier}
    @rtype: bool
    @returns: the job is owned by the user
    '''
    connection = connect(self.__database_file)
    cursor = connection.cursor()
    owner_id = cursor.execute('SELECT user_id FROM jobs WHERE id=?',  [job_id]).next()[0] # suppose that the job_id is valid
    cursor.close()
    connection.close()
    return (owner_id==user_id)
  
  
  
  def getJobs(self, user_id):
    '''
    Returns the jobs owned by the user.
    
    @type user_id: C{UserIdentifier}
    @rtype: sequence of C{JobIdentifier}
    @returns: jobs owned by the user
    '''
    connection = connect(self.__database_file)
    cursor = connection.cursor()
    job_ids = []
    for row in cursor.execute('SELECT id FROM jobs WHERE user_id=?', [user_id]):
      id, = row
      job_ids.append(id)
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
    connection = connect(self.__database_file)
    cursor = connection.cursor()
    drmaa_id = cursor.execute('SELECT drmaa_id FROM jobs WHERE id=?', [job_id]).next()[0] #supposes that the job_id is valid
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
    connection = connect(self.__database_file)
    cursor = connection.cursor()
    result = cursor.execute('SELECT stdout_file, stderr_file FROM jobs WHERE id=?', [job_id]).next()#supposes that the job_id is valid
    cursor.close()
    connection.close()
    return result
  
  def areErrOutJoined(self, job_id):
    '''
    Tells if the standard error and output are joined in the same file.
    
    @type job_id: C{JobIdentifier}
    @rtype: boolean
    @return: value of join_errout 
    '''
    connection = connect(self.__database_file)
    cursor = connection.cursor()
    join_errout = cursor.execute('SELECT join_errout FROM jobs WHERE id=?', [job_id]).next()[0]#supposes that the job_id is valid
    cursor.close()
    connection.close()
    return join_errout
  

  def getJobStatus(self, job_id):
    '''
    Returns the job status sored in the database (updated by L{DrmaaJobScheduler}).
    '''
    connection = connect(self.__database_file)
    cursor = connection.cursor()
    status = cursor.execute('SELECT status FROM jobs WHERE id=?', [job_id]).next()[0]#supposes that the job_id is valid
    status = status.encode('utf-8')
    cursor.close()
    connection.close()
    return status
  

  def getReturnedValue(self, job_id):
    '''
    Returns the job returned value sored in the database (by L{DrmaaJobScheduler}).
    '''
    connection = connect(self.__database_file)
    cursor = connection.cursor()
    retured_value = cursor.execute('SELECT returned_value FROM jobs WHERE id=?', [job_id]).next()[0]#supposes that the job_id is valid
    cursor.close()
    connection.close()
    return status


  #TRANSFERS
  
  def isUserTransfer(self, local_file_path, user_id):
    '''
    Check that a local file path match with a transfer and that the transfer 
    is owned by the user.
    The local_file_path must be associated to a transfer.
    
    @type local_file_path: string
    @type user_id: C{UserIdentifier}
    @rtype: bool
    @returns: the local file path match with a transfer owned by the user
    '''
    connection = connect(self.__database_file)
    cursor = connection.cursor()
    owner_id = cursor.execute('SELECT user_id FROM transfers WHERE local_file_path=?',  [local_file_path]).next()[0] #supposes that the local_file_path is associated to a transfer
    cursor.close()
    connection.close()
    return (owner_id==user_id)



  def getTransfers(self, user_id):
    '''
    Returns the transfers owned by the user.
    
    @type user_id: C{UserIdentifier}
    @rtype: sequence of local file path
    @returns: local file path associated with a transfer owned by the user
    '''
    connection = connect(self.__database_file)
    cursor = connection.cursor()
    local_file_paths = []
    for row in cursor.execute('SELECT local_file_path FROM transfers WHERE user_id=?', [user_id]):
      local_file = row[0]
      local_file = local_file.encode('utf-8')
      local_file_paths.append(local_file)
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
    connection = connect(self.__database_file)
    cursor = connection.cursor()
    info = cursor.execute('SELECT local_file_path, remote_file_path, expiration_date FROM transfers WHERE local_file_path=?', [local_file_path]).next() #supposes that the local_file_path is associated to a transfer
    result = (info[0].encode('utf-8'), info[1].encode('utf-8'), info[2].encode('utf-8'))
    cursor.close()
    connection.close()
    return result