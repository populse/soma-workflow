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
import os

__docformat__ = "epytext en"


class JobServer ( object ):
  '''
  
  
  '''
  
  tmpFileDirPath="/neurospin/tmp/Soizic/jobFiles/"
  
  
  
  def __init__(self, database_file):
    '''
    The constructor gets as parameter the database information.
    
    @type  database_file: string
    @param database_file: the SQLite database file 
    '''
    
    self.database_file = database_file
    self.connection = connect(self.database_file) #Question: Is that better to do a connect in each JobServer method using the database ??
    
    
  def __del__(self):
    self.connection.close()
    
    
    
  
  def registerUser(self, login):
    '''
    Register a user so that he can submit job.
    
    @rtype: C{UserIdentifier}
    @return: user identifier
    '''
    
    cursor = self.connection.cursor()

    count = cursor.execute('SELECT count(*) FROM users WHERE login=?', [login]).next()[0]
    if count==0:
      cursor.execute('INSERT INTO users (login) VALUES (?)', [login])
      os.mkdir(JobServer.tmpFileDirPath + login)
    
    id = cursor.execute('SELECT id FROM users WHERE login=?', [login]).next()[0]
    
    self.connection.commit()
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
    cursor = self.connection.cursor()
    login = cursor.execute('SELECT login FROM users WHERE id=?',  [user_id]).next()[0]#supposes that the user_id is valid
    login = login.encode('utf-8')
    
    cursor.execute('INSERT INTO fileCounter (foo) VALUES (?)', [0])
    file_num = cursor.lastrowid
    
    newFilePath = JobServer.tmpFileDirPath + login + '/'
    if remote_file_path == None:
      newFilePath += repr(file_num)
    else:
      iextention = remote_file_path.rfind(".")
      if iextention == -1 :
        newFilePath += remote_file_path[remote_file_path.rfind("/")+1:] + '_' + repr(file_num) 
      else: 
        newFilePath += remote_file_path[remote_file_path.rfind("/")+1:iextention] + '_' + repr(file_num) + remote_file_path[iextention:]
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
    
    cursor = self.connection.cursor()
    cursor.execute('''INSERT INTO transfers 
                    (local_file_path, remote_file_path, transfer_date, expiration_date, user_id) VALUES (?, ?, ?, ?, ?)''',
                    (local_file_path, remote_file_path, date.today(), expiration_date, user_id))
    self.connection.commit()



  def removeTransferASAP(self, local_file_path):
    '''
    Set the expiration date of the transfer associated to the local file path 
    to today (yesterday?). That way it will be disposed as soon as no job will need it.
    
    @type  local_file_path: string
    @param local_file_path: local file path to identifying the transfer 
    record to delete.
    '''
    
    cursor = self.connection.cursor()
    yesterday = date.today() - timedelta(days=1)
    cursor.execute('UPDATE transfers SET expiration_date=? WHERE local_file_path=?', (yesterday, local_file_path))
    self.connection.commit()

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
    
    cursor = self.connection.cursor()
    cursor.execute('''INSERT INTO jobs 
                    (submission_date, user_id, expiration_date, stdout_file, stderr_file, join_errout, stdin_file, name_description, drmaa_id, working_directory) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                    (date.today(), 
                     user_id,
                     expiration_date,
                     stdout_file,
                     stderr_file,
                     join_stderrout,
                     stdin_file,
                     name_description,
                     drmaa_id, 
                     working_directory))
    self.connection.commit()
    id = cursor.lastrowid
    return id
   
    
  def registerInputs(self, job_id, local_input_files):
    '''
    Register associations between a job and input file path.
    
    @type job_id: C{JobIdentifier}
    @type  local_input_files: sequence of string
    @param local_input_files: local input file paths 
    '''
    
    cursor = self.connection.cursor()
    for file in local_input_files:
      cursor.execute('INSERT INTO ios (job_id, local_file_path, is_input) VALUES (?, ?, ?)',
                    (job_id, file, True))
                    
    self.connection.commit()
    
    
    
  def registerOutputs(self, job_id, local_output_files):
    '''
    Register associations between a job and output file path.
    
    @type job_id: C{JobIdentifier}
    @type  local_input_files: sequence of string
    @param local_input_files: local output file paths 
    '''
    cursor = self.connection.cursor()
    for file in local_output_files:
      cursor.execute('INSERT INTO ios (job_id, local_file_path, is_input) VALUES (?, ?, ?)',
                    (job_id, file, False))
                    
    self.connection.commit()



  def deleteJob(self, job_id):
    '''
    Remove the job from the database. Remove all associated transfered files if
    their expiration date passed and they are not used by any other job.
    
    @type job_id: 
    '''
    # set expiration date to yesterday + clean() ?
    
    cursor = self.connection.cursor()
    cursor2 = self.connection.cursor()
    
    yesterday = date.today() - timedelta(days=1)
    cursor.execute('UPDATE jobs SET expiration_date=? WHERE id=?', (yesterday, job_id))
    
    for row in cursor.execute('SELECT local_file_path FROM ios WHERE job_id=?', [job_id]):
      local_file_path, = row
      remote_file_path = cursor2.execute('SELECT remote_file_path FROM transfers WHERE local_file_path=?', [local_file_path]).next()[0] #supposes that all local_file_path of ios correspond to a transfer
      if remote_file_path == None :
        cursor2.execute('UPDATE transfers SET expiration_date=? WHERE local_file_path=?', (yesterday, local_file_path))
    
    self.connection.commit()
    self.clean()


  def clean(self) :
    '''
    Delete all expired jobs and transfers, except transfers which are requested 
    by valid job.
    '''
    
    cursor = self.connection.cursor()
    cursor2 = self.connection.cursor()
    
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
    
    self.connection.commit()
      
    
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
    
    cursor = self.connection.cursor()
    owner_id = cursor.execute('SELECT user_id FROM jobs WHERE id=?',  [job_id]).next()[0] # suppose that the job_id is valid
    return (owner_id==user_id)
  
  
  
  def getJobs(self, user_id):
    '''
    Returns the jobs owned by the user.
    
    @type user_id: C{UserIdentifier}
    @rtype: sequence of C{JobIdentifier}
    @returns: jobs owned by the user
    '''
   
    cursor = self.connection.cursor()
    job_ids = []
    for row in cursor.execute('SELECT id FROM jobs WHERE user_id=?', [user_id]):
      id, = row
      job_ids.append(id)
    return job_ids
    
    
    
  def getDrmaaJobId(self, job_id):
    '''
    Returns the DRMAA job id associated with the job.
    The job_id must be valid.
    
    @type job_id: C{JobIdentifier}
    @rtype: string
    @return: DRMAA job identifier (job identifier on DRMS if submitted via DRMAA)
    '''
    
    cursor = self.connection.cursor()
    drmaa_id = cursor.execute('SELECT drmaa_id FROM jobs WHERE id=?', [job_id]).next()[0] #supposes that the job_id is valid
    return drmaa_id
    
    
    
  def getStdOutErrFilePath(self, job_id):
    '''
    Returns the path of the standard output and error files.
    The job_id must be valid.
    
    @type job_id: C{JobIdentifier}
    @rtype: tuple
    @return: (stdout_file_path, stderr_file_path)
    '''

    cursor = self.connection.cursor()
    result = cursor.execute('SELECT stdout_file, stderr_file FROM jobs WHERE id=?', [job_id]).next()#supposes that the job_id is valid
    return result
  
  def areErrOutJoined(self, job_id):
    '''
    Tells if the standard error and output are joined in the same file.
    
    @type job_id: C{JobIdentifier}
    @rtype: boolean
    @return: value of join_errout 
    '''
    
    cursor = self.connection.cursor()
    join_errout = cursor.execute('SELECT join_errout FROM jobs WHERE id=?', [job_id]).next()[0]#supposes that the job_id is valid
    return join_errout
  
    

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
    
    cursor = self.connection.cursor()
    owner_id = cursor.execute('SELECT user_id FROM transfers WHERE local_file_path=?',  [local_file_path]).next()[0] #supposes that the local_file_path is associated to a transfer
    return (owner_id==user_id)



  def getTransfers(self, user_id):
    '''
    Returns the transfers owned by the user.
    
    @type user_id: C{UserIdentifier}
    @rtype: sequence of local file path
    @returns: local file path associated with a transfer owned by the user
    '''
    
    cursor = self.connection.cursor()
    local_file_paths = []
    for row in cursor.execute('SELECT local_file_path FROM transfers WHERE user_id=?', [user_id]):
      local_file, = row
      local_file_paths.append(local_file)
    return local_file_paths
    
    

  def getTransferInformation(self, local_file_path):
    '''
    Returns the information related to the transfer associated to the local file path.
    The local_file_path must be associated to a transfer.
    
    @type local_file_path: string
    @rtype: tuple
    @returns: (local_file_path, remote_file_path, expiration_date)
    '''

    cursor = self.connection.cursor()
    result = cursor.execute('SELECT local_file_path, remote_file_path, expiration_date FROM transfers WHERE local_file_path=?', [local_file_path]).next() #supposes that the local_file_path is associated to a transfer
    return result