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


from soma.jobs.drmaaJobScheduler import DrmaaJobScheduler
from soma.jobs.jobServer import JobServer
from soma.pyro import ThreadSafeProxy
import Pyro.naming, Pyro.core
from Pyro.errors import NamingError
from datetime import date
from datetime import timedelta
import pwd
import os



class JobScheduler( object ):
  '''
  Instances of this class give access to a set of machines linked together
  via a distributed resource management system (DRMS) like Condor, SGE, LSF,
  etc. A L{JobScheduler} allows to submit, monitor, control, and manage $
  information related to submitted jobs: id, author and associated files 
  (stdin, stdout, stderr, input and output files) for example.
  The use of L{JobScheduler} requires a L{JobServer} to be available.
  Job submissions and file transfers are registered on the server l{JobServer}. 
  Job information and temporary files are automatically disposed after a 
  timeout which is set a priori by the user. The user can also dispose the jobs 
  himself calling the L{dispose} or L{cancelTransfer} methods. 
 
  In case of the implementation of L{JobScheduler} using the DRMAA API: 
  JobScheduler must be created on one of the machines which is allowed to 
  submit jobs by the DRMS.
  '''
  def __init__( self ):
    '''
    Opens a connection to the pool of machines and to the data server L{JobServer}.
    In case of the implementation using the DRMAA API: A L{JobScheduler} instance 
    can only be created from a machine that is allowed to submit jobs by the 
    underlying DRMS.
    '''
    
    self.__drmaaJS = DrmaaJobScheduler()
    
    
    Pyro.core.initClient()
    locator = Pyro.naming.NameServerLocator()
    ns = locator.getNS(host='is143016') 
  
    try:
        URI=ns.resolve('JobServer')
        print 'URI:',URI
    except NamingError,x:
        print 'Couldn\'t find JobServer, nameserver says:',x
        raise SystemExit
    
    self.__jobServer= Pyro.core.getProxyForURI( URI )#ThreadSafeProxy( Pyro.core.getProxyForURI( URI ) )
    
    userLogin = pwd.getpwuid(os.getuid())[0]
    self.__user_id = self.__jobServer.registerUser(userLogin)
   
    self.__fileToRead = None
    self.__fileToWrite = None
    
    

  def __del__( self ):
    '''
    Closes the connection with the pool and the data server L{JobServer}. 
    It doesn't have any impact on the submitted jobs or file transfer. 
    Job and transfer information remains stored on the data server.
    '''
  

  ########## FILE TRANSFER ###############################################
  
  '''
  The main purpose of file transfer is the submission of job from a remote 
  machine. However it can also be used by user who has access to directory 
  shared by the machine of the pool, to make sure that all the machine of 
  the pool will have access to his files and take advantage of the file life 
  management services.
  
  For the following methods:
    Local means that it is located on a directory shared by the machine of the pool
    Remote means that it is located on a remote machine or on any directory 
    owned by the user. 
    A transfer will associate remote file path to unique local file path.
  
  Use L{registerTransfer} then L{writeLine} or scp or 
  shutil.copy to tranfer input file from the remote to the local 
  environment.
  Use L{registerTransfer} and once the job has run use L{readline} or scp or
  shutil.copy to transfer the output file from the local to the remote environment.
  
  
  '''

  def registerTransfer(self, remote_file_path, disposal_timeout=168): 
    '''
    An unique local path is generated and associated with the remote path. 
    When the disposal timout will be past, and no exisiting job will 
    declare using the file as input, the files will be disposed. 
    
    @type  remote_file_path: string
    @param remote_file_path: remote path of file
    @type  disposalTimeout: int
    @param disposalTimeout: Number of hours before each local file is considered 
    to have been forgotten by the user. Passed that delay, and if no existing job 
    declares using the file as input, the local file and information 
    related to the transfer are disposed. 
    Default delay is 168 hours (7 days).
    @rtype: string or sequence of string
    @return: local file path associated with the remote file
    '''
      
    local_input_file_path = self.__jobServer.generateLocalFilePath(self.__user_id, remote_file_path)
    expirationDate = date.today() + timedelta(hours=disposal_timeout) 
    self.__jobServer.addTransfer(local_input_file_path, remote_file_path, expirationDate, self.__user_id)
    return local_input_file_path


  '''
  Transfer of input files
  
  Example :
     
  local_infile_path = jobSchedulerProxy.registerTransfer(remote_infile_path)
  infile = open(remote_infile_path)
  line = readline(infile)
  while line:
      jobSchedulerProxy.writeLine(line, local_infile_path)
      line = readline(infile)
  
  => For larger files its possible to use scp (faster transfer but a connection is 
  needed):
  
  import pexpect
  local_infile_path = jobSchedulerProxy.registerTransfer(remote_infile_path)
  command = "scp %s %s@%s:%s" %(remote_input_file,
                                login,
                                server_address,
                                local_infile_path)
  child = pexpect.spawn(command) 
  child.expect('.ssword:*')
  child.sendline(password)
  time.sleep(2)
  
  => For files on the local file system:
  
  import shutil
  local_infile_path = jobScheduler.registerTransfer(remote_infile_path)
  shutil.copy(remote_infile_path,local_infile_path)
  '''

  def writeLine(self, line, local_file_path):
    '''
    Write a line to the local file. The path of the local input file
    must have been generated using the L{beginTransferInputFile} method.
    
    @type  line: string
    @param line: line to write in the local input file
    @type  local_file_path: string
    @param local_file_path: local file path to fill up
    '''
    
    # TBI if not a transfer raise exception.
    
    if not self.__fileToWrite or not self.__fileToWrite.name == local_file_path:
      self.__fileToWrite = open(local_file_path, 'wt')
   
    self.__fileToWrite.write(line)
   
  '''
  Transfer of output files
  
  Example :
 
  local_outfile_path =  registerTransfer(remote_outfile_path, 168)
  # job submission
  # when the job has run:
  ofile = open(remote_outfile_path, "w")
  line = jobSchedulerProxy.readline(local_outfile_path)
  while line
      outfile.write(line)
      line = jobSchedulerProxy.readline(local_outfile_path)
 
  => For larger files its possible to use scp (faster transfer but a potentially 
  slow new connection is needed):
  
  import pexpect
  local_outfile = registerTransfer(remote_outfile_path, 168)
  # job submission
  # when the job has run:
  command = "scp %s@%s:%s %s" %(login,
                                server_address,
                                local_outfile_path,
                                remote_outfile_path)
  child = pexpect.spawn(command) 
  child.expect('.ssword:*')
  child.sendline(password)
  time.sleep(2)
  
  
  => For files on the local file system:
  import shutil
  local_outfile_path =  registerTransfer(remote_outfile_path, 168)
  # job submission
  # when the job has run:
  shutil.copy(remote_infile_path,local_infile_path)
  '''
  
  def readline(self, local_file_path):
    '''
    Read a line from the local file. The path of the local input file
    must have been generated using the L{beginTransferInputFile} method.
    
    @type  line: string
    @param line: line to write in the local input file
    @type: string
    @param: local file path to fill up
    '''
    # TBI if not a transfer raise exception.
    
    if not self.__fileToRead or not self.__fileToRead.name == local_file_path:
      self.__fileToRead = open(local_file_path, 'rt')
      
    return self.__fileToRead.readline()

  

  def cancelTransfer(self, local_file_path):
    '''
    Delete the specified local file unless a job has declared to use it as input 
    or output. In the former case, the file will only be deleted after all its 
    associated jobs are disposed. (set its disposal date to now).
    
    @type local_file_path: string
    @param local_file_path: local file path associated with a transfer (ie 
    belongs to the list returned by L{getTransfers}    
    '''
    
    if not(self.__jobServer.isUserTransfer(local_file_path, self.__user_id)):
      # raise TBI
      print('Error: the transfer is owned by a different user \n')
      return

    self.__jobServer.removeTransferASAP(local_file_path)
    

  ########## JOB SUBMISSION ##################################################

  '''
  L{submit}, L{customSubmit} and L{submitWithTransfer} methods submit a 
  job for execution to the DRMS. A job identifier is returned. 
  This private structure must be used to inspect and control the job via 
  L{JobScheduler} methods.
  
  Example::
    from soma.jobs import jobScheduler
      
    jobScheduler = JobScheduler()
    job_id = jobScheduler.submit( ( 'python', '/somewhere/something.py' ), stdout=True, stderr='stdout' )
    jobScheduler.wait( job_id )
    file = jobScheduler.jobStdout( job_id )
    jobScheduler.dispose( job_id )
    for line in file:
      print line,
  '''

  def customSubmit( self,
                    command,
                    working_directory,
                    stdout_path,
                    stderr_path=None,
                    stdin=None,
                    disposal_timeout=168):
    '''
    Customized submission. All the files involved belong to the user and must 
    be specified. They are never disposed automatically and are not deleted when 
    using the L{kill} or L{dispose} methods.
    All the path must refer to shared files or directory on the pool.
    
    @type  command: sequence of string 
    @param command: The command to execute. Must constain at least one element.
    @type  working_directory: string
    @param working_directory: path to a directory where the job will be executed.
    @type  stdout_path: string
    @param stdout_path: the job's standard output will be directed to this file. 
    @type  stderr_path: string 
    @param stderr_path: the job's standard error will be directed to this file. If C{None}
    the error output stream will be stored in the same file as the standard output stream.
    @type  stdin: string
    @param stdin: job's standard input as a path to a file. C{None} if the 
    job doesn't require an input stream.
    @type  disposal_timeout: int
    @param disposal_timeout: Number of hours before the job is consider
    else:ed to have been 
      forgotten by the submitter. Passed that delay, the job is destroyed and its
      resources released as if the submitter had called L{kill} and L{dispose}.
      Default delay is 168 hours (7 days).
    @rtype:   C{JobIdentifier}
    @return:  the identifier of the submitted job 
    '''
    
    if len(command) == 0:
      #raise TBI
      print('Error: the command must contain at least one element \n')
      return

    job_id = self._drmaaJS.customSubmit(command,
                                        working_directory,
                                        stdout_path,
                                        stderr_path,
                                        stdin,
                                        disposal_timeout)

    return job_id
   


  def submit( self,
              command,
              working_directory=None,
              join_stderrout=False,
              stdin=None,
              disposal_timeout=168):
    '''
    Regular submission. If stdout and stderr are set to C{True}, the standard output 
    and error files are created on a directory shared by the machine of the pool. 
    These files will be deleted when the job will be disposed (after the disposal 
    timeout or when calling the L{kill} and L{dispose} methods).  
    All the path must refer to shared files or directory on the pool.
    
    @type  command: sequence
    @param command: The command to execute
    @type  working_directory: string
    @param working_directory: path to a directory where the job will be executed.
    If C{None}, a default working directory will be used (its value depends on 
    the DRMS installed on the pool).
    @type  join_stderrout: bool
    @param join_stderrout: C{True}  if the standard error should be redirect in the 
    same file as the standard output.
    @type  stdin: string
    @param stdin: job's standard inpout as a path to a file. C{None} if the 
    job doesn't require an input stream.
    @type  disposal_timeout: int
    @param disposal_timeout: Number of hours before the job is considered to have been 
      forgotten by the submitter. Passed that delay, the job is destroyed and its
      resources released (including standard output and error files) as if the 
      user had called L{kill} and L{dispose}.
      Default delay is 168 hours (7 days).
    @rtype:   C{JobIdentifier} 
    @return:  the identifier of the submitted job
    '''
    
    if len(command) == 0:
      #raise TBI
      print('Error: the command must contain at least one element \n')
      return
    
    job_id = self._drmaaJS.submit(command,
                                  working_directory,
                                  join_stderrout,
                                  stdin,
                                  disposal_timeout)
                          
    return job_id 



  def submitWithTransfer( self,
                          command,
                          required_local_input_files,
                          required_local_output_file,
                          join_stderrout=False,
                          stdin=None,
                          disposal_timeout=168):
    '''
    Submission with file transfer (well suited for remote submission). Submission 
    of a job for which all input files (stdin and input files) were already copied 
    to the pool shared directory. A local path
    for output file were also obtained via the L{registerTransfer} method.
    The list of involved local input and output file must be specified here to 
    guarantee that the files will exist during the whole job life. 
    All the path must refer to shared files or directory on the pool.
    
    @type  command: sequence
    @param command: The command to execute
    @type  required_local_input_file: sequence of string
    @param required_local_input_file: local files which are required for the job to run 
    @type  required_local_output_file: sequence of string
    @param required_local_output_file: local files the job will created and filled
    @type  join_stderrout: bool
    @param join_stderrout: C{True}  if the standard error should be redirect in the 
    same file as the standard output.
    @type  stdin: string
    @param stdin: job's standard inpout as a path to a file. C{None} if the 
    job doesn't require an input stream.
    @type  disposalTimeout: int
    @param disposalTimeout: Number of hours before the job is considered to have been 
      forgotten by the submitter. Passed that delay, the job is destroyed and its
      resources released as if the submitter had called L{kill} and L{dispose}.
      Default delay is 168 hours (7 days). 
      The local files associated with the job won't be deleted unless their own 
      disposal timeout is past and no other existing job has declared to use them 
      as input or output.
    @rtype:   C{JobIdentifier}
    @return:  the identifier of the submitted job 
    ''' 

    if len(command) == 0:
      #raise TBI
      print('Error: the command must contain at least one element \n')
      return

    job_id = self.__drmaaJS.submitWithTransfer( command,
                                                required_local_input_files,
                                                required_local_output_file,
                                                join_stderrout,
                                                stdin,
                                                disposal_timeout)
    
    return job_id




  def dispose( self, job_id ):
    '''
    Frees all the resources allocated to the submitted job on the data server
    L{JobServer}. After this call, the C{job_id} becomes invalid and
    cannot be used anymore. 
    To avoid that jobs create non handled files, L{dispose} kills the job if 
    it's running.

    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{jobs} or the submission 
    methods L{submit}, L{customSubmit} or L{submitWithTransfer})
    '''
    
    if not(self.__jobServer.isUserJob(job_id, self.__user_id)) :
      #TBI raise ...
      print('Error: the job is owned by a different user \n')
      return 

    self.__drmaaJS.dispose(job_id)


  ########## SERVER STATE MONITORING ########################################


  def jobs(self):
    '''
    Returns the identifier of the submitted and not diposed jobs.

    @rtype:  sequence of C{JobIdentifier}
    @return: series of job identifiers
    '''
    
    return self.__jobServer.getJobs(self.__user_id)
    
  def getTransfers(self):
    '''
    Returns the transfers currently owned by the user as a sequece of local file path 
    returned by the L{registerTransfer} method. 
    
    @rtype: sequence of string
    @return: local_file_path : path of the file on the directory shared by the machines
    of the pool
    '''
 
    return self.__jobServer.getTransfers(self.__user_id)

    
  def getTransferInformation(self, local_file_path):
    '''
    The local_file_path must belong to the list of paths returned by L{getTransfers}.
    Returns the information related to the file transfer corresponding to the 
    local_file_path.

    @rtype: tuple (local_file_path, remote_file_path, expiration_date)
    @return:
        -local_file_path: path of the file on the directory shared by the machines
        of the pool
        -remote_file_path: path of the file on the remote machine 
        -expiration_date: after this date the local file will be deleted, unless an
        existing job has declared this file as output or input.
    '''
    #TBI raise an exception if local_file_path is not valid transfer??
    return self.__jobServer.getTransferInformation(local_file_path)
    
    
  #def getTransfers( self ):
    #'''
    #Returns the information related to the user's file transfers created via the 
    #L{registerTransfer} method

    #@rtype: sequence of tuple (local_file_path, remote_file_path, expiration_date)
    #@return: For each transfer
        #-local_file_path: path of the file on the directory shared by the machines
        #of the pool
	#-remote_file_path: path of the file on the remote machine 
	#-expiration_date: after this date the local file will be deleted, unless an
	#existing job has declared this file as output or input.
    #'''
    
    #result = []
    #for transfer in self.__jobServer.getTransfers(self.__user_id):
      #result.append(self.__jobServer.getTransferInformation(transfer))
    #return result

  
  ########### DRMS MONITORING ################################################

  def status( self, job_id ):
    '''
    Returns the status of a submitted job.
    
    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{submit} or L{jobs})
    @rtype:  C{JobStatus}
    @return: the status of the job. The possible values are UNDETERMINED, 
    QUEUED_ACTIVE, SYSTEM_ON_HOLD, USER_ON_HOLD, USER_SYSTEM_ON_HOLD, RUNNING,
    SYSTEM_SUSPENDED, USER_SUSPENDED, USER_SYSTEM_SUSPENDED, DONE, FAILED
    '''
    
    # TBI exception if job_id is not valid ??
    return self.__jobServer.getJobStatus(job_id)
        

  def returnValue( self, job_id ):
    '''
    Gives the value returned by the job if it has finished normally. In case
    of a job running a C program, this value is typically the one given to the
    C{exit()} system call.
    
    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{submit} or L{jobs})
    @rtype:  int or None
    @return: job exit value, it may be C{None} if the job is not finished or
    exited abnormally (for instance on a signal).
    '''
  
    # TBI exception if job_id is not valid ??
    return self.__jobServer.getReturnedValue(job_id)
    
   

  def output( self, job_id ):
    '''
    Opens a file containing what had been written on the job standard 
    output stream. It may return C{None} if the process is not terminated
    or if recording of standard output had not been requested by the 
    submission methods (L{customSubmit}, L{submit} or L{submitWithTransfer}).
    
    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{submit} or L{jobs})
    @rtype:  file object or None
    @return: file object containing the job's output.
    '''

    # doesn't work in the remote case !!! TBI properly !!!
    stdout_path, stderr_path = self.__jobServer.getStdOutErrFilePath(job_id)
    outputFile = open(stdout_path)
    return outputFile

  def errorOutput( self, job_id ):
    '''
    Opens a file containing what had been written on the job error 
    output stream. It may return C{None} if the process is not terminated,
    if recording of standard output had not been requested by L{submit} or
    L{submitWithTransfer}, or if the user has specified his own standard 
    output files using L{customSubmit}.
    
    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{submit} or L{jobs})
    @rtype:  file object or None
    @return: file object containing the job's error output.
    '''

    # doesn't work in the remote case !!! TBI properly !!!
    if self.__jobServer.areErrOutJoined(job_id):
      return None
    stdout_path, stderr_path = self.__jobServer.getStdOutErrFilePath(job_id)
    errorFile = open(stderr_path)
    return errorFile
    
    
  ########## JOB CONTROL VIA DRMS ########################################
  
  
  def wait( self, job_id ):
    '''
    Waits for all the specified jobs to finish execution or fail. 
    
    @type job_ids: set of C{JobIdentifier}
    @param job_ids: Set of jobs to wait for
    '''
    
    self.__drmaaJS.wait(job_id)

  def stop( self, job_id ):
    '''
    Temporarily stops the job until the method L{restart} is called. The job 
    is held if it was waiting in a queue and suspended if was running. 
    
    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{submit} or L{jobs})
    '''
    
    self.__drmaaJS.stop(job_id)
   
  
  
  def restart( self, job_id ):
    '''
    Restarts a job previously stopped by the L{stop} method.
    
    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{submit} or L{jobs})
    '''
    
    self.__drmaaJS.restart(job_id)


  def kill( self, job_id ):
    '''
    Definitely terminates a job execution. After a L{kill}, a job is still in
    the list returned by L{jobs} and it can still be inspected by methods like
    L{status} or L{output}. To completely erase a job, it is necessary to call
    the L{dispose} method.
    
    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{submit} or L{jobs})
    '''

    self.__drmaaJS.kill(job_id)
