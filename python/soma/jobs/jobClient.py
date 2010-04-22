'''
@author: Soizic Laguitton
@organization: U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''

__docformat__ = "epytext en"

''' 
Definitions:
'Local' refers to the hosts of the cluster where the jobs are submitted to. Eg: a local file can be reached by any host of the cluser and a local process runs on a cluster submitting host.
'Remote' refers to all hosts, processes or files which are not local.
'''


class Jobs(object):
  
  def __init__(self, resource_id = None, mode = None, login = None, password = None):
    '''
    @type  resource_id: C{ResourceIdentifier} or None
    @param resource_id: The name of the resource to use, eg: "NeuroSpin HiPiP" or "CCRT"... if None we suppose that the mode is 'local'.
    @type  mode: string or None
    @param mode: 
       - 'local': (default) if run on a submitting machine of the cluster
       - 'remote': if run from a machine whichis not a submitting machine 
       of the pool and doesn't share a file system with these machines
       - 'local_no_disconnection": used for debugging purpose only.
       - if None the mode will be inferred from the pool_name (never 
       local_no_disconnection)
    @param login and password: only required if mode is 'remote'
    '''
    
    #TBI, read the configuration files and infer from the ressource_id 
    #argument to find the mode
    #TBI read src_local_process from configuration file
    src_local_process = "/neurospin/tmp/Soizic/jobFiles/srcServers/localJobProcess.py"
    
    self.__mode = mode
    if self.__mode == 'local':
      from soma.jobs.connection import LocalFileTransfer, JobLocalConnection
      self.__connection = JobLocalConnection(src_local_process)
      self.__js_proxy = self.__connection.getJobScheduler()
      self.__file_transfer = LocalFileTransfer(self.__js_proxy)
    if self.__mode == 'remote':
      from soma.jobs.connection import RemoteFileTransfer, JobRemoteConnection
      self.__connection = JobRemoteConnection(login, password, src_local_process)
      self.__js_proxy = self.__connection.getJobScheduler()
      self.__file_transfer = RemoteFileTransfer(self.__js_proxy)
    if self.__mode == 'local_no_disconnection':
      from soma.jobs.connection import LocalFileTransfer
      from soma.jobs.jobScheduler import JobScheduler
      self.__js_proxy  = JobScheduler()
      self.__file_transfer = LocalFileTransfer(self.__js_proxy)
      self.__connection = None



  ########## FILE TRANSFER ###############################################
    
  '''
  The file transfer method must be used to submit jobs with the L{submitWithTransfer} 
  method.
  
  For example:
  #job remote input files: rfin_1, rfin_2, ..., rfin_n 
  #job remote output files: rfout_1, rfout_2, ..., rfout_m  
  
  #how to proceed for output files:
  lfout_1 = jobs.registerTransfer(rfout_1)
  lfout_2 = jobs.registerTransfer(rfout_2)
  ...
  lfout_n = jobs.registerTransfer(rfout_m)
  
  #how to procedd with input files:
  lfin_1= jobs.transferInputFile(rfin_1)
  lfin_2= jobs.transferInputFile(rfin_2)
  ...
  lfin_n= jobs.transferInputFile(rfin_n)
    
  #Job submittion: don't forget to specifies required local input and output files 
  job_id = jobs.submit(['python', '/somewhere/something.py'], 
                       [lfin_1, lfin_2, ..., lfin_n],
                       [lfout_1, lfout_2, ..., lfout_n])
  jobs.wait(job_id)
  
  #After Job execution, transfer back the output file
  jobs.transferOutputFile(lfout_1)
  jobs.transferOutputFile(lfout_2)
  ...
  jobs.transferOutputFile(lfout_m)
 
  '''
    
  def transferInputFile(self, remote_input_file, disposal_timeout = 168):
    '''
    Transfers a remote file, bound to be used as job input, to a local directory. 

    @type  remote_input_file: string 
    @param remote_input_file: remote path of input file
    @type  disposalTimeout: int
    @param disposalTimeout:  The local file and transfer information is 
    automatically disposed after disposal_timeout hours, except if a job 
    declare using the file as input. Default delay is 168 hours (7 days).
    @rtype: string 
    @return: local file path where the remote file was copied 
    '''
    return self.__file_transfer.transferInputFile(remote_input_file, disposal_timeout) 
    

  def registerTransfer(self, remote_file_path, disposal_timeout=168): 
    '''
    Generates a unique local path and save the (local_path, remote_path) association.
    
    @type  remote_file_path: string
    @param remote_file_path: remote path of file
    @type  disposalTimeout: int
    @param disposalTimeout: The local file and transfer information is 
    automatically disposed after disposal_timeout hours, except if a job 
    declare using the file as output or input. Default delay is 168 hours (7 days).
    @rtype: string or sequence of string
    @return: local file path associated with the remote file
    '''
    return self.__js_proxy.registerTransfer(remote_file_path, disposal_timeout)



  def transferOutputFile(self, local_file):
    '''
    Copies the local file to the associated remote file path. 
    The local file path must belong to the user's transfered files (ie belong to 
    the sequence returned by the L{getTransfers} method). 
    
    @type  local_file: string or sequence of string
    @param local_file: local file path(s) 
    '''
    self.__file_transfer.transferOutputFile(local_file)
  

  def cancelTransfer(self, local_file_path):
    '''
    Deletes the specified local file and the associated transfer information.
    If some jobs declares using the file, the transfer won't be deleted immediately
    but as soon as all the jobs will be disposed.
    
    @type local_file_path: string
    @param local_file_path: local file path associated with a transfer (ie 
    belongs to the list returned by L{getTransfers}    
    '''
    self.__js_proxy.cancelTransfer(local_file_path)
    

  ########## JOB SUBMISSION ##################################################

  '''
  L{submit}, L{customSubmit} and L{submitWithTransfer} methods submit a 
  job for execution to the cluster. A job identifier is returned. 
  This private structure must be used to inspect and control the job via 
  L{JobScheduler} methods.
  
  Example::
    from soma.jobs import jobClient
      
    jobs = jobClient.Jobs()
    job_id = jobs.submit( ['python', '/somewhere/something.py'] )
    jobs.stop(job_id)
    jobs.restart(job_id)
    jobs.wait([job_id])
    exitinfo = jobs.exitInformation(job_id)
    jobs.dispose(job_id)
  '''

  def submit( self,
              command,
              required_local_input_files=None,
              required_local_output_files=None,
              stdin=None,
              join_stderrout=False,
              disposal_timeout=168,
              name_description=None,
              stdout_path=None,
              stderr_path=None,
              working_directory=None):

    '''
    Submits a job for execution to the cluster. A job identifier is returned and 
    must be used to inspect and control the job.
    If the job used transfered files (L{transferInputFile} and L{registerTransfer} 
    methods) The list of involved local input and output file must be specified to 
    guarantee that the files will exist during the whole job life. 

    
    Every path must be local (ie reachable by the cluster machines) 

    @type  command: sequence of string 
    @param command: The command to execute. Must constain at least one element.
    
    @type  required_local_input_files: sequence of string
    @param required_local_input_files: list of all local input files obtained with the 
    L{transferInputFile} method which are required for the job to run. 
    
    @type  required_local_output_files: sequence of string
    @param required_local_output_files: list of all registered local files obtained 
    with the L{registerTransfer} method the job will use as output.
    
    @type  stdin: string
    @param stdin: job's standard input as a path to a file. C{None} if the 
    job doesn't require an input stream.
    
    @type  join_stderrout: bool
    @param join_stderrout: C{True}  if the standard error should be redirect in the 
    same file as the standard output.
   
    @type  disposal_timeout: int
    @param disposal_timeout: Number of hours before the job is considered to have been 
    forgotten by the submitter. Passed that delay, the job is destroyed and its
    resources released (including standard output and error files) as if the 
    user had called L{kill} and L{dispose}.
    Default delay is 168 hours (7 days).
    
    @type  name_description: string
    @param name_description: optional job name or description for user usage only
 
    @type  stdout_path: string
    @param stdout_path: this argument can be set to choose the file where the job's 
    standard output will be redirected. (optional)
    @type  stderr_path: string 
    @param stderr_path: this argument can be set to choose the file where the job's 
    standard error will be redirected (optional). It won't be used if the stdout_path argument is not set. 
    
    @type  working_directory: string
    @param working_directory: his argument can be set to choose the directory where 
    the job will be executed. (optional) 
 
    @rtype:   C{JobIdentifier}
    @return:  the identifier of the submitted job 

    '''

    job_id = self.__js_proxy.submit(command,
                                    required_local_input_files,
                                    required_local_output_files,
                                    stdin,
                                    join_stderrout,
                                    disposal_timeout,
                                    name_description,
                                    stdout_path,
                                    stderr_path,
                                    working_directory)
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
    
    self.__js_proxy.dispose(job_id)
    

  ########## MONITORING #############################################


  def jobs(self):
    '''
    Returns the identifier of the submitted and not diposed jobs.

    @rtype:  sequence of C{JobIdentifier}
    @return: series of job identifiers
    '''
    
    return self.__js_proxy.jobs()
    
  def getTransfers(self):
    '''
    Returns the transfers currently owned by the user as a sequece of local file path 
    returned by the L{registerTransfer} method. 
    
    @rtype: sequence of string
    @return: local_file_path : path of the file on the directory shared by the machines
    of the pool
    '''
 
    return self.__js_proxy.getTransfers()

    
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
    return self.__js_proxy.getTransferInformation(local_file_path)
   
  
  
  def status( self, job_id ):
    '''
    Returns the status of a submitted job.
    
    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{submit} or L{jobs})
    @rtype:  C{JobStatus} or None
    @return: the status of the job, if its valid and own by the current user, None 
    otherwise. UNDETERMINED, QUEUED_ACTIVE, SYSTEM_ON_HOLD, 
    USER_ON_HOLD, USER_SYSTEM_ON_HOLD, RUNNING, SYSTEM_SUSPENDED, USER_SUSPENDED,
    USER_SYSTEM_SUSPENDED, DONE or FAILED
    '''
    return self.__js_proxy.status(job_id)
        

  def exitInformation(self, job_id ):
    '''
    Gives the information related to the end of the job.
   
    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{submit} or L{jobs})
    @rtype:  tuple (exit_status, exit_value, term_signal, resource_usage) or None
    @return: It may be C{None} if the job is not valid. 
        - exit_status: EXIT_UNDETERMINED, EXIT_ABORTED (the job never ran), 
                       FINISHED_REGULARLY, FINISHED_TERM_SIG, 
                       FINISHED_UNCLEAR_CONDITIONS or USER_KILLED
        - exit_value: operating system exit code if the job terminated normally.
        - term_signal: representation of the signal that caused the termination of 
          the  job if the job terminated due to the receipt of a signal.
        - resource_usage: resource usage information as given by the cluser 
          distributed resource management system (DRMS).
    '''
    return self.__js_proxy.exitInformation(job_id)
    
 
  def generalInformation(self, job_id):
    '''
    Gives general information about the job: name/description, command and 
    submission date.
    
    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{submit} or L{jobs})
    @rtype: tuple or None
    @return: (name_description, command, submission_date) it may be C{None} if the job 
    is not valid. 
    '''
    return self.__js_proxy.generalInformation(job_id)
    


  def stdoutReadLine(self, job_id):
    '''
    Reads a line from the file where the job standard output stream is written.
    
    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{submit} or L{jobs})
    @rtype: string
    return: read line
    '''
    return self.__js_proxy.stdoutReadLine(job_id)

  

  def stderrReadLine(self, job_id):
    '''
    Reads a line from the file where the job standard error stream is written.
    
    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{submit} or L{jobs})
    @rtype: string
    return: read line
    '''
    return self.__js_proxy.stderrReadLine(job_id)

    
  ########## JOB CONTROL VIA DRMS ########################################
  
  
  def wait( self, job_ids, timeout = -1):
    '''
    Waits for all the specified jobs to finish execution or fail. 
    The job_id must be valid.
    
    @type  job_ids: set of C{JobIdentifier}
    @param job_ids: Set of jobs to wait for
    @type  timeout: int
    @param timeout: the call exits before timout seconds. a negative value 
    means to wait indefinetely for the result. 0 means to return immediately
    '''
    self.__js_proxy.wait(job_ids, timeout)

  def stop( self, job_id ):
    '''
    Temporarily stops the job until the method L{restart} is called. The job 
    is held if it was waiting in a queue and suspended if was running. 
    The job_id must be valid.
    
    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{submit} or L{jobs})
    '''
    self.__js_proxy.stop(job_id)
   
  
  
  def restart( self, job_id ):
    '''
    Restarts a job previously stopped by the L{stop} method.
    The job_id must be valid.
    
    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{submit} or L{jobs})
    '''
    self.__js_proxy.restart(job_id)


  def kill( self, job_id ):
    '''
    Definitely terminates a job execution. After a L{kill}, a job is still in
    the list returned by L{jobs} and it can still be inspected by methods like
    L{status} or L{output}. To completely erase a job, it is necessary to call
    the L{dispose} method.
    The job_id must be valid.
    
    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{submit} or L{jobs})
    '''
    self.__js_proxy.kill(job_id)

    
    
    