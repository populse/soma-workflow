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

   def __del__( self ):
     '''
     Closes the connection with the pool and the data server L{JobServer}. 
     It doesn't have any impact on the submitted jobs or file transfer. 
     Job and transfer information remains stored on the data server.
     '''


  ########## REGISTRATION ###############################################


  def jobs( self, all=False ):
    '''
    Returns the identifier of the submitted and not diposed jobs.

    @type  all: bool
    @param all: If C{all=False} (the default), only the jobs submitted by the
      current user are returned. If C{all=True} all jobs are returned.
    @rtype:  sequence of C{JobIdentifier}
    @return: series of job identifiers
    '''
    
  def transfers( self ):
    '''
    Returns the identifier of the current user's file transfers via the 
    L{createTransfer}.

    @rtype:  sequence of C{TransferIdentifier}
    @return: series of transfer identifiers 
    '''
  
  
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

  def cancelTransfer(self, transfer_id):
    '''
    Delete the files associated to the transfer on the directory shared by the 
    machines of the pool. The output files are deleted as well and the transfer 
    id becomes invalid.
    
    @type transfer_id: C{TransferIdentifier}
    @param transfer_id: The transfer identifier (returned by L{createTransfer})
    '''


  ########## FILE TRANSFER ###############################################
  
  def createTransfer( self,
                      remote_input_files, 
                      remote_output_files, 
                      remote_stdin = None,
                      disposalTimeout = 168 ):
    '''
    Transfers input files and saves the correspondences between files location on the 
    remote machine and their copy on a directory shared by the machine of the pool. 
    Return a transfer id. The correspondences are stored via the Job Server. 
    
    @type  remote_input_files: sequence
    @param remote_input_files: paths of input files on the remote machine
    @type  remote_output_files: sequence
    @param remote_output_files: paths of output files on the remote machine
    @type  disposalTimeout: int
    @param disposalTimeout: Number of hours before the files copied on the shared 
    directory is considered to have been forgotten by the submitter. Passed that delay, 
    all the files and information related to the transfer are disposed and the transfer 
    id become invalid (as if L{cancelTransfer} was called). 
    Default delay is 168 hours (7 days).
    @rtype: C{TransferIdentifier}
    @return: The transfer identifier.
    '''

  def getLocalInputFiles(self, transfer_id):
    '''
    Gives the paths of input files on the directory shared by the machines of the pool.
    The job must use exclusively these input files when running on the pool.
    
    @type  transfer_id: C{TransferIdentifier}
    @param transfer_id: The transfer identifier (returned by L{createTransfer})
    @rtype: sequence of string
    @return: paths of input files on the directory shared by the machines of the pool.
    '''

  def getLocalOutputFiles(self, transfer_id):
    '''
    Gives the paths of output files on the directory shared by the machines of the pool.
    The job must write exclusively these output files when running on the pool.
    
    @type  transfer_id: C{TransferIdentifier}
    @param transfer_id: The transfer identifier (returned by L{createTransfer})
    @rtype: sequence of string
    @return: paths of output files on the directory shared by the machines of the pool.
    '''
  
  
  def transferOutputFiles(self, transfer_id):
    '''
    Transfers the output files from the directory shared by the machine of the pool 
    to the remote machine. The method raises an exception if the file doesn't exist.
    Thus it must be used when the associated job is finished.
    
    @type transfer_id: C{TransferIdentifier}
    @param transfer_id: The transfer identifier (returned by L{createTransfer})
    '''


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
              workingDirectory,
              stdoutPath,
              stderrPath,
              jointStdErrOut=False,
              stdin=None,
              disposalTimeout=168):
    '''
    Customized submission. All the files involved belong to the user and must 
    be specified. They are never disposed automatically and are not deleted when 
    using the L{kill} or L{dispose} methods.
    All the path must refer to shared files or directory on the pool.
    
    @type  command: sequence
    @param command: The command to execute
    @type  workingDirectory: string
    @param workingDirectory: path to a directory where the job will be executed.
    @type  stdout: string
    @param stdout: the job's standard output will be directed to this file. 
    @type  stderr: string 
    @param stderr: the job's standard error will be directed to this file. 
    @type  stdin: string
    @param stdin: job's standard input as a path to a file. C{None} if the 
    job doesn't require an input stream.
    @type  disposalTimeout: int
    @param disposalTimeout: Number of hours before the job is considered to have been 
      forgotten by the submitter. Passed that delay, the job is destroyed and its
      resources released as if the submitter had called L{kill} and L{dispose}.
      Default delay is 168 hours (7 days).
    @rtype:   C{JobIdentifier}
    @return:  the identifier of the submitted job 
    '''

  def submit( self,
              command,
              workingDirectory=None,
              stdout=False, 
              stderr=False, 
              stdin=None,
              disposalTimeout=168):
    '''
    Regular submission. If stdout and stderr are set to C{True}, the standard output 
    and error files are created on a directory shared by the machine of the pool. 
    These files will be deleted when the job will be disposed (after the disposal 
    timeout or when calling the L{kill} and L{dispose} methods).  
    All the path must refer to shared files or directory on the pool.
    
    @type  command: sequence
    @param command: The command to execute
    @type  workingDirectory: string
    @param workingDirectory: path to a directory where the job will be executed.
    If C{None}, a default working directory will be used (its value depends on 
    the DRMS installed on the pool).
    @type  stdout: bool
    @param stdout: C{True} if the job's standard output stream must be recorded.
    @type  stderr: bool or string
    @param stderr: C{True} if the job's standard output stream must be recorded. 
      The error output stream will be stored in the same file as the standard 
      output stream.
    @type  stdin: string
    @param stdin: job's standard inpout as a path to a file. C{None} if the 
    job doesn't require an input stream.
    @type  disposalTimeout: int
    @param disposalTimeout: Number of hours before the job is considered to have been 
      forgotten by the submitter. Passed that delay, the job is destroyed and its
      resources released (including standard output and error files) as if the 
      user had called L{kill} and L{dispose}.
      Default delay is 168 hours (7 days).
    @rtype:   C{JobIdentifier} 
    @return:  the identifier of the submitted job
    '''


  def submitWithTransfer( self,
                          command,
                          transfer_id,
                          stdout=False, 
              	          stderr=False, 
                          disposalTimeout=168):
    '''
    Submission with file transfer (well suited for remote submission). Submission 
    of a job for which all needed files (stdin and input files) were already copied 
    to the pool shared directory using the L{createTransfer} method. Once the job has 
    run, the output files can be recovered using the L{transferOutputFiles} method.
    The life of files created on the pool shared directory correspond to the life of 
    the associated transfer_id (deleted automatically after a timeout or via the 
    L{cancelTransfer} method).
    
    @type  command: sequence
    @param command: The command to execute
    @type  transfer_id: C{TransferIdentifier}
    @param transfer_id: The transfer identifier (returned by L{createTransfer})
    @type  stdout: bool
    @param stdout: C{True} if the job's standard output stream must be recorded.
    @type  stderr: bool or string
    @param stderr: C{True} if the job's standard output stream must be recorded. 
      The error output stream will be stored in the same file as the standard output 
      stream.
    @type  disposalTimeout: int
    @param disposalTimeout: Number of hours before the job is considered to have been 
      forgotten by the submitter. Passed that delay, the job is destroyed and its
      resources released as if the submitter had called L{kill} and L{dispose}.
      Default delay is 168 hours (7 days). !!! The files associated to the transfer 
      (input, output, executable files) won't be deleted. !!!
    @rtype:   C{JobIdentifier}
    @return:  the identifier of the submitted job 
    ''' 

  
  ########### MONITORING ################################################

  def status( self, job_id ):
    '''
    Returns the status of a submitted job.
    
    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{submit} or L{jobs})
    @rtype:  C{JobStatus}
    @return: the status of the job.
    '''

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

  ########## CONTROL #################################################
  
  
  def wait( self, job_ids ):
    '''
    Waits for all the specified jobs to finish execution or fail. 
    
    @type job_ids: set of C{JobIdentifier}
    @param job_ids: Set of jobs to wait for
    '''

  def stop( self, job_id ):
    '''
    Temporarily stops the job until the method L{restart} is called. The job 
    is held if it was waiting in a queue and suspended if was running. 
    
    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{submit} or L{jobs})
    '''
  
  def restart( self, job_id ):
    '''
    Restarts a job previously stopped by the L{stop} method.
    
    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{submit} or L{jobs})
    '''

  def kill( self, job_id ):
    '''
    Definitely terminates a job execution. After a L{kill}, a job is still in
    the list returned by L{jobs} and it can still be inspected by methods like
    L{status} or L{output}. To completely erase a job, it is necessary to call
    the L{dispose} method.
    
    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{submit} or L{jobs})
    '''

