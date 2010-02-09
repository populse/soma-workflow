'''
This module allows to submit jobs to predefined sets of machines
linked together via a distributed resource management systems (DRMS) like 
Condor, SGE, LSF, etc.
  - L{JobScheduler}
  - L{JobSchedulerProxy}

@author: Yann Cointepas
@author: Soizic Laguitton
@organization: U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''
__docformat__ = "epytext en"

class JobScheduler( object ):
  '''
  Instances of this class gives access to a set of machines linked together
  via a distributed resource management system (DRMS) like Condor, SGE, LSF,
  etc. A L{JobScheduler} allows to submit and control jobs. It must be used
  from one of the machines that is allowed to submit jobs by the DRMS.
  Implementation of L{JobScheduler} uses DRMAA API.
  '''
  def __init__( self ):
   '''
   Open a connection to the pool of machines allowing to submit and check jobs.
   A L{JobScheduler} instance can only be created from a machine that is 
   allowed to submit jobs by the underlying DRMS.
   '''

  def submit( self, command, stdout=False, stderr=False, close=False,
              oblivion=168 ):
    '''
    Submit a job for execution. The job is started as soon as possible. A
    job identifier is returned. This private structure must be used to inspect
    and control the job via L{JobScheduler} methods.
    
    Example::
      from soma.jobs import Pool
      
      pool = Pool()
      job_id = pool.submit( ( 'python', '/somewhere/something.py' ), stdout=True, stderr='stdout' )
      pool.wait( job_id )
      file = pool.jobStdout( job_id )
      pool.close( job_id )
      for line in file:
        print line,
    
    @type  command: sequence
    @param command: The command to execute
    @type  stdout: bool
    @param stdout: C{True} if the job's standard output stream must be recorded.
    @type  stderr: bool or string
    @param stderr: C{True} if the job's standard output stream must be recorded. 
      If c{'stdout'} is given, the error output stream will be stored in the 
      same file as the standard output stream.
    @type  close: bool
    @param close: If C{True}, the job is immediately closed (see L{close}) and 
      C{None} is returned.
    @type  oblivion: int
    @param oblivion: Number of hours before the job is considered to have been 
      forgotten by the submiter. Passed that delay, the job is destroyed and its
      resources released as if the submiter had called L{kill} and L{forget}.
      Default delay is 168 hours (7 days).
    @rtype:   C{JobIdentifier} or C{None}
    @return:  the identifier of the submited job or C{None} if C{fireAndForget}
      is C{True}
    '''
  

  def status( self, job_id ):
    '''
    Return the status of a submited job.
    
    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{submit} or L{jobs})
    @rtype:  C{JobStatus}
    @return: the status of the job.
    '''
  
  def jobs( self, all=False ):
    '''
    Return the identifier of submited jobs that are not yet closed.
    
    @type  all: bool
    @param all: If C{all=False} (the default), only the jobs submited by the
      curent user are returned. If C{all=True} all jobs are returned.
    @rtype:  sequence of C{JobIdentifier}
    @return: series of jobs identifier
    '''
  

  def returnValue( self, job_id ):
    '''
    The value returned by the job if it exited normally. In the case of a
    job running a C program, this value is typically the one given to the
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
    or if recording of standard output had not been requested by L{submit}.
    
    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{submit} or L{jobs})
    @rtype:  file object or None
    @return: file object containing the job's output.
    '''

  def errorOutput( self, job_id ):
    '''
    Opens a file containing what had been written on the job error 
    output stream. It may return C{None} if the process is not terminated
    or if recording of standard output had not been requested by L{submit}.
    
    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{submit} or L{jobs})
    @rtype:  file object or None
    @return: file object containing the job's error output.
    '''


  def stop( self, job_id ):
    '''
    Temporarily stops the job execution. The execution of the job is stopped
    until L{resume} is called.
    
    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{submit} or L{jobs})
    '''
  

  def suspend( self, job_id ):
    '''
    Suspend the execution of a job that is waiting in the scheduling
    queue.
    
    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{submit} or L{jobs})
    '''


  def resume( self, job_id ):
    '''
    Restart a job previously stopped by L{stop} or L{suspend}.
    
    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{submit} or L{jobs})
    '''
  

  def kill( self, job_id ):
    '''
    Definitely terminate a job execution. After a L{kill}, a job is still in
    the list returned by L{jobs} and it can still be inspected by methods like
    L{status} or L{output}. To completely erase a job, it is necessary to call
    L{kill} and L{forget}.
    
    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{submit} or L{jobs})
    '''


  def forget( self, job_id ):
    '''
    Free all resources allocated to keep a link between the L{JobScheduler}
    and a submited job. After this call, the C{job_id} becomes invalid and
    cannot be used anymore. However, L{forget} does not kill the job if it
    is running (see L{kill}).
    
    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{submit} or L{jobs})
    '''


class JobSchedulerProxy( object ):
  '''
  This class creates a connection between an instance of L{JobScheduler} 
  running on a computer and a client program running on another computer. 
  The client machine is not controlled by a DRMS. An instance of 
  L{JobSchedulerProxy} has the same API as L{JobScheduler} with two more 
  methods allowing to upload and download files.
  '''
  def upload( self, local_file_name, distant_file_name ):
    '''
    '''
  
  
  def download( self, distant_file_name, local_file_name ):
    '''
    '''
  
  
  
