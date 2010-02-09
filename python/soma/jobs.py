'''
This module allows to submit jobs to predefined sets of machines
linked together via a distributed resource management systems (DRMS) like 
Condor, SGE, LSF, etc.
  - L{Pool}
  - L{PoolProxy}

@author: Yann Cointepas
@organization: U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''
__docformat__ = "epytext en"

class Pool( object ):
  '''
  Instances of this class represents a set of machines linked together
  via a distributed resource management system (DRMS) like Condor, SGE, LSF,
  etc. A Pool allow to submit and control jobs from one of the machines in
  the pool (via DRMAA library)
  '''
  def __init__( self ):
   '''
   Open a connection to the pool allowing to submit and check jobs. A pool
   instance can only be created from submission machine in the underlying DRMS.
   '''

  def submit( self, command, stdout=False, stderr=False, close=False ):
    '''
    Submit a job in the pool. 
    
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
    @param stderr: C{True} if the job's standard output stream must be recorded. If c{'stdout'} is
      given, the error output stream will be stored in the same file as the standard output stream.
    @type  close: bool
    @param close: If C{True}, the job is immediately closed (see L{close}) and C{None} is returned.
    @rtype:   C{JobIdentifier} or C{None}
    @return:  the identifier of the submited job or C{None} if C{fireAndForget} is C{True}
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
    Return the identifier of jobs submited (and not yet closed) in the pool.
    
    @type  all: bool
    @param all: If C{all=False} (the default), only the jobs submited by the
      curent user are returned. If C{all=True} all jobs in the pool are returned.
    @rtype:  sequence of C{JobIdentifier}
    @return: series of jobs identifier
    '''
  

  def returnValue( self, job_id ):
    '''
    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{submit} or L{jobs})
    '''

  def output( self, job_id ):
    '''
    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{submit} or L{jobs})
    '''

  def errorOutput( self, job_id ):
    '''
    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{submit} or L{jobs})
    '''

  def stop( self, job_id ):
    '''
    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{submit} or L{jobs})
    '''
  
  def suspend( self, job_id ):
    '''
    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{submit} or L{jobs})
    '''

  def close( self, job_id ):
    '''
    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{submit} or L{jobs})
    '''


class PoolProxy( object ):
  '''
  Same API as L{Pool} with some specialization.
  '''

