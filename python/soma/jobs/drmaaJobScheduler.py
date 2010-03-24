'''
The L{DrmaaJobScheduler} hold the DRMAA session and implement all the 
L{JobScheduler} functions related to DRMAA (job submission and job control).
A thread of L{DrmaaJobScheduler} updates constantly the job status stored in 
the L{JobServer}.

@author: Yann Cointepas
@author: Soizic Laguitton
@organization: U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''
__docformat__ = "epytext en"

from soma.pipeline.somadrmaajobssip import DrmaaJobs
from soma.jobs.jobServer import JobServer
from soma.pyro import ThreadSafeProxy
import Pyro.naming, Pyro.core
from Pyro.errors import NamingError
from datetime import date
from datetime import timedelta
import pwd
import os
import threading
import time

  


class DrmaaJobScheduler( object ):

  '''
  Instances of this class opens a DRMAA session and allows to submit and control 
  the jobs. It updates constantly the jobs status on the L{JobServer}. 
  The L{DrmaaJobScheduler} must be created on one of the machine which is allowed
  to submit jobs by the DRMS.
  '''
  def __init__( self ):
    '''
    Opens a connection to the pool of machines and to the data server L{JobServer}.

    '''
    
    self.__drmaa = DrmaaJobs()
    Pyro.core.initClient()
    locator = Pyro.naming.NameServerLocator()
    ns = locator.getNS(host='is143016')
  
    try:
        URI=ns.resolve('JobServer')
        print 'URI:',URI
    except NamingError,x:
        print 'Couldn\'t find JobServer, nameserver says:',x
        raise SystemExit
    
    self.__jobServer= ThreadSafeProxy( Pyro.core.getProxyForURI( URI ) )
    
    userLogin = pwd.getpwuid(os.getuid())[0] 
    self.__user_id = self.__jobServer.registerUser(userLogin) 

    self.__jobs = set([])
    self.__lock = threading.RLock()
    
    def startJobStatusUpdateLoop( self, interval ):
      logfilepath = "/home/sl225510/statusUpdateThreadLog"
      if os.path.isfile(logfilepath):
        os.remove(logfilepath)
      logFile = open(logfilepath, "w")
      while True:
        print >> logFile, " "
        self.__lock.acquire()
        try:
          serverJobs = self.__jobServer.getJobs(self.__user_id)
          self.__jobs = self.__jobs.intersection(serverJobs)
          jobs = self.__jobs
          for job_id in jobs:
            status = self.__status(job_id)
            print >> logFile, "job " + repr(job_id) + " : " + status
            self.__jobServer.setJobStatus(job_id, status)
        finally:
          self.__lock.release()
        logFile.flush()
        time.sleep(interval)
    
    
    
    self.__job_status_thread = threading.Thread(name = "job_status_loop", 
                                                target = startJobStatusUpdateLoop, 
                                                args = (self, 1))
    self.__job_status_thread.daemon = True
    self.__job_status_thread.start()


   

  def __del__( self ):
    '''
    Closes the connection with the pool and the data server L{JobServer} and
    stops updating the L{JobServer}. (should be called when all the jobs are
    done) 
    '''
 
    
  ########## JOB STATUS UPDATE LOOP ########################################

 


  ########## JOB SUBMISSION #################################################

  def customSubmit( self,
                    command,
                    working_directory,
                    stdout_path,
                    stderr_path,
                    stdin,
                    disposal_timeout):
    '''
    Implementation of the L{JobScheduler} method.
    '''

    drmaaJobTemplateId = self.__drmaa.allocateJobTemplate()
    self.__drmaa.setCommand(drmaaJobTemplateId, command[0], command[1:])
  
    drmaa_stdout_arg = "[void]:" + stdout_path
    self.__drmaa.setAttribute(drmaaJobTemplateId, "drmaa_output_path", drmaa_stdout_arg)

    if stderr_path != None:
      drmaa_stderr_arg = "[void]:" + stderr_path
      self.__drmaa.setAttribute(drmaaJobTemplateId, "drmaa_error_path", drmaa_stderr_arg)
    else:
      self.__drmaa.setAttribute(drmaaJobTemplateId,"drmaa_join_files", "y")

    if stdin != None:
      drmaa_stdin_arg = "[void]:" + stdin
      self.__drmaa.setAttribute(drmaaJobTemplateId, "drmaa_input_path", drmaa_stdin_arg)
      
    self.__drmaa.setAttribute(drmaaJobTemplateId, "drmaa_wd", working_directory)
     
     
    drmaaSubmittedJobId = self.__drmaa.runJob(drmaaJobTemplateId)
    self.__drmaa.deleteJobTemplate(drmaaJobTemplateId)
    
    join_stderrout = (stderr_path == None)
    expiration_date = date.today() + timedelta(hours=disposal_timeout) 
    
    
    self.__lock.acquire()
    try: 
      job_id = self.__jobServer.addJob(self.__user_id, 
                                    expiration_date, 
                                    stdout_path,
                                    stderr_path,
                                    join_stderrout,
                                    stdin, 
                                    None,  # Name_description
                                    drmaaSubmittedJobId,
                                    working_directory)
      self.__jobs.add(job_id)
    finally:
      self.__lock.release()
  
    return job_id
  


  def submit( self,
              command,
              working_directory,
              join_stderrout,
              stdin,
              disposal_timeout):
    '''
    Implementation of the L{JobScheduler} method.
    '''
    
    expiration_date = date.today() + timedelta(hours=disposal_timeout) 
    
    self.__lock.acquire()
    try: 
      stdout_file = self.__jobServer.generateLocalFilePath(self.__user_id)
      stderr_file = self.__jobServer.generateLocalFilePath(self.__user_id)
      self.__jobServer.addTransfer(stdout_file, None, expiration_date, self.__user_id)
      self.__jobServer.addTransfer(stderr_file, None, expiration_date, self.__user_id)
    finally:
      self.__lock.release()
    
    drmaaJobTemplateId = self.__drmaa.allocateJobTemplate()
    self.__drmaa.setCommand(drmaaJobTemplateId, command[0], command[1:])
    
    self.__drmaa.setAttribute(drmaaJobTemplateId, "drmaa_output_path", "[void]:" + stdout_file)

    if join_stderrout:
      self.__drmaa.setAttribute(drmaaJobTemplateId,"drmaa_join_files", "y")
    else:
      self.__drmaa.setAttribute(drmaaJobTemplateId, "drmaa_error_path", "[void]:" + stderr_file)

    if stdin != None:
      self.__drmaa.setAttribute(drmaaJobTemplateId, "drmaa_input_path", "[void]:" + stdin)
    
    if working_directory != None:
      self.__drmaa.setAttribute(drmaaJobTemplateId, "drmaa_wd", working_directory)
    
    drmaaSubmittedJobId = self.__drmaa.runJob(drmaaJobTemplateId)
    self.__drmaa.deleteJobTemplate(drmaaJobTemplateId)
    
    join_stderrout = join_stderrout
    
    self.__lock.acquire()
    try:
    
      job_id = self.__jobServer.addJob(self.__user_id, 
                                   expiration_date, 
                                   stdout_file,
                                   stderr_file,
                                   join_stderrout,
                                   stdin, 
                                   None,  # Name_description
                                   drmaaSubmittedJobId,
                                   working_directory)
      self.__jobServer.registerOutputs(job_id, [stdout_file, stderr_file])
      self.__jobs.add(job_id)
    finally:
      self.__lock.release()
    
    
    return job_id 



  def submitWithTransfer( self,
                          command,
                          required_local_input_files,
                          required_local_output_file,
                          join_stderrout,
                          stdin,
                          disposal_timeout):
    '''
    Implementation of the L{JobScheduler} method.
    ''' 

    expiration_date = date.today() + timedelta(hours=disposal_timeout) 
    
    
    self.__lock.acquire()
    try:
      stdout_file = self.__jobServer.generateLocalFilePath(self.__user_id)
      stderr_file = self.__jobServer.generateLocalFilePath(self.__user_id)
      self.__jobServer.addTransfer(stdout_file, None, expiration_date, self.__user_id)
      self.__jobServer.addTransfer(stderr_file, None, expiration_date, self.__user_id)
    finally:
      self.__lock.release()
    
    drmaaJobTemplateId = self.__drmaa.allocateJobTemplate()
    self.__drmaa.setCommand(drmaaJobTemplateId, command[0], command[1:])
    
    self.__drmaa.setAttribute(drmaaJobTemplateId, "drmaa_output_path", "[void]:" + stdout_file)

    if join_stderrout:
      self.__drmaa.setAttribute(drmaaJobTemplateId,"drmaa_join_files", "y")
    else:
      self.__drmaa.setAttribute(drmaaJobTemplateId, "drmaa_error_path", "[void]:" + stderr_file)

    if stdin != None:
      self.__drmaa.setAttribute(drmaaJobTemplateId, "drmaa_input_path", "[void]:" + stdin)
    
    drmaaSubmittedJobId = self.__drmaa.runJob(drmaaJobTemplateId)
    self.__drmaa.deleteJobTemplate(drmaaJobTemplateId)
    
    
    self.__lock.acquire()
    try:
      job_id = self.__jobServer.addJob(self.__user_id, 
                                    expiration_date, 
                                    stdout_file,
                                    stderr_file,
                                    join_stderrout,
                                    stdin, 
                                    None,  # Name_description
                                    drmaaSubmittedJobId)
                                    
      self.__jobServer.registerOutputs(job_id, [stdout_file, stderr_file])
      self.__jobServer.registerInputs(job_id, required_local_input_files)
      self.__jobServer.registerOutputs(job_id, required_local_output_file)

      self.__jobs.add(job_id)
    finally:
      self.__lock.release()
    
    return job_id




  def dispose( self, job_id ):
    '''
    Implementation of the L{JobScheduler} method.
    '''
    self.__lock.acquire()
    try:
      jobStatus = self.__status(job_id)
      if jobStatus != JobServer.FAILED and jobStatus != JobServer.DONE and jobStatus != JobServer.UNDETERMINED :
        drmaaJobId=self.__jobServer.getDrmaaJobId(job_id)
        self.__drmaa.terminate(drmaaJobId)
        
      self.__jobServer.deleteJob(job_id)
      self.__jobs.discard(job_id)
    finally:
      self.__lock.release()
    
    

  ########### DRMS MONITORING ################################################




  def __status( self, job_id ):
    '''
    Returns the status of a submitted job. => add a converstion from DRMAA job 
    status strings to JobServer status ???
    
    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{submit} or L{jobs})
    @rtype:  C{JobStatus}
    @return: the status of the job. The possible values are UNDETERMINED, 
    QUEUED_ACTIVE, SYSTEM_ON_HOLD, USER_ON_HOLD, USER_SYSTEM_ON_HOLD, RUNNING,
    SYSTEM_SUSPENDED, USER_SUSPENDED, USER_SYSTEM_SUSPENDED, DONE, FAILED
    '''
    self.__lock.acquire()
    try:
      drmaaJobId = self.__jobServer.getDrmaaJobId(job_id)
    finally:
      self.__lock.release()
    return self.__drmaa.jobStatus(drmaaJobId) 
    #add conversion from DRMAA status strings to JobServer status strings if needed
    

  def __returnedValue( self, job_id ):
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
    self.__lock.acquire()
    try:
      drmaaJobId = self.__jobServer.getDrmaaJobId(job_id)
    finally:
      self.__lock.release()
    
    if self.__status(job_id)!=jobServer.DONE :
      return None
    #TBI

    
  ########## JOB CONTROL VIA DRMS ########################################
  
  
  def wait( self, job_id ):
    '''
    Implementation of the L{JobScheduler} method.
    '''
    self.__lock.acquire()
    try:
      drmaaJobId = self.__jobServer.getDrmaaJobId(job_id)
    finally:
      self.__lock.release()
    self.__drmaa.wait(drmaaJobId)


  def stop( self, job_id ):
    '''
    Implementation of the L{JobScheduler} method.
    '''
    self.__lock.acquire()
    try:
      drmaaJobId = self.__jobServer.getDrmaaJobId(job_id)
      if self.__status(job_id)==JobServer.RUNNING:
        self.__drmaa.suspend(drmaaJobId)
      else:
        self.__drmaa.hold(drmaaJobId)
    finally:
      self.__lock.release()
  
  
  def restart( self, job_id ):
    '''
    Implementation of the L{JobScheduler} method.
    '''
    self.__lock.acquire()
    try:
      drmaaJobId = self.__jobServer.getDrmaaJobId(job_id)
      if self.__status(job_id)==JobServer.USER_SUSPENDED:
        self.__drmaa.resume(drmaaJobId)
      else:
        self.__drmaa.release(drmaaJobId)
    finally:
      self.__lock.release()
  


  def kill( self, job_id ):
    '''
    Implementation of the L{JobScheduler} method.
    '''
    self.__lock.acquire()
    try:
      drmaaJobId = self.__jobServer.getDrmaaJobId(job_id)
      self.__drmaa.terminate(drmaaJobId)
    finally:
      self.__lock.release()
