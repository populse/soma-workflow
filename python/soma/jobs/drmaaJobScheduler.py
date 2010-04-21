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
from __future__ import with_statement

__docformat__ = "epytext en"


from soma.pipeline.somadrmaajobssip import DrmaaJobs
from soma.jobs.jobServer import JobServer
import Pyro.naming, Pyro.core
from Pyro.errors import NamingError
from datetime import date
from datetime import timedelta
import pwd
import os
import threading
import time
import logging

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
    self.logger = logging.getLogger('ljp.drmaajs')
    
    self.__drmaa = DrmaaJobs()
    Pyro.core.initClient()
    locator = Pyro.naming.NameServerLocator()
    ns = locator.getNS(host='is143016')
  
    try:
        URI=ns.resolve('JobServer')
        self.logger.info('JobServer URI:'+ repr(URI))
    except NamingError,x:
        self.logger.critical('Couldn\'t find JobServer, nameserver says:',x)
        raise SystemExit
    
    self.__jobServer= Pyro.core.getProxyForURI( URI )
    
    try:
      userLogin = pwd.getpwuid(os.getuid())[0] 
    except Exception, e:
      self.logger.critical("Couldn't identify user %s: %s \n" %(type(e), e))
      raise SystemExit
    
    self.__user_id = self.__jobServer.registerUser(userLogin) 

    self.__jobs = set([])
    self.__drmaa_lock = threading.RLock()
    self.__jobs_lock = threading.RLock()
    
    self.__jobsEnded = False
    
    def startJobStatusUpdateLoop( self, interval ):
      logger_su = logging.getLogger('ljp.drmaajs.su')
      while True:
        # get rid of all the jobs that doesn't exist anymore
        serverJobs = self.__jobServer.getJobs(self.__user_id)
        with self.__jobs_lock:
          self.__jobs = self.__jobs.intersection(serverJobs)
        allJobsEnded = True
        ended = []
        with self.__jobs_lock:
          for job_id in self.__jobs:
            # get back the status from DRMAA
            status = self.__status(job_id)
            logger_su.debug("job " + repr(job_id) + " : " + status)
            if status == JobServer.DONE or status == JobServer.FAILED:
              # update the exit status and status on the job server 
              self.__endOfJob(job_id, status)
              ended.append(job_id)
            else:
              allJobsEnded = False
              # update the status on the job server 
              self.__jobServer.setJobStatus(job_id, status)
        self.__jobsEnded = allJobsEnded
        
        # get the exit information for terminated jobs and update the jobServer
        for job_id in ended:
          with self.__jobs_lock:
            self.__jobs.discard(job_id)
        logger_su.debug("---------- all jobs done : " + repr(self.__jobsEnded))
      
        time.sleep(interval)
   
    
    
    self.__job_status_thread = threading.Thread(name = "job_status_loop", 
                                                target = startJobStatusUpdateLoop, 
                                                args = (self, 1))
    self.__job_status_thread.setDaemon(True)
    self.__job_status_thread.start()


   

  def __del__( self ):
    pass
    '''
    Closes the connection with the pool and the data server L{JobServer} and
    stops updating the L{JobServer}. (should be called when all the jobs are
    done) 
    '''
 
    
  


  ########## JOB SUBMISSION #################################################

  def submit( self,
          command,
          required_local_input_files,
          required_local_output_files,
          stdin,
          join_stderrout,
          disposal_timeout,
          name_description,
          stdout_path,
          stderr_path,
          working_directory):
    
    '''
    Implementation of the L{JobScheduler} method.
    '''
    
    expiration_date = date.today() + timedelta(hours=disposal_timeout) 
    
    if not stdout_path:
      stdout_path = self.__jobServer.generateLocalFilePath(self.__user_id)
      stderr_path = self.__jobServer.generateLocalFilePath(self.__user_id)
      custom_submit = False #the std out and err file has to be removed with the job
    else:
      custom_submit = True #the std out and err file won't to be removed with the job
      
    with self.__drmaa_lock:
      drmaaJobTemplateId = self.__drmaa.allocateJobTemplate()
      self.__drmaa.setCommand(drmaaJobTemplateId, command[0], command[1:])
    
      self.__drmaa.setAttribute(drmaaJobTemplateId, "drmaa_output_path", "[void]:" + stdout_path)

      if join_stderrout:
        self.__drmaa.setAttribute(drmaaJobTemplateId,"drmaa_join_files", "y")
      else:
        if stderr_path:
          self.__drmaa.setAttribute(drmaaJobTemplateId, "drmaa_error_path", "[void]:" + stderr_path)
  
      if stdin:
        self.__drmaa.setAttribute(drmaaJobTemplateId, "drmaa_input_path", "[void]:" + stdin)
        
      if working_directory:
        self.__drmaa.setAttribute(drmaaJobTemplateId, "drmaa_wd", working_directory)
      
      drmaaSubmittedJobId = self.__drmaa.runJob(drmaaJobTemplateId)
      self.__drmaa.deleteJobTemplate(drmaaJobTemplateId)
    
    # for user information only
    command_info = ""
    for command_element in command:
      command_info = command_info + " " + command_element
    
    job_id = self.__jobServer.addJob(self.__user_id, 
                                      custom_submit,
                                      expiration_date, 
                                      stdout_path,
                                      stderr_path,
                                      join_stderrout,
                                      stdin, 
                                      name_description, 
                                      drmaaSubmittedJobId,
                                      None,
                                      command_info)
                                    
    if required_local_input_files:
      self.__jobServer.registerInputs(job_id, required_local_input_files)
    if required_local_output_files:
      self.__jobServer.registerOutputs(job_id, required_local_output_files)

    with self.__jobs_lock:
      self.__jobs.add(job_id)
    
    return job_id
   

  def dispose( self, job_id ):
    '''
    Implementation of the L{JobScheduler} method.
    '''
    
    self.logger.debug("Dispose job %s", job_id)
    drmaaJobId=self.__jobServer.getDrmaaJobId(job_id)
    
    with self.__drmaa_lock:
      self.__drmaa.terminate(drmaaJobId)
    with self.__jobs_lock:
      self.__jobs.discard(job_id)
    
    self.__jobServer.deleteJob(job_id)
    

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
    
    drmaaJobId = self.__jobServer.getDrmaaJobId(job_id)
    
    with self.__drmaa_lock:
      status = self.__drmaa.jobStatus(drmaaJobId) 
       #add conversion from DRMAA status strings to JobServer status strings if needed
      
    return status
   
     


  def __endOfJob(self, job_id, status):
    '''
    The method is called when the job status is JobServer.DONE or FAILED,
    to get the job exit inforation from DRMAA and update the JobServer.
    The job_id is also remove form the job list.
    '''
    
    drmaaJobId = self.__jobServer.getDrmaaJobId(job_id)
    self.logger.debug("End of job %s, drmaaJobId = %s", job_id, drmaaJobId)
    
    with self.__drmaa_lock:
      exit_status, exit_value, term_sig, resource_usage = self.__drmaa.wait(drmaaJobId, 0)
      
    self.logger.debug("job %s, exit_status=%s exit_value =%d", job_id, exit_status, exit_value)
    
    str_rusage = ''
    for rusage in resource_usage:
      str_rusage = str_rusage + rusage + ' '
    
    self.__jobServer.setJobExitInfo(job_id, exit_status, exit_value, term_sig, str_rusage)
    self.__jobServer.setJobStatus(job_id, status)
    
    assert(self.__jobServer.getJobStatus(job_id) == status)


  def areJobsDone(self):
    return self.__jobsEnded
    
  ########## JOB CONTROL VIA DRMS ########################################
  
  
  #def wait( self, job_ids, timeout = -1):
    #'''
    #Implementation of the L{JobScheduler} method.
    #'''
    #drmaaJobIds = []
      
    #for jid in job_ids:
      #drmaaJobIds.append(self.__jobServer.getDrmaaJobId(jid))
    #with self.__drmaa_lock:
    #self.__drmaa.synchronize(drmaaJobIds, timeout)


  def stop( self, job_id ):
    '''
    Implementation of the L{JobScheduler} method.
    '''
    drmaaJobId = self.__jobServer.getDrmaaJobId(job_id)
    status = self.__status(job_id)
    if status==JobServer.RUNNING:
      with self.__drmaa_lock:
        self.__drmaa.suspend(drmaaJobId)
    if status==JobServer.QUEUED_ACTIVE:
      with self.__drmaa_lock:
        self.__drmaa.hold(drmaaJobId)

  
  def restart( self, job_id ):
    '''
    Implementation of the L{JobScheduler} method.
    '''
    
    drmaaJobId = self.__jobServer.getDrmaaJobId(job_id)
    status = self.__status(job_id)
    if status==JobServer.USER_SUSPENDED or status==JobServer.USER_SYSTEM_SUSPENDED:
      with self.__drmaa_lock:
        self.__drmaa.resume(drmaaJobId)
    if status==JobServer.USER_ON_HOLD or status==JobServer.USER_SYSTEM_ON_HOLD :
      with self.__drmaa_lock:
        self.__drmaa.release(drmaaJobId)

  


  def kill( self, job_id ):
    '''
    Implementation of the L{JobScheduler} method.
    '''
    
    drmaaJobId = self.__jobServer.getDrmaaJobId(job_id)
    with self.__drmaa_lock:
      self.__drmaa.terminate(drmaaJobId)
    self.__jobServer.setJobExitInfo(job_id, 
                                    JobServer.USER_KILLED,
                                    None,
                                    None,
                                    None)
    self.__jobServer.setJobStatus(job_id, JobServer.FAILED)
    with self.__jobs_lock:
      self.__jobs.discard(job_id)
      
