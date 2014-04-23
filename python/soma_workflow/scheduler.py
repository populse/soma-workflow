from __future__ import with_statement

'''
@author: Soizic Laguitton

@organization: I2BM, Neurospin, Gif-sur-Yvette, France
@organization: CATI, France
@organization: U{IFR 49<http://www.ifr49.org>}

@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''


import subprocess
import threading
import time
import logging
import os
import sys
import signal
import ctypes
import atexit
import os.path
import socket

import soma_workflow.constants as constants
from soma_workflow.errors import DRMError
from soma_workflow.configuration import LocalSchedulerCfg, Configuration
from soma_workflow.utils import DetectFindLib

_drmaa_lib_env_name = 'DRMAA_LIBRARY_PATH'

(DRMAA_LIB_FOUND,_lib)=DetectFindLib(_drmaa_lib_env_name,'drmaa')

if DRMAA_LIB_FOUND==True:
    from somadrmaa.errors import *
    from somadrmaa.const import JobControlAction


class Scheduler(object):
  '''
  Allow to submit, kill and get the status of jobs.
  '''
  parallel_job_submission_info = None
  
  logger = None

  is_sleeping = None

  def __init__(self):
    self.parallel_job_submission_info = None
    self.is_sleeping = False

  def sleep(self):
    self.is_sleeping = True

  def wake(self):
    self.is_sleeping = False

  def clean(self):
    pass

  def job_submission(self, job):
    '''
    * job *EngineJob*
    * return: *string*
        Job id for the scheduling system (DRMAA for example)
    '''
    raise Exception("Scheduler is an abstract class!")

  def get_job_status(self, scheduler_job_id):
    '''
    * scheduler_job_id *string*
        Job id for the scheduling system (DRMAA for example)
    * return: *string*
        Job status as defined in constants.JOB_STATUS
    '''
    raise Exception("Scheduler is an abstract class!")

  def get_job_exit_info(self, scheduler_job_id):
    '''
    * scheduler_job_id *string*
        Job id for the scheduling system (DRMAA for example)
    * return: *tuple*
        exit_status, exit_value, term_sig, resource_usage
    '''
    raise Exception("Scheduler is an abstract class!")

  def kill_job(self, scheduler_job_id):
    '''
    * scheduler_job_id *string*
        Job id for the scheduling system (DRMAA for example)
    '''
    raise Exception("Scheduler is an abstract class!")

if DRMAA_LIB_FOUND==True:

  class DrmaaCTypes(Scheduler):
        '''
        Scheduling using a Drmaa session. 
        Contains possible patch depending on the DRMAA impementation. 
        '''
        
        # DRMAA session. DrmaaJobs
        _drmaa = None
        # string
        _drmaa_implementation = None
        # DRMAA doesn't provide an unified way of submitting
        # parallel jobs. The value of parallel_job_submission is cluster dependant. 
        # The keys are:
        #      -Drmaa job template attributes 
        #      -parallel configuration name as defined in soma_workflow.constants
        # dict
        parallel_job_submission_info = None
        
        logger = None
        
        _configured_native_spec = None
  
        tmp_file_path = None
  
        is_sleeping = False
        FAKE_JOB = -167
  
          
        def __init__(self, 
                     drmaa_implementation, 
                     parallel_job_submission_info, 
                     tmp_file_path=None, 
                     configured_native_spec=None):
  
          import somadrmaa
  
          self.logger = logging.getLogger('ljp.drmaajs')
          
          self.wake()
   
          self.hostname = socket.gethostname()
      
          self._drmaa_implementation = drmaa_implementation
      
          self.parallel_job_submission_info = parallel_job_submission_info
      
          self._configured_native_spec = configured_native_spec 
      
          self.logger.debug("Parallel job submission info: %s", 
                            repr(parallel_job_submission_info))
  
          if tmp_file_path == None:
             self.tmp_file_path = os.path.abspath("tmp")
          else:
             self.tmp_file_path = os.path.abspath(tmp_file_path) 
       
        def clean(self):
          if self._drmaa_implementation == "PBS":
            tmp_out = os.path.join(self.tmp_file_path, "soma-workflow-empty-job-patch-torque.o")
            tmp_err = os.path.join(self.tmp_file_path, "soma-workflow-empty-job-patch-torque.e")
          
            #print "tmp_out="+tmp_out
            #print "tmp_err="+tmp_err        
  
            if os.path.isfile(tmp_out):
              os.remove(tmp_out)
            if os.path.isfile(tmp_err):
              os.remove(tmp_err) 
  
  
        def close_drmaa_session(self):
          if self._drmaa:
            self._drmaa.exit()
            self._drmaa=None
  
        def __del__(self):
          self.clean()
          self.close_drmaa_session()      
   
        def sleep(self):
          '''
          Some Drmaa sessions expire if they idle too long.  
          '''
          self.close_drmaa_session()
          self.is_sleeping = True
         
  
        def wake(self):
          '''
          Creates a fresh Drmaa session.
          '''
          import somadrmaa
  
          self.is_sleeping = False
          
          if not self._drmaa:
            self._drmaa=somadrmaa.Session()
            self._drmaa.initialize()
 
        def submit_simple_test_job(self, outstr,out_o_file,out_e_file):
          import somadrmaa
          # patch for the PBS-torque DRMAA implementation
          if self._drmaa_implementation == "PBS":
        
                '''
                Create a job to test
                '''
                jobTemplateId = self._drmaa.createJobTemplate()
                jobTemplateId.remoteCommand='echo'
                jobTemplateId.args = ["%s"%(outstr)]
                jobTemplateId.outputPath="%s:%s" %(self.hostname, os.path.join(self.tmp_file_path, "%s"%(out_o_file)))
                jobTemplateId.errorPath="%s:%s" %(self.hostname, os.path.join(self.tmp_file_path, "%s"%(out_e_file)))
    
                #print "jobTemplateId="+repr(jobTemplateId)
                #print "jobTemplateId.remoteCommand="+repr(jobTemplateId.remoteCommand)
                #print "jobTemplateId.args="+repr(jobTemplateId.args)
                #print "jobTemplateId.outputPath="+repr(jobTemplateId.outputPath)
                #print "jobTemplateId.errorPath="+repr(jobTemplateId.errorPath)
  
                jobid=self._drmaa.runJob(jobTemplateId)
                #print "jobid="+jobid
                retval = self._drmaa.wait(jobid, drmaa.Session.TIMEOUT_WAIT_FOREVER) 
                #print "retval="+repr(retval)
                self._drmaa.deleteJobTemplate(jobTemplateId) 
 

 
        def _setDrmaaParallelJob(self, 
                                 drmaa_job_template_id, 
                                 configuration_name, 
                                 max_num_node):
          '''
          Set the DRMAA job template information for a parallel job submission.
          The configuration file must provide the parallel job submission 
          information specific to the cluster in use. 
      
          @type  drmaa_job_template_id: string 
          @param drmaa_job_template_id: id of drmaa job template
          @type  parallel_job_info: tuple (string, int)
          @param parallel_job_info: (configuration_name, max_node_num)
          configuration_name: type of parallel job as defined in soma_workflow.constants 
          (eg MPI, OpenMP...)
          max_node_num: maximum node number the job requests (on a unique machine or 
          separated machine depending on the parallel configuration)
          ''' 
          if self.is_sleeping: self.wake()
          
          self.logger.debug(">> _setDrmaaParallelJob")
          cluster_specific_cfg_name = self.parallel_job_submission_info[configuration_name]
          
          for drmaa_attribute in constants.PARALLEL_DRMAA_ATTRIBUTES:
            value = self.parallel_job_submission_info.get(drmaa_attribute)
            if value: 
              value = value.replace("{config_name}", cluster_specific_cfg_name)
              value = value.replace("{max_node}", repr(max_num_node))
  
  
              setattr(drmaa_job_template_id,drmaa_attribute,value)
  
              self.logger.debug("Parallel job, drmaa attribute = %s, value = %s ",
                                drmaa_attribute, value) 
      
      
          job_env = []
          for parallel_env_v in constants.PARALLEL_JOB_ENV:
            value = self.parallel_job_submission_info.get(parallel_env_v)
            if value: job_env.append((parallel_env_v,value.rstrip()))
  
          drmaa_job_template_id.jobEnvironment=dict(job_env)
  
          self.logger.debug("Parallel job environment : " + repr(job_env)) 
          self.logger.debug("<< _setDrmaaParallelJob")
  
          return drmaa_job_template_id
     
     
        def job_submission(self, job):
          '''
          @type  job: soma_workflow.client.Job
          @param job: job to be submitted
          @rtype: string
          @return: drmaa job id 
          '''
  
          if self.is_sleeping: self.wake()
          # patch for the PBS-torque DRMAA implementation
          command = []
          if job.is_barrier:
            # barrier jobs don't actually go through DRMAA.
            self.logger.debug('job_submission, DRMAA - barrier job.')
            job.status = constants.DONE
            return self.FAKE_JOB

          job_command = job.plain_command()

          ## This is only for the old drmaa version
          ## Now it is not necessary anymore
          #if self._drmaa_implementation == "PBS":
          if False:
            if job_command[0] == 'python':
              job_command[0] = sys.executable
            for command_el in job_command:
              command_el = command_el.replace('"', '\\\"')
              command.append("\"" + command_el + "\"")
            self.logger.debug("PBS case, new command:" + repr(command))
          else:
            command = job_command


          self.logger.debug("command: " + repr(command))
          self.logger.debug("job.name=" + repr(job.name))

          stdout_file = job.plain_stdout()
          stderr_file = job.plain_stderr()
          stdin = job.plain_stdin()

          try:
            jobTemplateId = self._drmaa.createJobTemplate()
            jobTemplateId.remoteCommand=command[0]
            jobTemplateId.args = command[1:]
  
            self.logger.info("jobTemplateId="+repr(jobTemplateId)+" command[0]="+repr(command[0])+" command[1:]="+repr(command[1:]))
            self.logger.info("hostname and stdout_file= [%s]:%s" %(self.hostname, stdout_file))
       
            jobTemplateId.outputPath="%s:%s" %(self.hostname, stdout_file)
                      
        
            if job.join_stderrout:
              jobTemplateId.joinFiles="y" 
            else:
              if stderr_file:
                 jobTemplateId.errorPath="%s:%s" %(self.hostname, stderr_file)              
            
            if job.stdin:
              #self.logger.debug("stdin: " + repr(stdin))
              #self._drmaa.setAttribute(drmaaJobId, 
              #                        "drmaa_input_path", 
              #                        "%s:%s" %(self.hostname, stdin))
              self.logger.debug("stdin: " + repr(stdin))
              jobTemplateId.inputPath=stdin
  
       
            working_directory = job.plain_working_directory()
            if working_directory:
              jobTemplateId.workingDirectory=working_directory
  
            self.logger.debug("JOB NATIVE_SPEC " + repr(job.native_specification))
            self.logger.debug("CONFIGURED NATIVE SPEC " + repr(self._configured_native_spec))
            native_spec = None
  
            if job.native_specification:
              native_spec = job.native_specification
            elif self._configured_native_spec:
              native_spec = self._configured_native_spec
            
            if job.queue and native_spec:
              jobTemplateId.nativeSpecification="-q " + str(job.queue) + " " + str(native_spec)
              self.logger.debug("NATIVE specification " + "-q " + str(job.queue) + " " + str(native_spec))
            elif job.queue:
              jobTemplateId.nativeSpecification="-q " + str(job.queue)
              self.logger.debug("NATIVE specification " + "-q " + str(job.queue))
            elif native_spec:
              jobTemplateId.nativeSpecification=str(native_spec)
              self.logger.debug("NATIVE specification " + str(native_spec))
        
            
            if job.parallel_job_info :
              parallel_config_name, max_node_number = job.parallel_job_info
              jobTemplateId=self._setDrmaaParallelJob(jobTemplateId, 
                                        parallel_config_name, 
                                        max_node_number)
             
            if self._drmaa_implementation == "PBS": 
              job_env = []
              for var_name in os.environ.keys():
                #job_env.append(var_name+"="+os.environ[var_name])
                job_env.append((var_name,os.environ[var_name]))
              jobTemplateId.jobEnvironment=dict(job_env)
  
            self.logger.debug("before submit command: " + repr(command))
            self.logger.debug("before submit job.name=" + repr(job.name)) 
            drmaaSubmittedJobId=self._drmaa.runJob(jobTemplateId)
            self._drmaa.deleteJobTemplate(jobTemplateId) 
           
          except DrmaaException, e:
            try:
              f = open(stderr_file, "wa")
              f.write("Error in job submission: %s" %(e))
              f.close()
            except IOError, ioe:
              pass
            self.logger.error("Error in job submission: %s" %(e))
            raise DRMError("Job submission error: %s" %(e))
       
          return drmaaSubmittedJobId
  
        def kill_job(self, scheduler_job_id):
          if self.is_sleeping: self.wake()
          if scheduler_job_id == self.FAKE_JOB:
            return # barriers are not run, thus cannot be killed.
          try:
            self._drmaa.control(scheduler_job_id, JobControlAction.TERMINATE)
          except DrmaaException, e:
            self.logger.critical("%s" %e)
            raise e

        def get_job_status(self, scheduler_job_id):
          if self.is_sleeping: self.wake()
          if scheduler_job_id == self.FAKE_JOB:
            # a barrier job is done as soon as it is started.
            return constants.DONE
          try:
            status = self._drmaa.jobStatus(scheduler_job_id)
          except DrmaaException, e:
            self.logger.error("%s" %(e))
            raise DRMError("%s" %(e))
          return status

        def get_job_exit_info(self, scheduler_job_id):
          if self.is_sleeping: self.wake()
  
          if scheduler_job_id == self.FAKE_JOB:
            res_resourceUsage = ''
            res_status = constants.FINISHED_REGULARLY
            res_exitValue = 0
            res_termSignal = None
            return (res_status, res_exitValue, res_termSignal,
              res_resourceUsage)

          res_resourceUsage=[]
          res_status = constants.EXIT_UNDETERMINED
          res_exitValue = 0
          res_termSignal = None        
  
          try:
            self.logger.debug("  ==> Start to find info of job %s"%(scheduler_job_id)) 
            jid_out, exit_value, signaled, term_sig, coredumped, aborted,exit_status,resource_usage=self._drmaa.wait(scheduler_job_id, self._drmaa.TIMEOUT_NO_WAIT)
            
            self.logger.debug("  ==> jid_out="+repr(jid_out))
            self.logger.debug("  ==> exit_value="+repr(exit_value))
            self.logger.debug("  ==> signaled="+repr(signaled))
            self.logger.debug("  ==> term_sig="+repr(term_sig))
            self.logger.debug("  ==> coredumped="+repr(coredumped))
            self.logger.debug("  ==> aborted="+repr(aborted))
            self.logger.debug("  ==> exit_status="+repr(exit_status))
            self.logger.debug("  ==> resource_usage="+repr(resource_usage))
    
            if aborted == 1:
              res_status=constants.EXIT_ABORTED
            else:
              if exit_value == 1:
                res_status=constants.FINISHED_REGULARLY
                res_exitValue=exit_status
              else :
                if signaled == 1:
                  res_status=constants.FINISHED_TERM_SIG
                  res_termSignal=term_sig
                else:
                  res_status=constants.FINISHED_UNCLEAR_CONDITIONS
    
  
            res_resourceUsage = ''
            for k,v in resource_usage.iteritems():
              res_resourceUsage = res_resourceUsage + k + '=' + v + ' '
  
  
          except ExitTimeoutException:
            res_status = constants.EXIT_UNDETERMINED
            self.logger.debug("  ==> self._drmaa.wait time out")

          # DRMAA may leave files in ~/.drmaa
          self.cleanup_drmaa_files(scheduler_job_id)

          return (res_status,res_exitValue , res_termSignal, res_resourceUsage)

        def cleanup_drmaa_files(self, scheduler_job_id):
          filename = os.path.join(Configuration.get_home_dir(),
            '.drmaa', str(scheduler_job_id))
          startfile = '%s.started' % filename
          endfile = '%s.exitcode' % filename
          for f in (startfile, endfile):
            if os.path.exists(f):
              os.unlink(f)
else:

  class DrmaaCTypes(Scheduler):pass


class LocalScheduler(Scheduler):
  '''
  Allow to submit, kill and get the status of jobs.
  Run on one machine without dependencies.

  * _proc_nb *int*

  * _queue *list of scheduler jobs ids*
  
  * _jobs *dictionary job_id -> soma_workflow.engine_types.EngineJob*
  
  * _processes *dictionary job_id -> subprocess.Popen*

  * _status *dictionary job_id -> job status as defined in constants*

  * _exit_info * dictionay job_id -> exit info*

  * _loop *thread*

  * _interval *int*

  * _look *threading.RLock*
  '''
  parallel_job_submission_info = None
  
  logger = None

  _proc_nb = None

  _queue = None

  _jobs = None 
  
  _processes = None

  _status = None

  _exit_info = None

  _loop = None

  _interval = None

  _lock = None

  def __init__(self, proc_nb=1, interval=1):
    super(LocalScheduler, self).__init__()
  
    self.parallel_job_submission_info = None

    self._proc_nb = proc_nb
    self._interval = interval
    self._queue = []
    self._jobs = {}
    self._processes = {}
    self._status = {}
    self._exit_info = {}

    self._lock = threading.RLock()

    self.stop_thread_loop = False

    def loop(self):
      while not self.stop_thread_loop:
        with self._lock:
          self._iterate()
        time.sleep(self._interval)

    self._loop = threading.Thread(name="scheduler_loop",
                                  target=loop,
                                  args=[self])
    self._loop.setDaemon(True)
    self._loop.start()

    atexit.register(LocalScheduler.end_scheduler_thread, self)

  def change_proc_nb(self, proc_nb):
    with self._lock:
      self._proc_nb = proc_nb

  def change_interval(self, interval):
    with self._lock:
      self._interval = interval

  def end_scheduler_thread(self):
    with self._lock:
      self.stop_thread_loop = True
      self._loop.join()
      #print "Soma scheduler thread ended nicely."

  def _iterate(self):
    # Nothing to do if the queue is empty and nothing is running
    if not self._queue and not self._processes:
      return
    #print "#############################"
    # Control the running jobs
    ended_jobs = []
    for job_id, process in self._processes.iteritems():
      ret_value = process.poll()
      #print "job_id " + repr(job_id) + " ret_value " + repr(ret_value)
      if ret_value != None:
        ended_jobs.append(job_id) 
        self._exit_info[job_id] = (constants.FINISHED_REGULARLY,
                                     ret_value,
                                     None,
                                     None)

    # update for the ended job
    for job_id in ended_jobs:
      #print "updated job_id " + repr(job_id) + " status DONE"
      self._status[job_id] = constants.DONE
      del self._processes[job_id]

    # run new jobs
    while (self._queue and
           len(self._processes) < self._proc_nb):
      job_id = self._queue.pop(0)
      job = self._jobs[job_id]
      #print "new job " + repr(job.job_id)
      if job.is_barrier:
        # barrier jobs are not actually run using Popen:
        # they succeed immediately.
        self._exit_info[job.job_id] = (constants.FINISHED_REGULARLY,
                                  0,
                                  None,
                                  None)
        self._status[job.job_id] = constants.DONE
      else:
        process = LocalScheduler.create_process(job)
        if process == None:
          self._exit_info[job.job_id] = (constants.EXIT_ABORTED,
                                    None,
                                    None,
                                    None)
          self._status[job.job_id] = constants.FAILED
        else:
          self._processes[job.job_id] = process
          self._status[job.job_id] = constants.RUNNING


  @staticmethod
  def create_process(engine_job):
    '''
    * engine_job *EngineJob*
   
    * returns: *Subprocess process* 
    '''

    command = engine_job.plain_command()

    stdout = engine_job.plain_stdout()
    stdout_file = None
    if stdout:
      try:
        stdout_file = open(stdout, "wb")
      except Exception, e:
        return None

    stderr = engine_job.plain_stderr()
    stderr_file = None
    if stderr:
      try:
        stderr_file = open(stderr, "wb")
      except Exception, e:
        return None

    stdin = engine_job.plain_stdin()
    stdin_file = None
    if stdin:
      try:
        stdin_file = open(stdin, "rb")
      except Exception, e:
        if stderr:
          stderr_file = open(stderr, "wb")
          s = '%s: %s \n' %(type(e), e)
          stderr_file.write(s)
          stderr_file.close()
        else:
          stdout_file = open(stdout, "wb")
          s = '%s: %s \n' %(type(e), e)
          stdout_file.write(s)
          stdout_file.close()
        return None

    working_directory = engine_job.plain_working_directory()
    
    try:
      process = subprocess.Popen( command,
                                  stdin=stdin_file,
                                  stdout=stdout_file,
                                  stderr=stderr_file,
                                  cwd=working_directory)

    
    except Exception, e:
      if stderr:
        stderr_file = open(stderr, "wb")
        s = '%s: %s \n' %(type(e), e)
        stderr_file.write(s)
        stderr_file.close()
      else:
        stdout_file = open(stdout, "wb")
        s = '%s: %s \n' %(type(e), e)
        stdout_file.write(s)
        stdout_file.close()
      return None

    return process


  def job_submission(self, job):
    '''
    * job *EngineJob*
    * return: *string*
        Job id for the scheduling system (DRMAA for example)
    '''
    if not job.job_id or job.job_id == -1:
      raise LocalSchedulerError("Invalid job: no id")
    with self._lock:
      #print "job submission " + repr(job.job_id)
      self._queue.append(job.job_id)
      self._jobs[job.job_id] = job
      self._status[job.job_id] = constants.QUEUED_ACTIVE
      self._queue.sort(key=lambda job_id: self._jobs[job_id].priority, 
                       reverse=True)
    return job.job_id


  def get_job_status(self, scheduler_job_id):
    '''
    * scheduler_job_id *string*
        Job id for the scheduling system (DRMAA for example)
    * return: *string*
        Job status as defined in constants.JOB_STATUS
    '''
    if not scheduler_job_id in self._status:
      raise LocalSchedulerError("Unknown job.")

    status = self._status[scheduler_job_id]
    return status

  def get_job_exit_info(self, scheduler_job_id):
    '''
    * scheduler_job_id *string*
        Job id for the scheduling system (DRMAA for example)
    * return: *tuple*
        exit_status, exit_value, term_sig, resource_usage
    '''
    # TBI errors
    with self._lock:
      exit_info = self._exit_info[scheduler_job_id]
      del self._exit_info[scheduler_job_id]
    return exit_info


  def kill_job(self, scheduler_job_id):
    '''
    * scheduler_job_id *string*
        Job id for the scheduling system (DRMAA for example)
    '''
    # TBI Errors
    
    with self._lock:
      #print "kill job " + repr(scheduler_job_id)
      if scheduler_job_id in self._processes:
        #print "    => kill the process "
        process = self._processes[scheduler_job_id]
        if sys.version_info < (2, 6):
          if sys.platform == 'win32':
            PROCESS_TERMINATE = 1
            handle = ctypes.windll.kernel32.OpenProcess(PROCESS_TERMINATE, 
                                               False, 
                                               process.pid)
            ctypes.windll.kernel32.TerminateProcess(handle, -1)
            ctypes.windll.kernel32.CloseHandle(handle)
          else:
            os.kill(process.pid, signal.SIGKILL)
            os.wait()
        else:
          process.kill()

        del self._processes[scheduler_job_id]
        self._status[scheduler_job_id] = constants.FAILED
        self._exit_info[scheduler_job_id] = (constants.USER_KILLED,
                                              None,
                                              None,
                                              None)
      elif scheduler_job_id in self._queue:
        #print "    => removed from queue "
        self._queue.remove(scheduler_job_id)
        del self._jobs[scheduler_job_id]
        self._status[scheduler_job_id] = constants.FAILED
        self._exit_info[scheduler_job_id] = (constants.EXIT_ABORTED,
                                              None,
                                              None,
                                              None)


class ConfiguredLocalScheduler(LocalScheduler):
  '''
  Local scheduler synchronized with a configuration object.
  '''

  _config = None

  def __init__(self, config):
    '''
    * config *LocalSchedulerCfg*
    '''
    super(ConfiguredLocalScheduler, self).__init__(config.get_proc_nb(), 
                                           config.get_interval())
    self._config = config

    self._config.addObserver(self,
                             "update_from_config", 
                             [LocalSchedulerCfg.PROC_NB_CHANGED, LocalSchedulerCfg.INTERVAL_CHANGED])


  def update_from_config(self, observable, event, msg):
    if event == LocalSchedulerCfg.PROC_NB_CHANGED:
      self.change_proc_nb(self._config.get_proc_nb())
    if event == LocalSchedulerCfg.PROC_NB_CHANGED:
      self.change_interval(self._config.get_interval())
    self._config.save_to_file()
    

