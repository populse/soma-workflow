from __future__ import with_statement

import subprocess
import threading
import time
import logging
import os

import soma.workflow.constants as constants

try:
  from soma.workflow.somadrmaajobssip import DrmaaJobs, DrmaaError
except ImportError:
  class DrmaaError(Exception): pass 
  class DrmaaJobs(object): pass


class Scheduler(object):
  '''
  Allow to submit, kill and get the status of jobs.
  '''
  parallel_job_submission_info = None
  
  logger = None

  def __init__(self):
    self.parallel_job_submission_info = None

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


class Drmaa(Scheduler):
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
  #  -Drmaa job template attributes 
  #  -parallel configuration name as defined in soma.workflow.constants
  # dict
  parallel_job_submission_info = None
  
  logger = None

  def __init__(self, drmaa_implementation, parallel_job_submission_info):

    self.logger = self.logger = logging.getLogger('ljp.drmaajs')

    self._drmaa = DrmaaJobs()
    try:
      self._drmaa.initSession()
    except DrmaaError, e:
      raise DRMError("Could not create the DRMAA session: %s" %e )

    self._drmaa_implementation = drmaa_implementation

    self.parallel_job_submission_info = parallel_job_submission_info

    self.logger.debug("Parallel job submission info: %s", 
                      repr(parallel_job_submission_info))

    # patch for the PBS-torque DRMAA implementation
    if self._drmaa_implementation == "PBS":
      try:
        jobTemplateId = self._drmaa.allocateJobTemplate()
        self._drmaa.setCommand(jobTemplateId, "echo", [])
        self._drmaa.setAttribute(jobTemplateId, 
                                "drmaa_output_path", 
                                "[void]:/tmp/soma-workflow-empty-job.o")
        self._drmaa.setAttribute(jobTemplateId, 
                                "drmaa_error_path", 
                                "[void]:/tmp/soma-workflow-empty-job.e")
        self._drmaa.runJob(jobTemplateId)
      except DrmaaError, e:
        raise DRMError("%s" %e)
      ################################
    

  def job_submission(self, job):
    '''
    @type  job: soma.workflow.client.Job
    @param job: job to be submitted
    @rtype: string
    @return: drmaa job id 
    '''

    # patch for the PBS-torque DRMAA implementation
    command = []
    job_command = job.plain_command()
    if self._drmaa_implementation == "PBS":
      for command_el in job_command:
        command_el = command_el.replace('"', '\\\"')
        command.append("\"" + command_el + "\"")
      #self.logger.debug("PBS case, new command:" + repr(command))
    else:
      command = job_command

    #self.logger.debug("command: " + repr(command))
    
    stdout_file = job.plain_stdout()
    stderr_file = job.plain_stderr()
    stdin = job.plain_stdin()
    job_env = []
    for var_name in os.environ.keys():
      job_env.append(var_name+"="+os.environ[var_name])

    try:
      drmaaJobId = self._drmaa.allocateJobTemplate()

      self._drmaa.setCommand(drmaaJobId, command[0], command[1:])
 
      self._drmaa.setAttribute(drmaaJobId, 
                              "drmaa_output_path", 
                              "[void]:" + stdout_file)
 
      if job.join_stderrout:
        self._drmaa.setAttribute(drmaaJobId,
                                "drmaa_join_files", 
                                "y")
      else:
        if stderr_file:
          self._drmaa.setAttribute(drmaaJobId, 
                                  "drmaa_error_path", 
                                  "[void]:" + stderr_file)
      
      if job.stdin:
        #self.logger.debug("stdin: " + repr(stdin))
        self._drmaa.setAttribute(drmaaJobId, 
                                "drmaa_input_path", 
                                "[void]:" + stdin)
        
      working_directory = job.plain_working_directory()
      if working_directory:
        self._drmaa.setAttribute(drmaaJobId, "drmaa_wd", working_directory)

      if job.queue:
        self._drmaa.setAttribute(drmaaJobId, "drmaa_native_specification", "-q " + str(job.queue))

      #self._drmaa.setAttribute(drmaaJobId, "drmaa_native_specification", "-l h_rt=0:0:30" )
      
      if job.parallel_job_info :
        parallel_config_name, max_node_number = job.parallel_job_info
        self._setDrmaaParallelJob(drmaaJobId, 
                                  parallel_config_name, 
                                  max_node_number)
        
      self._drmaa.setVectorAttribute(drmaaJobId, 'drmaa_v_env', job_env)

      drmaaSubmittedJobId = self._drmaa.runJob(drmaaJobId)
      self._drmaa.deleteJobTemplate(drmaaJobId)
    except DrmaaError, e:
      self.logger.debug("Error in job submission: %s" %(e))
      raise DRMError("Job submission error: %s" %(e))

    return drmaaSubmittedJobId
    

  def get_job_status(self, scheduler_job_id):
    try:
      status = self._drmaa.jobStatus(scheduler_job_id)
    except DrmaaError, e:
      raise DRMError("%s" %(e))
    return status


  def get_job_exit_info(self, scheduler_job_id):
    exit_status, exit_value, term_sig, resource_usage = self._drmaa.wait(scheduler_job_id, 0)

    str_rusage = ''
    for rusage in resource_usage:
      str_rusage = str_rusage + rusage + ' '

    return (exit_status, exit_value, term_sig, str_rusage)

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
    configuration_name: type of parallel job as defined in soma.workflow.constants 
    (eg MPI, OpenMP...)
    max_node_num: maximum node number the job requests (on a unique machine or 
    separated machine depending on the parallel configuration)
    ''' 

    self.logger.debug(">> _setDrmaaParallelJob")
  
    cluster_specific_cfg_name = self.parallel_job_submission_info[configuration_name]
    
    for drmaa_attribute in constants.PARALLEL_DRMAA_ATTRIBUTES:
      value = self.parallel_job_submission_info.get(drmaa_attribute)
      if value: 
        #value = value.format(config_name=cluster_specific_cfg_name, max_node=max_num_node)
        value = value.replace("{config_name}", cluster_specific_cfg_name)
        value = value.replace("{max_node}", repr(max_num_node))
        self._drmaa.setAttribute( drmaa_job_template_id, 
                                    drmaa_attribute, 
                                    value)
        self.logger.debug("Parallel job, drmaa attribute = %s, value = %s ",
                          drmaa_attribute, value) 


    job_env = []
    for parallel_env_v in constants.PARALLEL_JOB_ENV:
      value = self.parallel_job_submission_info.get(parallel_env_v)
      if value: job_env.append(parallel_env_v+'='+value.rstrip())
    
    self._drmaa.setVectorAttribute(drmaa_job_template_id, 'drmaa_v_env', job_env)
    self.logger.debug("Parallel job environment : " + repr(job_env))
        
    self.logger.debug("<< _setDrmaaParallelJob")


  def kill_job(self, scheduler_job_id):
    try:
      self._drmaa.terminate(scheduler_job_id)
    except DrmaaError, e:
      raise DRMError("%s" %e)




class LocalScheduler(object):
  '''
  Allow to submit, kill and get the status of jobs.
  Run on one machine without dependencies.

  * _nb_proc *int*

  * _queue *list of scheduler jobs ids*
  
  * _jobs *dictionary job_id -> soma.workflow.engine_types.EngineJob*
  
  * _processes *dictionary job_id -> subprocess.Popen*

  * _status *dictionary job_id -> job status as defined in constants*

  * _exit_info * dictionay job_id -> exit info*

  * _loop *thread*

  * _period *int*

  * _look *threading.RLock*
  '''
  parallel_job_submission_info = None
  
  logger = None

  _nb_proc = None

  _queue = None

  _jobs = None 
  
  _processes = None

  _status = None

  _exit_info = None

  _loop = None

  _period = None

  _lock = None

  def __init__(self, nb_proc=1, period=1):
    super(LocalScheduler, self).__init__()
  

    self.parallel_job_submission_info = None

    self._nb_proc = nb_proc
    self._period = period
    self._queue = []
    self._jobs = {}
    self._processes = {}
    self._status = {}
    self._exit_info = {}

    self._lock = threading.RLock()

    def loop(self):
      while True:
        with self._lock:
          self._iterate()
        time.sleep(self._period)

    self._loop = threading.Thread(name="scheduler_loop",
                                  target=loop,
                                  args=[self])
    self._loop.setDaemon(True)
    self._loop.start()


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
    while self._queue and \
          len(self._processes) < self._nb_proc:
      job_id = self._queue.pop(0)
      job = self._jobs[job_id]
      #print "new job " + repr(job.job_id)
      process = self._create_process(job)
      if process == None:
        self._exit_info[job.job_id] = (constants.EXIT_ABORTED,
                                   None,
                                   None,
                                   None)
        self._status[job.job_id] = constants.FAILED
      else:
        self._processes[job.job_id] = process
        self._status[job.job_id] = constants.RUNNING

  def _create_process(self, engine_job):

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
        self._processes[scheduler_job_id].kill()
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
