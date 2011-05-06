from __future__ import with_statement

'''
@author: Yann Cointepas
@author: Soizic Laguitton
@organization: U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''

'''
soma-workflow engine classes
'''

#-----------------------------------------------------------------------------
# Imports
#-----------------------------------------------------------------------------

from datetime import date, timedelta, datetime
import threading
import pwd
import os
import time
import logging
import stat, hashlib, operator
import itertools


#import cProfile

from soma.workflow.engine_types import EngineJob, EngineWorkflow, EngineTransfer
import soma.workflow.constants as constants
from soma.workflow.client import WorkflowController
from soma.workflow.errors import JobError, UnknownObjectError, EngineError, DRMError
from soma.workflow.transfer import RemoteFileController
from soma.workflow.somadrmaajobssip import DrmaaJobs, DrmaaError

#-----------------------------------------------------------------------------
# Globals and constants
#-----------------------------------------------------------------------------

__docformat__ = "epytext en"
refreshment_interval = 1 #seconds
# if the last status update is older than the refreshment_timeout 
# the status is changed into WARNING
refreshment_timeout = 60 #seconds

#-----------------------------------------------------------------------------
# Classes and functions
#-----------------------------------------------------------------------------

class EngineLoopThread(threading.Thread):
  def __init__(self, engine_loop):
    super(EngineLoopThread, self).__init__()
    self.engine_loop = engine_loop
    self.time_interval = refreshment_interval
  
  def run(self):
    #cProfile.runctx("self.engine_loop.start_loop(self.time_interval)", globals(), locals(), "/home/sl225510/profiling/profile_loop_thread")
    self.engine_loop.start_loop(self.time_interval)

  def stop(self):
    self.engine_loop.stop_loop()
    self.join()
  
  
class Drmaa(object):
  '''
  Manipulation of the Drmaa session. 
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
    

  def get_job_status(self, drmaa_job_id):
    try:
      status = self._drmaa.jobStatus(drmaa_job_id)
    except DrmaaError, e:
      raise DRMError("%s" %(e))
    return status


  def get_job_exit_info(self, drmaa_job_id):
    exit_status, exit_value, term_sig, resource_usage = self._drmaa.wait(drmaa_job_id, 0)

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


  def kill_job(self, job_drmaa_id):
    try:
      self._drmaa.terminate(job_drmaa_id)
    except DrmaaError, e:
      raise DRMError("%s" %e)




class WorkflowEngineLoop(object):

  # jobs managed by the current engine process instance. 
  # The workflows jobs are not duplicated here.
  # dict job_id -> registered job
  _jobs = None
  # workflows managed ny the current engine process instance. 
  # each workflow holds a set of EngineJob
  # dict workflow_id -> workflow 
  _workflows = None
  # Drmaa session
  # DrmaaHelper
  _engine_drmaa = None
  # database server proxy
  # soma.workflow.database_server
  _database_server = None
  # user_id
  _user_id = None
  # for each namespace a dictionary holding the traduction 
  #  (association uuid => engine path)
  # dictionary, namespace => uuid => path
  _path_translation = None
  # max number of job for some queues
  # dictionary, queue name (str) => max nb of job (int)
  _queue_limits = None
  # Submission pending queues.
  # For each limited queue, a submission pending queue is needed to store the
  # jobs that couldn't be submitted. 
  # Dictionary queue name (str) => pending jobs (list) 
  _pending_queues = None
  # boolean
  _running = None
  # boolean
  _j_wf_ended = None

  _lock = None

  logger = None

  def __init__(self, 
               database_server, 
               engine_drmaa, 
               path_translation=None,
               queue_limits={}):
    
    self.logger = logging.getLogger('engine.WorkflowEngineLoop')

    self._jobs = {} 
    self._workflows = {}
    
    self._database_server = database_server
    
    self._engine_drmaa = engine_drmaa

    self._path_translation = path_translation

    self._queue_limits = queue_limits

    self.logger.debug('queue_limits ' + repr(self._queue_limits))

    self._pending_queues = {} 

    self._running = False

    try:
      userLogin = pwd.getpwuid(os.getuid())[0] 
    except Exception, e:
      self.logger.critical("Couldn't identify user %s: %s \n" %(type(e), e))
      raise EngineError("Couldn't identify user %s: %s \n" %(type(e), e))
  
    self._user_id = self._database_server.register_user(userLogin) 
    self.logger.debug("user_id : " + repr(self._user_id))

    self._j_wf_ended = True

    self._lock = threading.RLock()

  def are_jobs_and_workflow_done(self):
    with self._lock:
      ended = len(self._jobs) == 0 and len(self._workflows) == 0
      return ended

  def start_loop(self, time_interval):
    '''
    Start the workflow engine loop. The loop will run until stop() is called.
    '''
    #one_wf_processed = False
    self._running = True
    drms_error_jobs = {}
    while True:
      if not self._running:
        break
      with self._lock:
        ended_jobs = drms_error_jobs #{}
        wf_to_inspect = set() # set of workflow id
        for job in drms_error_jobs.itervalues():
          if job.workflow_id != -1: 
            wf_to_inspect.add(job.workflow_id)
        

        # --- 1. Jobs and workflow deletion and kill ------------------------
        # Get the jobs and workflow with the status DELETE_PENDING 
        # and KILL_PENDING
        (jobs_to_delete, jobs_to_kill) = self._database_server.jobs_to_delete_and_kill(self._user_id)
        (wf_to_delete, wf_to_kill) = self._database_server.workflows_to_delete_and_kill(self._user_id)

        # Delete and kill properly the jobs and workflows in _jobs and _workflows
        for job_id in jobs_to_kill + jobs_to_delete:
          if job_id in self._jobs:
            self.logger.debug(" stop job " + repr(job_id))
            try:
              stopped = self._stop_job(job_id,  self._jobs[job_id])
            except DRMError, e:
              #TBI how to communicate the error ?
              self.logger.error("!!!ERROR!!! %s :%s" %(type(e), e))
            if job_id in jobs_to_delete:
              self.logger.debug("Delete job : " + repr(job_id))
              self._database_server.delete_job(job_id)
              del self._jobs[job_id]
            elif stopped:
              ended_jobs[job_id] = self._jobs[job_id]
              if job.workflow_id != -1: 
                wf_to_inspect.add(job.workflow_id)

        for wf_id in wf_to_kill + wf_to_delete:
          if wf_id in self._workflows:
            self.logger.debug("Kill workflow : " + repr(wf_id))
            ended_jobs_in_wf = self._stop_wf(wf_id)            
            if wf_id in wf_to_delete:
              self.logger.debug("Delete workflow : " + repr(wf_id))
              self._database_server.delete_workflow(wf_id)
              del self._workflows[wf_id]
            else:
              ended_jobs.update(ended_jobs_in_wf)
              wf_to_inspect.add(wf_id)
        
        # --- 2. Update job status from DRMAA -------------------------------
        # get back the termination status and terminate the jobs which ended 
        
        wf_jobs = {}
        wf_transfers = {}
        for wf in self._workflows.itervalues():
          #one_wf_processed = True
          # TBI add a condition on the workflow status
          wf_jobs.update(wf.registered_jobs)
          wf_transfers.update(wf.registered_tr)

        for job in itertools.chain(self._jobs.itervalues(), wf_jobs.itervalues()):
          if job.exit_status == None and job.drmaa_id != None:
            try:
              job.status = self._engine_drmaa.get_job_status(job.drmaa_id)
            except DrmaaError, e:
              self.logger.error("!!!ERROR!!! %s: %s" %(type(e), e))
              job.status = constants.UNDETERMINED
            #self.logger.debug("job " + repr(job.job_id) + " : " + job.status)
            if job.status == constants.DONE or job.status == constants.FAILED:
              #self.logger.debug("End of job %s, drmaaJobId = %s", 
              #                  job.job_id, job.drmaa_id)
              (job.exit_status, 
              job.exit_value, 
              job.terminating_signal, 
              job.str_rusage) = self._engine_drmaa.get_job_exit_info(job.drmaa_id)
              if job.workflow_id != -1: 
                wf_to_inspect.add(job.workflow_id)
              if job.status == constants.DONE:
                for ft in job.referenced_output_files:
                  engine_path = job.transfer_mapping[ft].engine_path
                  self._database_server.set_transfer_status(engine_path, 
                                                  constants.FILES_ON_CR)
                     
              ended_jobs[job.job_id] = job
              #self.logger.debug("  => exit_status " + repr(job.exit_status))
              #self.logger.debug("  => exit_value " + repr(job.exit_value))
              #self.logger.debug("  => signal " + repr(job.terminating_signal))


        # --- 3. Get back transfered status ----------------------------------
        for engine_path, transfer in wf_transfers.iteritems():
          status = self._database_server.get_transfer_status(engine_path,
                                                             self._user_id)
          transfer.status = status

        for wf_id in self._workflows.iterkeys():
          if self._database_server.pop_workflow_ended_transfer(wf_id):
            self.logger.debug("ended transfer for the workflow " + repr(wf_id))
            wf_to_inspect.add(wf_id)

        # --- 4. Inspect workflows -------------------------------------------
        #self.logger.debug("wf_to_inspect " + repr(wf_to_inspect))
        for wf_id in wf_to_inspect:
          (to_run, 
          aborted_jobs, 
          status) = self._workflows[wf_id].find_out_jobs_to_process()
          self._workflows[wf_id].status = status
          self.logger.debug("NEW status wf " + repr(wf_id) + " " + repr(status))
          #jobs_to_run.extend(to_run)
          ended_jobs.update(aborted_jobs)
          for job in to_run:
            self._pend_for_submission(job)

        # --- 5. Check if pending jobs can now be submitted ------------------
        jobs_to_run = self._get_pending_job_to_submit()

        # --- 6. Submit jobs -------------------------------------------------
        drms_error_jobs = {}
        drmaa_id_for_db_up = {}
        for job in jobs_to_run:
          try:
            job.drmaa_id = self._engine_drmaa.job_submission(job)
          except DRMError, e:
            #TBI how to communicate the error ?
            #if job.queue in self._pending_queues:
            #  self._pending_queues[job.queue].insert(0, job)
            #else:
            #  self._pending_queues[job.queue] = [job]
            #job.status = constants.SUBMISSION_PENDING
            self.logger.error("job %s !!!ERROR!!! %s: %s" %(repr(job.command), type(e), e))
            job.status = constants.FAILED
            drms_error_jobs[job.job_id] = job
          else:
            drmaa_id_for_db_up[job.job_id] = job.drmaa_id
            job.status = constants.UNDETERMINED     

        self._database_server.set_submission_information(drmaa_id_for_db_up,
                                                         datetime.now())  
   
        # --- 7. Update the workflow and jobs status to the database_server -
        ended_job_ids = []
        ended_wf_ids = []
        #self.logger.debug("update job and wf status ~~~~~~~~~~~~~~~ ")
        job_status_for_db_up = {}
        for job_id, job in itertools.chain(self._jobs.iteritems(),
                                          wf_jobs.iteritems()):
          job_status_for_db_up[job_id] = job.status
          #self._database_server.set_job_status(job.job_id, job.status)
          self._j_wf_ended = self._j_wf_ended and \
                                    (job.status == constants.DONE or \
                                    job.status == constants.FAILED)
          if job_id in self._jobs and \
             (job.status == constants.DONE or \
              job.status == constants.FAILED):
              ended_job_ids.append(job_id)
          #self.logger.debug("job " + repr(job_id) + " " + repr(job.status))
       
        self._database_server.set_jobs_status(job_status_for_db_up)

        for job_id, job in ended_jobs.iteritems():
          self._database_server.set_job_exit_info(job_id, 
                                                  job.exit_status, 
                                                  job.exit_value, 
                                                  job.terminating_signal, 
                                                  job.str_rusage)
        for wf_id, workflow in self._workflows.iteritems():
          self._database_server.set_workflow_status(wf_id, workflow.status)
          if workflow.status == constants.WORKFLOW_DONE:
            ended_wf_ids.append(wf_id)
          self.logger.debug("wf " + repr(wf_id) + " " + repr(workflow.status))
        #self.logger.debug("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ")
        
        for job_id in ended_job_ids: del self._jobs[job_id]
        for wf_id in ended_wf_ids: del self._workflows[wf_id]

      #if len(self._workflows) == 0 and one_wf_processed: 
      #  break
      time.sleep(time_interval)

    
  def stop_loop(self):
    self._running = False

  def add_job(self, client_job, queue):
    # register
    engine_job = EngineJob(client_job=client_job, 
                           queue=queue, 
                           path_translation=self._path_translation)
                          

    engine_job = self._database_server.add_job(self._user_id, engine_job)

    # create standard output files 
    try:  
      tmp = open(engine_job.stdout_file, 'w')
      tmp.close()
    except Exception, e:
      self._database_server.delete_job(engine_job.job_id)
      raise JobError("Could not create the standard output file " 
                     "%s %s: %s \n"  %
                     (repr(engine_job.stdout_file), type(e), e))
    if engine_job.stderr_file:
      try:
        tmp = open(engine_job.stderr_file, 'w')
        tmp.close()
      except Exception, e:
        self._database_server.delete_job(engine_job.job_id)
        raise JobError("Could not create the standard error file "
                       "%s: %s \n"  %(type(e), e))

    for transfer in engine_job.transfer_mapping.itervalues():
      if transfer.client_paths and not os.path.isdir(transfer.engine_path):
        try:
          os.mkdir(transfer.engine_path) 
        except Exception, e:
          self._database_server.delete_job(engine_job.job_id)
          raise JobError("Could not create the directory %s %s: %s \n"  %
                         (repr(transfer.engine_path), type(e), e))

    # submit
    self._pend_for_submission(engine_job)
    # add to the engine managed job list
    with self._lock:
      self._jobs[engine_job.job_id] = engine_job

    return engine_job


  def _pend_for_submission(self, engine_job):
    '''
    All the job submission are actually done in the loop (start_loop method).
    The jobs to submit after add_job, add_workflow and restart_workflow are 
    first stored in _pending_queues waiting to be submitted.
    '''
    if engine_job.queue in self._pending_queues:
      self._pending_queues[engine_job.queue].append(engine_job)
    else:
      self._pending_queues[engine_job.queue] = [engine_job]
    engine_job.status = constants.SUBMISSION_PENDING


  def _get_pending_job_to_submit(self):
    '''
    @rtype: list of EngineJob
    @return: the list of job to be submitted 
    '''    
    to_run = []
    for queue_name, jobs in self._pending_queues.iteritems():
      if jobs and queue_name in self._queue_limits:
        nb_queued_jobs = self._database_server.nb_queued_jobs(self._user_id, 
                                                              queue_name)
        nb_jobs_to_run = self._queue_limits[queue_name] - nb_queued_jobs
        self.logger.debug("queue " + repr(queue_name) + " nb_queued_jobs " + repr(nb_queued_jobs) + " nb_jobs_to_run " + repr(nb_jobs_to_run))
        while nb_jobs_to_run > 0 and \
              len(self._pending_queues[queue_name]) > 0:
          to_run.append(self._pending_queues[queue_name].pop(0))
          nb_jobs_to_run = nb_jobs_to_run - 1
      else:
        to_run.extend(jobs)
        self._pending_queues[queue_name] = []
    #self.logger.debug("to_run " + repr(to_run))
    return to_run
    


  def add_workflow(self, client_workflow, expiration_date, name, queue):
    '''
    @type client_workflow: soma.workflow.client.Workflow
    @type expiration_date: datetime.datetime
    @type name: str
    @type queue: str
    '''
    # register
    engine_workflow = EngineWorkflow(client_workflow, 
                                     self._path_translation,
                                     queue,
                                     expiration_date,
                                     name)
   
    engine_workflow = self._database_server.add_workflow(self._user_id, engine_workflow)

    for job in engine_workflow.job_mapping.itervalues():
      try:  
        tmp = open(job.stdout_file, 'w')
        tmp.close()
      except Exception, e:
        self._database_server.delete_workflow(engine_workflow.wf_id)
        raise JobError("Could not create the standard output file " 
                       "%s %s: %s \n"  %
                       (repr(job.stdout_file), type(e), e))
      if job.stderr_file:
        try:
          tmp = open(job.stderr_file, 'w')
          tmp.close()
        except Exception, e:
          self._database_server.delete_workflow(engine_workflow.wf_id)
          raise JobError("Could not create the standard error file "
                         "%s %s: %s \n"  %
                         (repr(job.stderr_file), type(e), e))

    for transfer in engine_workflow.transfer_mapping.itervalues():
      if transfer.client_paths and not os.path.isdir(transfer.engine_path):
        try:
          os.mkdir(transfer.engine_path) 
        except Exception, e:
          self._database_server.delete_workflow(engine_workflow.wf_id)
          raise JobError("Could not create the directory %s %s: %s \n"  %
                         (repr(transfer.engine_path), type(e), e))

    # submit independant jobs
    (jobs_to_run, 
     engine_workflow.status) = engine_workflow.find_out_independant_jobs()
    for job in jobs_to_run:
      self._pend_for_submission(job)
    # add to the engine managed workflow list
    with self._lock:
      self._workflows[engine_workflow.wf_id] = engine_workflow

    return engine_workflow.wf_id

  def _stop_job(self, job_id, job):
    if job.status == constants.DONE or job.status == constants.FAILED:
      self._database_server.set_job_status( job_id, 
                                            job.status, 
                                            force = True)
      return False
    else:
      if job.drmaa_id:
        self.logger.debug("Kill job " + repr(job_id) + " drmaa id: " + repr(job.drmaa_id) + " status " + repr(job.status))
        try:
	  self._engine_drmaa.kill_job(job.drmaa_id)
	except DRMError, e:
	  #TBI how to communicate the error
          self.logger.error("!!!ERROR!!! %s:%s" %(type(e), e))
      elif job.queue in self._pending_queues and \
           job in self._pending_queues[job.queue]:
        self._pending_queues[job.queue].remove(job)
      job.status = constants.FAILED
      job.exit_status = constants.USER_KILLED
      self._database_server.set_job_exit_info(job_id,
                                              constants.USER_KILLED,
                                              None,
                                              None,
                                              None)
      self._database_server.set_job_status(job_id, 
                                            constants.FAILED, 
                                            force = True)
      return True
    

  def _stop_wf(self, wf_id):
    wf = self._workflows[wf_id]
    #self.logger.debug("wf.registered_jobs " + repr(wf.registered_jobs))
    ended_jobs = {}
    for job_id, job in wf.registered_jobs.iteritems():
      if self._stop_job(job_id, job):
        ended_jobs[job_id] = job
    self._database_server.set_workflow_status(wf_id, 
                                              constants.WORKFLOW_DONE, 
                                              force = True)
    return ended_jobs


  def restart_workflow(self, wf_id, status):
    if wf_id in self._workflows:
      workflow = self._workflows[wf_id]
      (jobs_to_run, 
       workflow.status) = workflow.restart(self._database_server)
      for job in jobs_to_run:
        self._pend_for_submission(job)
    else:
      workflow = self._database_server.get_engine_workflow(wf_id, self._user_id)
      workflow.status = status
      (jobs_to_run, workflow.status) = workflow.restart(self._database_server)
      for job in jobs_to_run:
        self._pend_for_submission(job)
      # add to the engine managed workflow list
      with self._lock:
        self._workflows[wf_id] = workflow

  def restart_job(self, job_id, status):
    (job, workflow_id) = self._database_server.get_engine_job(job_id, self._user_id)
    if workflow_id == -1: 
      job.status = status
      # submit
      self._pend_for_submission(job)
      # add to the engine managed job list
      with self._lock:
        self._jobs[job.job_id] = job
    else:
      
      pass
      #TBI


class WorkflowEngine(RemoteFileController):
  '''
  '''
  # database server
  # soma.workflow.database_server.WorkflowDatabaseServer
  _database_server = None
  # WorkflowEngineLoop
  _engine_loop = None
  # id of the user on the database server
  _user_id = None


  def __init__( self, database_server, engine_loop):
    ''' 
    @type  database_server:
           L{soma.workflow.database_server.WorkflowDatabaseServer}
    @type  engine_loop: L{WorkflowEngineLoop}
    '''
    
    self.logger = logging.getLogger('engine.WorkflowEngine')
    
    self._database_server= database_server
    self._engine_loop = engine_loop
    
    try:
      user_login = pwd.getpwuid(os.getuid())[0]
    except Exception, e:
      raise EngineError("Couldn't identify user %s: %s \n" %(type(e), e))
    
    self._user_id = self._database_server.register_user(user_login)
    self.logger.debug("user_id : " + repr(self._user_id))

  def __del__( self ):
    pass

  ########## FILE TRANSFER ###############################################

  def register_transfer(self, 
                        file_transfer): 
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
    engine_transfer = EngineTransfer(file_transfer)
    engine_transfer = self._database_server.add_transfer(engine_transfer, 
                                                         self._user_id)

    if engine_transfer.client_paths:
      os.mkdir(engine_transfer.engine_path)

    return engine_transfer

  def transfer_information(self, engine_path):
    '''
    @rtype: tuple (string, string, date, int, sequence)
    @return: (engine_file_path, 
              client_file_path, 
              expiration_date, 
              workflow_id,
              client_paths,
              transfer_type, 
              status)
    '''
    return self._database_server.get_transfer_information(engine_path, self._user_id)


  #def init_transfer_from_cr(self, 
                            #engine_path,
                            #client_path, 
                            #expiration_date, 
                            #workflow_id, 
                            #client_paths,
                            #status):
    #'''
    #Initialize the transfer of a file or a directory from the computing resource 
    #to the client.
    #'''
    #if not client_paths:
      #if os.path.isfile(engine_path):
        #transfer_type = constants.TR_FILE_CR_TO_C
      #elif os.path.isdir(engine_path):
        #transfer_type = constants.TR_DIR_CR_TO_C
      #else:
        #pass 
        ## TBI raise execption
    #else: #client_paths
      ## TBI check that the files exit
      #transfer_type = constants.TR_MFF_CR_TO_C

    #self._database_server.set_transfer_status(engine_path,    
                                              #constants.TRANSFERING_FROM_CR_TO_CLIENT)
    #self._database_server.set_transfer_type(engine_path, 
                                            #transfer_type, 
                                            #self._user_id)     
    #return transfer_type


  def set_transfer_type(self, engine_path, transfer_type):

    self._database_server.set_transfer_type(engine_path, 
                                            transfer_type,
                                            self._user_id)

    
  def set_transfer_status(self, engine_path, status):
    '''
    Set a transfer status. 
    '''
    self._database_server.set_transfer_status(engine_path, status)


  def delete_transfer(self, engine_path):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''

    self._database_server.remove_transfer(engine_path, self._user_id)

    
  def signalTransferEnded(self, 
                          engine_path, 
                          workflow_id):
    '''
    Has to be called each time a file transfer ends for the 
    workflows to be proceeded.
    '''
    if workflow_id != -1:
      self._database_server.add_workflow_ended_transfer(workflow_id, engine_path)
    

  ########## JOB SUBMISSION ##################################################

  
  def submit_job( self, job, queue):
    '''
    Submits a job to the system. 
    
    @type  job: L{soma.workflow.client.Job}
    @param job: job informations 
    '''
    engine_job = self._engine_loop.add_job(job, queue)

    return engine_job


  def delete_job( self, job_id ):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
    status = self._database_server.get_job_status(job_id,
                                                  self._user_id)[0]
    if status == constants.DONE or status == constants.FAILED:
      self._database_server.delete_job(job_id)
    else:
      self._database_server.set_job_status(job_id, constants.DELETE_PENDING)
      if not self._wait_for_job_deletion(job_id):
        # TBI
        self.logger.critical("!! The job may not be properly deleted !!")
        self._database_server.delete_job(job_id)

  ########## WORKFLOW SUBMISSION ############################################
  
  def submit_workflow(self, workflow, expiration_date, name, queue):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
    if not expiration_date:
      expiration_date = datetime.now() + timedelta(days=7)
    
    wf_id = self._engine_loop.add_workflow(workflow, expiration_date, name, queue)

    return wf_id

  
  def delete_workflow(self, workflow_id, force=True):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
    
    status = self._database_server.get_workflow_status(workflow_id, 
                                                       self._user_id)[0]
    if status == constants.WORKFLOW_DONE:
      self._database_server.delete_workflow(workflow_id)
      return True
    else:
      self._database_server.set_workflow_status(workflow_id, 
                                                constants.DELETE_PENDING)
      if force and not self._wait_for_wf_deletion(workflow_id):
       self.logger.critical("!! The workflow may not be properly deleted !!")
       self._database_server.delete_workflow(workflow_id)
       return False
      return True

  def stop_workflow(self, workflow_id):

    status = self._database_server.get_workflow_status(workflow_id, 
                                                       self._user_id)[0]
    if status != constants.WORKFLOW_DONE:
      self._database_server.set_workflow_status(workflow_id, 
                                                constants.KILL_PENDING)


  def change_workflow_expiration_date(self, workflow_id, new_expiration_date):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    ''' 
    if new_expiration_date < datetime.now(): 
      return False
    # TO DO: Add other rules?
    
    self._database_server.change_workflow_expiration_date(workflow_id, 
                                                          new_expiration_date,
                                                          self._user_id)
    return True


  def restart_workflow(self, workflow_id):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
    (status, last_status_update) = self._database_server.get_workflow_status(workflow_id, self._user_id)
    
    if status != constants.WORKFLOW_DONE and status != constants.WARNING:
      return False

    self._engine_loop.restart_workflow(workflow_id, status)
    return True
    
   
  ########## SERVER STATE MONITORING ########################################


  def jobs(self, job_ids=None):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
    return self._database_server.get_jobs(self._user_id, job_ids)
    

  def transfers(self, transfer_ids=None):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
    return self._database_server.get_transfers(self._user_id, transfer_ids)
  
  
  def workflows(self, workflow_ids=None):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
    return self._database_server.get_workflows(self._user_id, workflow_ids)
  

  def workflow(self, wf_id):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
    return self._database_server.get_engine_workflow(wf_id, self._user_id)

  
  def job_status(self, job_id):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
    (status, 
    last_status_update) = self._database_server.get_job_status(job_id,
                                                               self._user_id)
    if status and not status == constants.DONE and \
       not status == constants.FAILED and \
       last_status_update and \
       datetime.now() - last_status_update > timedelta(seconds = refreshment_interval*refreshment_timeout):
      self._database_server.set_job_status(job_id, constants.WARNING)
      return constants.WARNING

    return status
        
  
  def workflow_status(self, wf_id):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
    (status, 
     last_status_update) = self._database_server.get_workflow_status(wf_id,                                                              
                                                                 self._user_id)

    if status and \
       not status == constants.WORKFLOW_DONE and \
       last_status_update and \
       datetime.now() - last_status_update > timedelta(seconds = refreshment_interval*refreshment_timeout):
      self._database_server.set_workflow_status(wf_id, constants.WARNING)
      return constants.WARNING

    return status
    
  
  def workflow_elements_status(self, wf_id, groupe = None):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
    (status, 
     last_status_update) = self._database_server.get_workflow_status(wf_id,
                                                                  self._user_id)
    if status and \
       not status == constants.WORKFLOW_DONE and \
       last_status_update and \
       datetime.now() - last_status_update > timedelta(seconds = refreshment_interval*refreshment_timeout):
      self._database_server.set_workflow_status(wf_id, constants.WARNING)

    wf_status = self._database_server.get_detailed_workflow_status(wf_id)
    return wf_status
        
        
  def transfer_status(self, engine_path):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
    transfer_status = self._database_server.get_transfer_status(engine_path,
                                                                self._user_id)  
    return transfer_status


  def job_termination_status(self, job_id ):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
    
    job_exit_info= self._database_server.get_job_exit_info(job_id, self._user_id)
    
    return job_exit_info

  def stdouterr_file_path(self, job_id):
    (stdout_file, 
    stderr_file) = self._database_server.get_std_out_err_file_path(job_id, 
                                                             self._user_id)
    return (stdout_file, stderr_file)
    
    
  ########## JOB CONTROL VIA DRMS ########################################
  
  def wait_job( self, job_ids, timeout = -1):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''    
    self.logger.debug("        waiting...")
    
    waitForever = timeout < 0
    startTime = datetime.now()
    for jid in job_ids:
      (status, 
       last_status_update) = self._database_server.get_job_status(jid, 
                                                                  self._user_id)
      if status:
        self.logger.debug("wait        job %s status: %s", jid, status)
        delta = datetime.now()-startTime
        delta_status_update = datetime.now() - last_status_update
        while status and not status == constants.DONE and not status == constants.FAILED and (waitForever or delta < timedelta(seconds=timeout)):
          time.sleep(refreshment_interval)
          (status, last_status_update) = self._database_server.get_job_status(jid, self._user_id) 
          self.logger.debug("wait        job %s status: %s last update %s," 
                            " now %s", 
                            jid, 
                            status, 
                            repr(last_status_update), 
                            repr(datetime.now()))
          delta = datetime.now() - startTime
          if last_status_update and datetime.now() - last_status_update > timedelta(seconds = refreshment_interval*refreshment_timeout):
            raise EngineError("wait_job: Could not wait for job %s. " 
                              "The process updating its status failed." %(jid))
       
 
  def restart_job( self, job_id ):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''

    (status, 
     last_status_update) = self._database_server.get_job_status(job_id,
                                                                self._user_id)
    
    if status != constants.FAILED and status != constants.WARNING:
      return False

    self._engine_loop.restart_job(job_id, status)
    return True


  def kill_job( self, job_id ):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
    
    status = self._database_server.get_job_status(job_id,
                                                  self._user_id)[0]
    if status != constants.DONE and status != constants.FAILED:
      self._database_server.set_job_status(job_id, 
                                          constants.KILL_PENDING)
      
      if not self._wait_job_status_update(job_id):
        self._database_server.set_job_status(job_id, 
                                             constants.WARNING)


  def _wait_for_job_deletion(self, job_id):
    self.logger.debug(">> _wait_for_job_deletion")
    action_time = datetime.now()
    time.sleep(refreshment_interval)
    (is_valid_job, 
     last_status_update) = self._database_server.is_valid_job(job_id, 
                                                              self._user_id)
    while is_valid_job and \
          last_status_update < action_time:
      time.sleep(refreshment_interval)
      (is_valid_job, 
       last_status_update) = self._database_server.is_valid_job(job_id, 
                                                                self._user_id)
      if last_status_update and datetime.now() - last_status_update > timedelta(seconds = refreshment_interval*refreshment_timeout):
        self.logger.debug("<< _wait_for_job_deletion")
        return False

    self.logger.debug("<< _wait_for_job_deletion")
    return True


  def _wait_job_status_update(self, job_id):
    
    self.logger.debug(">> _wait_job_status_update")
    action_time = datetime.now()
    time.sleep(refreshment_interval)
    try:
      (status, 
      last_status_update) = self._database_server.get_job_status(job_id,
                                                                  self._user_id)
      while status and not status == constants.DONE and \
            not status == constants.FAILED and \
            last_status_update < action_time:
        time.sleep(refreshment_interval)
        (status, 
        last_status_update) = self._database_server.get_job_status(job_id,
                                                                  self._user_id) 
        if last_status_update and datetime.now() - last_status_update > timedelta(seconds = refreshment_interval*refreshment_timeout):
          self.logger.debug("<< _wait_job_status_update")
          return False
    except UnknownObjectError, e:
      pass
    self.logger.debug("<< _wait_job_status_update")
    return True

  def _wait_wf_status_update(self, wf_id):  
    self.logger.debug(">> _wait_wf_status_update")
    action_time = datetime.now()
    time.sleep(refreshment_interval)
    try:
      (status, 
      last_status_update) = self._database_server.get_workflow_status(wf_id)
      while status and not status == constants.WORKFLOW_DONE and \
            last_status_update < action_time:
        time.sleep(refreshment_interval)
        (status, 
        last_status_update) = self._database_server.get_workflow_status(wf_id) 
        if last_status_update and \
          datetime.now() - last_status_update > timedelta(seconds=refreshment_interval*refreshment_timeout):
          self.logger.debug("<< _wait_wf_status_update")
          return False
    except UnknownObjectError, e:
      pass
    self.logger.debug("<< _wait_wf_status_update")
    return True


  def _wait_for_wf_deletion(self, wf_id):
    self.logger.debug(">> _wait_for_wf_deletion")
    action_time = datetime.now()
    time.sleep(refreshment_interval)
    (is_valid_wf, 
    last_status_update) = self._database_server.is_valid_workflow(wf_id, 
                                                                self._user_id)
    while is_valid_wf and \
          last_status_update < action_time:
      time.sleep(refreshment_interval)
      (is_valid_wf, 
      last_status_update) = self._database_server.is_valid_workflow(wf_id, 
                                                              self._user_id)
      if last_status_update and datetime.now() - last_status_update > timedelta(seconds = refreshment_interval*refreshment_timeout):
        self.logger.debug("<< _wait_for_wf_deletion")
        return False
    self.logger.debug("<< _wait_for_wf_deletion")
    return True
    
    
