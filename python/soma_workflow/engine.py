from __future__ import with_statement

'''
@author: Soizic Laguitton

@organization: I2BM, Neurospin, Gif-sur-Yvette, France
@organization: CATI, France
@organization: U{IFR 49<http://www.ifr49.org>}

@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''


#-----------------------------------------------------------------------------
# Imports
#-----------------------------------------------------------------------------

from datetime import date, timedelta, datetime
import threading
import getpass
import os
import time
import logging
import stat, hashlib, operator
import itertools
import atexit

#import cProfile
#import traceback

from soma_workflow.engine_types import EngineJob, EngineWorkflow, EngineTransfer, EngineTemporaryPath, FileTransfer
import soma_workflow.constants as constants
from soma_workflow.client import WorkflowController
from soma_workflow.errors import JobError, UnknownObjectError, EngineError, DRMError
from soma_workflow.transfer import RemoteFileController
from soma_workflow.configuration import Configuration

#-----------------------------------------------------------------------------
# Globals and constants
#-----------------------------------------------------------------------------

refreshment_interval = 1 #seconds
# if the last status update is older than the refreshment_timeout 
# the status is changed into WARNING
refreshment_timeout = 60 #seconds

def _out_to_date(last_status_update):
  '''
  Tells if a workflow or a job is out to date considering its last  
  status update. 

  * last_status_update: *datetime*
  '''
  if last_status_update is None:
    return False
  delta = datetime.now() - last_status_update
  out_to_date = delta > timedelta(seconds=refreshment_timeout)
  return out_to_date


#-----------------------------------------------------------------------------
# Classes and functions
#-----------------------------------------------------------------------------

class EngineLoopThread(threading.Thread):
  def __init__(self, engine_loop):
    super(EngineLoopThread, self).__init__()
    self.engine_loop = engine_loop
    self.time_interval = refreshment_interval
    atexit.register(EngineLoopThread.stop, self)
  
  def run(self):
    #cProfile.runctx("self.engine_loop.start_loop(self.time_interval)", globals(), locals(), "/home/sl225510/profiling/profile_loop_thread")
    self.engine_loop.start_loop(self.time_interval)

  def stop(self):
    self.engine_loop.stop_loop()
    self.join()
    #print "Soma workflow engine thread ended nicely."
  

class WorkflowEngineLoop(object):

  # jobs managed by the current engine process instance. 
  # The workflows jobs are not duplicated here.
  # dict job_id -> registered job
  _jobs = None
  # workflows managed ny the current engine process instance. 
  # each workflow holds a set of EngineJob
  # dict workflow_id -> workflow 
  _workflows = None
   
  _scheduler = None
  # database server proxy
  # soma_workflow.database_server
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
               scheduler, 
               path_translation=None,
               queue_limits={}):
    
    self.logger = logging.getLogger('engine.WorkflowEngineLoop')
    
    self._jobs = {} 
    self._workflows = {}
    
    self._database_server = database_server
    
    self._scheduler = scheduler

    self._path_translation = path_translation

    self._queue_limits = queue_limits

    self.logger.debug('queue_limits ' + repr(self._queue_limits))

    self._pending_queues = {} 

    self._running = False

    try:
      userLogin = getpass.getuser()
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
    idle_cmpt = 0
    while True:
      if not self._running:
        break
      with self._lock:
        ended_jobs = drms_error_jobs #{}
        wf_to_inspect = set() # set of workflow id
        for job in drms_error_jobs.itervalues():
          if job.workflow_id != -1: 
            wf_to_inspect.add(job.workflow_id)
        drms_error_jobs = {} 

        if not (len(self._jobs) == 0 and len(self._workflows) == 0):
          idle_cmpt = 0
        else:
          idle_cmpt = idle_cmpt + 1

        if idle_cmpt > 20 and not self._scheduler.is_sleeping:
          self.logger.debug("idle => scheduler sleep")
          self._scheduler.sleep()


        # --- 1. Jobs and workflow deletion and kill ------------------------
        # Get the jobs and workflow with the status DELETE_PENDING 
        # and KILL_PENDING
        jobs_to_delete = [] 
        jobs_to_kill = []
        if self._jobs:
          (jobs_to_delete, jobs_to_kill) = self._database_server.jobs_to_delete_and_kill(self._user_id)
        wf_to_delete = []
        wf_to_kill = []
        if self._workflows:
          (wf_to_delete, wf_to_kill) = self._database_server.workflows_to_delete_and_kill(self._user_id)

        # Delete and kill properly the jobs and workflows in _jobs and _workflows
        for job_id in jobs_to_kill + jobs_to_delete:
          if job_id in self._jobs:
            self.logger.debug(" stop job " + repr(job_id))
            try:
              stopped = self._stop_job(job_id,  self._jobs[job_id])
            except DRMError, e:
              #TBI how to communicate the error ?
              self.logger.error("!!!ERROR!!! stop job %s :%s" %(type(e), e))
            if job_id in jobs_to_delete:
              self.logger.debug("Delete job : " + repr(job_id))
              self._database_server.delete_job(job_id)
              del self._jobs[job_id]
            else:
              self._database_server.set_job_status(job_id, 
                                    job.status, 
                                    force = True)
              if stopped:
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
        
        # --- 2. Update job status from the scheduler -------------------------------
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
              job.status = self._scheduler.get_job_status(job.drmaa_id)
            except DRMError, e:
              self.logger.debug("!!!ERROR!!! get_job_status %s: %s" %(type(e), e))
              job.status = constants.FAILED
              job.exit_status = constants.EXIT_ABORTED
              stderr_file = open(job.stderr_file, "wa")
              stderr_file.write("Error while requesting the job status %s: %s \nWarning: the job may still be running.\n" %(type(e),e))
              stderr_file.close()
              drms_error_jobs[job.job_id] = job
            self.logger.debug("job " + repr(job.job_id) + " : " + job.status)
            if job.status == constants.DONE or job.status == constants.FAILED:
              self.logger.debug("End of job %s, drmaaJobId = %s, status= %s", 
                                job.job_id, job.drmaa_id, repr(job.status))
              (job.exit_status, 
              job.exit_value, 
              job.terminating_signal, 
              job.str_rusage) = self._scheduler.get_job_exit_info(job.drmaa_id)
              
              self.logger.debug("  after get_job_exit_info ")             
              self.logger.debug("  => exit_status " + repr(job.exit_status))
              self.logger.debug("  => exit_value " + repr(job.exit_value))
              self.logger.debug("  => signal " + repr(job.terminating_signal))  
              self.logger.debug("  => rusage "+repr(job.str_rusage))             

  
              if job.workflow_id != -1: 
                wf_to_inspect.add(job.workflow_id)
              if job.status == constants.DONE:
                for ft in job.referenced_output_files:
                  if isinstance(ft, FileTransfer):
                    engine_path = job.transfer_mapping[ft].engine_path
                    self._database_server.set_transfer_status(engine_path,
                                                    constants.FILES_ON_CR)
                  else:
                    # TemporaryPath
                    temp_path_id = job.transfer_mapping[ft].temp_path_id
                    self._database_server.set_temporary_status(temp_path_id,
                                                    constants.FILES_ON_CR)

              ended_jobs[job.job_id] = job
              self.logger.debug("  => exit_status " + repr(job.exit_status))
              self.logger.debug("  => exit_value " + repr(job.exit_value))
              self.logger.debug("  => signal " + repr(job.terminating_signal))


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
        self.logger.debug("wf_to_inspect " + repr(wf_to_inspect))
        for wf_id in wf_to_inspect:
          (to_run, 
          aborted_jobs, 
          status) = self._workflows[wf_id].find_out_jobs_to_process()
          self.logger.debug("to_run=" + repr(to_run)+" aborted_jobs="+repr(aborted_jobs))
          self._workflows[wf_id].status = status
          self.logger.debug("NEW status wf " + repr(wf_id) + " " + repr(status))
          #jobs_to_run.extend(to_run)
          ended_jobs.update(aborted_jobs)
          for job in to_run:
            self._pend_for_submission(job)

        # --- 5. Check if pending jobs can now be submitted ------------------
        self.logger.debug("Check pending jobs")
        jobs_to_run = self._get_pending_job_to_submit()
        self.logger.debug("jobs_to_run="+repr(jobs_to_run))
        self.logger.debug("len(jobs_to_run)="+repr(len(jobs_to_run)))
     
        # --- 6. Submit jobs -------------------------------------------------
        drmaa_id_for_db_up = {}
        for job in jobs_to_run:
            try:
              job.drmaa_id = self._scheduler.job_submission(job)
            except DRMError, e:
              #Resubmission ?
              #if job.queue in self._pending_queues:
              #  self._pending_queues[job.queue].insert(0, job)
              #else:
              #  self._pending_queues[job.queue] = [job]
              #job.status = constants.SUBMISSION_PENDING
              self.logger.debug("job %s !!!ERROR!!! %s: %s" %(repr(job.command), type(e), e))
              job.status = constants.FAILED
              job.exit_status = constants.EXIT_ABORTED
              stderr_file = open(job.stderr_file, "wa")
              stderr_file.write("Error while submitting the job %s: %s\n" %(type(e),e))
              stderr_file.close()
              drms_error_jobs[job.job_id] = job
            else:
              drmaa_id_for_db_up[job.job_id] = job.drmaa_id
              job.status = constants.UNDETERMINED

        if drmaa_id_for_db_up:
          self._database_server.set_submission_information(drmaa_id_for_db_up,
                                                          datetime.now())  
   
        # --- 7. Update the workflow and jobs status to the database_server -
        ended_job_ids = []
        ended_wf_ids = []
        self.logger.debug("update job and wf status ~~~~~~~~~~~~~~~ ")
        job_status_for_db_up = {}
        for job_id, job in itertools.chain(self._jobs.iteritems(),
                                          wf_jobs.iteritems()):
          job_status_for_db_up[job_id] = job.status
          self._j_wf_ended = self._j_wf_ended and \
                                    (job.status == constants.DONE or \
                                    job.status == constants.FAILED)
          if job_id in self._jobs and \
              (job.status == constants.DONE or \
                job.status == constants.FAILED):
            ended_job_ids.append(job_id)
          self.logger.debug("job " + repr(job_id) + " " + repr(job.status))
       
        if job_status_for_db_up:
          self._database_server.set_jobs_status(job_status_for_db_up)

        if len(ended_jobs):
          self._database_server.set_jobs_exit_info(ended_jobs)

        for wf_id, workflow in self._workflows.iteritems():
          self._database_server.set_workflow_status(wf_id, workflow.status)
          if workflow.status == constants.WORKFLOW_DONE:
            ended_wf_ids.append(wf_id)
          self.logger.debug("wf " + repr(wf_id) + " " + repr(workflow.status))
        self.logger.debug("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ")

        for job_id in ended_job_ids: del self._jobs[job_id]
        for wf_id in ended_wf_ids: del self._workflows[wf_id]

      #if len(self._workflows) == 0 and one_wf_processed: 
      #  break
      time.sleep(time_interval)

    
  def stop_loop(self):
    self._running = False

  def set_queue_limits(self, queue_limits):
    with self._lock:
      self._queue_limits = queue_limits

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
    with self._lock:
      if engine_job.queue in self._pending_queues:
        self._pending_queues[engine_job.queue].append(engine_job)
        self._pending_queues[engine_job.queue].sort(key=lambda job: job.priority, 
                                                    reverse=True)
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
    @type client_workflow: soma_workflow.client.Workflow
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
      if hasattr(transfer, 'client_paths') and transfer.client_paths \
          and not os.path.isdir(transfer.engine_path):
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
      return False
    else:
      with self._lock:
        if job.drmaa_id:
          self.logger.debug("Kill job " + repr(job_id) + " drmaa id: " + repr(job.drmaa_id) + " status " + repr(job.status))
          try:
            self._scheduler.kill_job(job.drmaa_id)
          except DRMError, e:
            #TBI how to communicate the error
            self.logger.error("!!!ERROR!!! %s:%s" %(type(e), e))
        elif job.queue in self._pending_queues and \
            job in self._pending_queues[job.queue]:
          self._pending_queues[job.queue].remove(job)
        job.status = constants.FAILED
        job.exit_status = constants.USER_KILLED
        job.exit_value = None
        job.terminating_signal = None
        job.str_rusage = None

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


  def restart_workflow(self, wf_id, status, queue):
    if wf_id in self._workflows:
      workflow = self._workflows[wf_id]
      workflow.queue = queue
      (jobs_to_run, 
       workflow.status) = workflow.restart(self._database_server, queue)
      for job in jobs_to_run:
        self._pend_for_submission(job)
    else:
      workflow = self._database_server.get_engine_workflow(wf_id, self._user_id)
      workflow.status = status
      (jobs_to_run, workflow.status) = workflow.restart(self._database_server, queue)
      for job in jobs_to_run:
        self._pend_for_submission(job)
      # add to the engine managed workflow list
      with self._lock:
        self._workflows[wf_id] = workflow

  def force_stop(self, wf_id):
    if wf_id in self._workflows:
      pass
    else:
      workflow = self._database_server.get_engine_workflow(wf_id,
                                                           self._user_id)
      workflow.force_stop(self._database_server)
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
  # soma_workflow.database_server.WorkflowDatabaseServer
  _database_server = None
  # WorkflowEngineLoop
  engine_loop = None
  # EngineLoopThread
  engine_loop_thread = None
  # id of the user on the database server
  _user_id = None

  def __init__( self, 
                database_server, 
                scheduler, 
                path_translation=None,
                queue_limits={}):
    ''' 
    @type  database_server:
           L{soma_workflow.database_server.WorkflowDatabaseServer}
    @type  engine_loop: L{WorkflowEngineLoop}
    '''
    
    self.logger = logging.getLogger('engine.WorkflowEngine')
    
    self._database_server= database_server
    
    
    try:
      user_login = getpass.getuser()
    except Exception, e:
      raise EngineError("Couldn't identify user %s: %s \n" %(type(e), e))
    
    self._user_id = self._database_server.register_user(user_login)
    self.logger.debug("user_id : " + repr(self._user_id))

    self.engine_loop = WorkflowEngineLoop(database_server,
                                           scheduler,
                                           path_translation,
                                           queue_limits)
    self.engine_loop_thread = EngineLoopThread(self.engine_loop)
    self.engine_loop_thread.setDaemon(True)
    self.engine_loop_thread.start()

  def __del__( self ):
    pass

  ########## FILE TRANSFER ###############################################

  def register_transfer(self, 
                        file_transfer): 
    '''
    Implementation of soma_workflow.client.WorkflowController API
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
    Implementation of soma_workflow.client.WorkflowController API
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
    
    @type  job: L{soma_workflow.client.Job}
    @param job: job informations 
    '''
    engine_job = self.engine_loop.add_job(job, queue)

    return engine_job.job_id


  def delete_job( self, job_id, force=True ):
    '''
    Implementation of soma_workflow.client.WorkflowController API
    '''
    status = self._database_server.get_job_status(job_id,
                                                  self._user_id)[0]
    if status == constants.DONE or status == constants.FAILED:
      self._database_server.delete_job(job_id)
      return True
    else:
      self._database_server.set_job_status(job_id, constants.DELETE_PENDING)
      if force and not self._wait_for_job_deletion(job_id):
        self.logger.critical("!! The job may not be properly deleted !!")
        self._database_server.delete_job(job_id)
        return False
      return True

  ########## WORKFLOW SUBMISSION ############################################
  
  def submit_workflow(self, workflow, expiration_date, name, queue):
    '''
    Implementation of soma_workflow.client.WorkflowController API
    '''
    if not expiration_date:
      expiration_date = datetime.now() + timedelta(days=7)
    
    wf_id = self.engine_loop.add_workflow(workflow, expiration_date, name, queue)

    return wf_id

  
  def delete_workflow(self, workflow_id, force=True):
    '''
    Implementation of soma_workflow.client.WorkflowController API
    '''
    
    status = self._database_server.get_workflow_status(workflow_id, 
                                                       self._user_id)[0]
    if status == constants.WORKFLOW_DONE:
      self._database_server.delete_workflow(workflow_id)
      return True
    else:
      (is_valid_wf, 
      last_status_update) = self._database_server.is_valid_workflow(workflow_id, 
                                                                  self._user_id)
      if is_valid_wf and _out_to_date(last_status_update):
        self.logger.critical("The workflow may not be properly deleted.")
        self._database_server.delete_workflow(workflow_id)
        return False
      
      self._database_server.set_workflow_status(workflow_id, 
                                                constants.DELETE_PENDING)
      if force and not self._wait_for_wf_deletion(workflow_id):
        self.logger.critical("The workflow may not be properly deleted.")
        self._database_server.delete_workflow(workflow_id)
        return False
      return True


  def stop_workflow(self, workflow_id):

    (status,  
     last_status_update) = self._database_server.get_workflow_status(workflow_id, self._user_id)

    if status != constants.WORKFLOW_DONE:
      if _out_to_date(last_status_update):
        self.engine_loop.force_stop(workflow_id)
        return False
      else:
        self._database_server.set_workflow_status(workflow_id, 
                                                  constants.KILL_PENDING)
        self._wait_wf_status_update(workflow_id, 
                                    expected_status = constants.WORKFLOW_DONE)

    return True


  def change_workflow_expiration_date(self, workflow_id, new_expiration_date):
    '''
    Implementation of soma_workflow.client.WorkflowController API
    ''' 
    if new_expiration_date < datetime.now(): 
      return False
    # TO DO: Add other rules?
    
    self._database_server.change_workflow_expiration_date(workflow_id, 
                                                          new_expiration_date,
                                                          self._user_id)
    return True


  def restart_workflow(self, workflow_id, queue):
    '''
    Implementation of soma_workflow.client.WorkflowController API
    '''
    (status, last_status_update) = self._database_server.get_workflow_status(workflow_id, self._user_id)
    
    if status == constants.WORKFLOW_DONE:
      self.engine_loop.restart_workflow(workflow_id, status, queue)
      self._wait_wf_status_update(workflow_id, 
                                  expected_status=constants.WORKFLOW_IN_PROGRESS)
      return True
    else:
      return False
    
   
  ########## SERVER STATE MONITORING ########################################


  def jobs(self, job_ids=None):
    '''
    Implementation of soma_workflow.client.WorkflowController API
    '''
    return self._database_server.get_jobs(self._user_id, job_ids)
    

  def transfers(self, transfer_ids=None):
    '''
    Implementation of soma_workflow.client.WorkflowController API
    '''
    return self._database_server.get_transfers(self._user_id, transfer_ids)
  
  
  def workflows(self, workflow_ids=None):
    '''
    Implementation of soma_workflow.client.WorkflowController API
    '''
    return self._database_server.get_workflows(self._user_id, workflow_ids)
  

  def workflow(self, wf_id):
    '''
    Implementation of soma_workflow.client.WorkflowController API
    '''
    return self._database_server.get_engine_workflow(wf_id, self._user_id)

  
  def job_status(self, job_id):
    '''
    Implementation of soma_workflow.client.WorkflowController API
    '''
    (status, 
    last_status_update) = self._database_server.get_job_status(job_id,
                                                               self._user_id)
    if status and not status == constants.DONE and \
       not status == constants.FAILED and \
       _out_to_date(last_status_update):
      return constants.WARNING

    return status
        
  
  def workflow_status(self, wf_id):
    '''
    Implementation of soma_workflow.client.WorkflowController API
    '''
    (status, 
     last_status_update) = self._database_server.get_workflow_status(wf_id,                                                              
                                                               self._user_id)

    if status and \
       not status == constants.WORKFLOW_DONE and \
       _out_to_date(last_status_update):
      return constants.WARNING

    return status
    
  
  def workflow_elements_status(self, wf_id, groupe = None):
    '''
    Implementation of soma_workflow.client.WorkflowController API
    '''
    (status, 
     last_status_update) = self._database_server.get_workflow_status(wf_id,
                                                                  self._user_id)
    
    wf_status = self._database_server.get_detailed_workflow_status(wf_id)
    if status and \
       not status == constants.WORKFLOW_DONE and \
       _out_to_date(last_status_update):
      wf_status = (wf_status[0], wf_status[1], constants.WARNING, wf_status[3], wf_status[4])
    
    return wf_status
        
        
  def transfer_status(self, engine_path):
    '''
    Implementation of soma_workflow.client.WorkflowController API
    '''
    transfer_status = self._database_server.get_transfer_status(engine_path,
                                                                self._user_id)  
    return transfer_status


  def job_termination_status(self, job_id ):
    '''
    Implementation of soma_workflow.client.WorkflowController API
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
    Implementation of soma_workflow.client.WorkflowController API
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
          if last_status_update and _out_to_date(last_status_update):
            raise EngineError("wait_job: Could not wait for job %s. " 
                              "The process updating its status failed." %(jid))
       
 
  def restart_job( self, job_id ):
    '''
    Implementation of soma_workflow.client.WorkflowController API
    '''

    (status, 
     last_status_update) = self._database_server.get_job_status(job_id,
                                                                self._user_id)
    
    if status == constants.FAILED or _out_to_date(last_status_update):
      self.engine_loop.restart_job(job_id, status)
      return True
    else:
      return False


  def kill_job( self, job_id ):
    '''
    Implementation of soma_workflow.client.WorkflowController API
    '''
    
    (status,
    last_status_update) = self._database_server.get_job_status(job_id,
                                                  self._user_id)

    if status != constants.DONE and status != constants.FAILED:
      if _out_to_date(last_status_update):
        # TO DO
        pass
      else:
        self._database_server.set_job_status(job_id, 
                                             constants.KILL_PENDING)
      
      self._wait_job_status_update(job_id)



  def _wait_job_status_update(self, job_id):
    self.logger.debug(">> _wait_job_status_update")
    try:
      (status, 
      last_status_update) = self._database_server.get_job_status(job_id,
                                                                 self._user_id)
      while status and not status == constants.DONE and \
            not status == constants.FAILED and \
            not _out_to_date(last_status_update):
        time.sleep(refreshment_interval)
        (status, 
        last_status_update) = self._database_server.get_job_status(job_id,
                                                                  self._user_id) 
    except UnknownObjectError, e:
      pass
    self.logger.debug("<< _wait_job_status_update")


  def _wait_for_job_deletion(self, job_id):
    self.logger.debug(">> _wait_for_job_deletion")
    (is_valid_job, 
     last_status_update) = self._database_server.is_valid_job(job_id, 
                                                              self._user_id)
    while is_valid_job and not _out_to_date(last_status_update):
      time.sleep(refreshment_interval)
      (is_valid_job, 
       last_status_update) = self._database_server.is_valid_job(job_id, 
                                                                self._user_id)
    self.logger.debug("<< _wait_for_job_deletion")
    if _out_to_date(last_status_update):
      return False
    return True


  def _wait_wf_status_update(self, wf_id, expected_status):  
    self.logger.debug(">> _wait_wf_status_update")
    try:
      (status, 
      last_status_update) = self._database_server.get_workflow_status(wf_id,
                                                                 self._user_id)
      if status != None and status != expected_status:
        time.sleep(refreshment_interval)
        (status, 
        last_status_update) = self._database_server.get_workflow_status(wf_id,
                                                                  self._user_id) 
        if status != None and status != expected_status:
          time.sleep(refreshment_interval)
          (status, 
          last_status_update) = self._database_server.get_workflow_status(wf_id,
                                                                    self._user_id)
          while status != None and \
                status != expected_status and \
                status != constants.WORKFLOW_DONE and \
                not _out_to_date(last_status_update):
            time.sleep(refreshment_interval)
            (status, 
            last_status_update) = self._database_server.get_workflow_status(wf_id,
                                                                      self._user_id) 
    except UnknownObjectError, e:
      pass
    self.logger.debug("<< _wait_wf_status_update")


  def _wait_for_wf_deletion(self, wf_id):
    self.logger.debug(">> _wait_for_wf_deletion")
    (is_valid_wf, 
    last_status_update) = self._database_server.is_valid_workflow(wf_id, 
                                                                self._user_id)
    while is_valid_wf and \
          not _out_to_date(last_status_update):
      time.sleep(refreshment_interval)
      (is_valid_wf, 
      last_status_update) = self._database_server.is_valid_workflow(wf_id, 
                                                              self._user_id)     
    self.logger.debug("<< _wait_for_wf_deletion")
    if _out_to_date(last_status_update):
      return False
    return True

class ConfiguredWorkflowEngine(WorkflowEngine):
  '''
  WorkflowEngineLoop synchronized with a configuration object.
  '''

  config = None
  
  def __init__(self, 
               database_server, 
               scheduler, 
               config):
    '''
    * config *configuration.Configuration*
    '''
    super(ConfiguredWorkflowEngine, self).__init__(
                               database_server,
                               scheduler,
                               path_translation=config.get_path_translation(),
                               queue_limits=config.get_queue_limits())

    self.config = config
    
    self.config.addObserver(self,
                            "update_from_config",
                            [Configuration.QUEUE_LIMITS_CHANGED])
    # set temp path in EngineTemporaryPath
    EngineTemporaryPath.temporary_directory \
      = config.get_shared_temporary_directory()

  def update_from_config(self, observable, event, msg):
    if event == Configuration.QUEUE_LIMITS_CHANGED:
      self.engine_loop.set_queue_limits(self.config.get_queue_limits())
    self.config.save_to_file()

    
    
