from __future__ import with_statement

import subprocess
import threading
import soma.workflow.constants as constants
import time


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

  def get_job_exit_info(self, drmaa_job_id):
    '''
    * scheduler_job_id *string*
        Job id for the scheduling system (DRMAA for example)
    * return: *tuple*
        exit_status, exit_value, term_sig, resource_usage
    '''
    raise Exception("Scheduler is an abstract class!")

  def kill_job(self, job_drmaa_id):
    '''
    * scheduler_job_id *string*
        Job id for the scheduling system (DRMAA for example)
    '''
    raise Exception("Scheduler is an abstract class!")



class LocalScheduler(object):
  '''
  Allow to submit, kill and get the status of jobs.
  Run on one machine without dependencies.

  * _nb_proc *int*

  * _queue *list of soma.workflow.engine_types.EngineJob*
  
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
    self._processes = {}
    self._status = {}
    self._exit_info = {}

    print "local_scheduler nb proc: " + repr(self._nb_proc) 

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

    print "#############################"
    # Control the running jobs
    ended_jobs = []
    for job_id, process in self._processes.iteritems():
      ret_value = process.poll()
      print "job_id " + repr(job_id) + " ret_value " + repr(ret_value)
      if ret_value != None:
        ended_jobs.append(job_id) 
        self._exit_info[job_id] = (constants.FINISHED_REGULARLY,
                                     ret_value,
                                     None,
                                     None)

    # update for the ended job
    for job_id in ended_jobs:
      print "updated job_id " + repr(job_id) + " status DONE"
      self._status[job_id] = constants.DONE
      del self._processes[job_id]

    # run new jobs
    while self._queue and \
          len(self._processes) < self._nb_proc:
      job = self._queue.pop(0)
      print "new job " + repr(job.job_id)
      self._processes[job.job_id] = self._create_process(job)
      self._status[job.job_id] = constants.RUNNING

  def _create_process(self, engine_job):
    separator = " "
    command = separator.join(engine_job.plain_command())
    print "command " +  repr(command)

    stdin = engine_job.plain_stdin()
    stdin_file = None
    if stdin:
      stdin_file = open(stdin, "rb")
    
    stdout = engine_job.plain_stdout()
    stdout_file = None
    if stdout:
      stdout_file = open(stdout, "wb")

    stderr = engine_job.plain_stderr()
    stderr_file = None
    if stderr:
      stderr_file = open(stderr, "wb")

    process = subprocess.Popen(command,
                               shell=True,
                               stdin=stdin_file,
                               stdout=stdout_file,
                               stderr=stderr_file)
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
      self._queue.append(job)
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
      print "kill job " + repr(scheduler_job_id)
      if scheduler_job_id in self._processes:
        self._processes[scheduler_job_id].kill()
        del self._processes[scheduler_job_id]
        self._status[scheduler_job_id] = constants.FAILED
        self._exit_info[scheduler_job_id] = (constants.USER_KILLED,
                                              None,
                                              None,
                                              None)
      elif scheduler_job_id in self._queue:
        del self._queue[scheduler_job_id]
        self._status[scheduler_job_id] = constants.FAILED
        self._exit_info[scheduler_job_id] = (constants.EXIT_ABORTED,
                                              None,
                                              None,
                                              None)
