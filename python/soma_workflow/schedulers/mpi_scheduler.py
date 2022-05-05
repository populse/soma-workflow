# -*- coding: utf-8 -*-

from __future__ import print_function
from __future__ import absolute_import
from .. import scheduler
import threading
import logging
import socket
import six
from mpi4py import MPI
from .. import constants


class MPIScheduler(scheduler.Scheduler):

    '''
    Allow to submit, kill and get the status of jobs.
    '''
    parallel_job_submission_info = None

    logger = None

    is_sleeping = None

    _proc_nb = None

    _queue = None

    _jobs = None

    _processes = None

    _status = None

    _exit_info = None

    _loop = None

    _interval = None

    _lock = None

    _failed_count = None

    JOB_REQUEST = 11
    JOB_SENDING = 12
    EXIT_SIGNAL = 13
    JOB_KILL = 14
    JOB_RESULT = 15
    NO_JOB = 16

    def __init__(self, communicator, interval=0.01, nb_attempt_per_job=1):
        super(MPIScheduler, self).__init__()

        self._communicator = communicator
        self.parallel_job_submission_info = None
        # self._proc_nb = proc_nb
        self._queue = []
        self._jobs = {}
        self._fail_count = {}  # job_id -> nb of fail
        # self._processes = {}
        self._status = {}
        self._exit_info = {}
        self._lock = threading.RLock()
        self.stop_thread_loop = False
        self._interval = interval

        self._nb_attempt_per_job = nb_attempt_per_job

        self._logger = logging.getLogger("testMPI")

        def master_loop(self):
            self._stopped_slaves = 0
            while not self.stop_thread_loop:
                self._master_iteration()
                # time.sleep(0)#self._interval)

        self._loop = threading.Thread(name="scheduler_loop",
                                      target=master_loop,
                                      args=[self])
        self._loop.daemon = True
        self._loop.start()

    def end_scheduler_thread(self):
        with self._lock:
            self.stop_thread_loop = True
            self._loop.join()
            print("[host: " + socket.gethostname() + "] "
                  + "Soma scheduler thread ended nicely.")

    def _master_iteration(self):
        MPIStatus = MPI.Status()
        # if not self._queue:
        #    return
        self._communicator.Probe(source=MPI.ANY_SOURCE,
                                 tag=MPI.ANY_TAG,
                                 status=MPIStatus)
        with self._lock:
            t = MPIStatus.Get_tag()
            if t == MPIScheduler.JOB_REQUEST:
                # self._logger.debug("Master received the JOB_REQUEST signal")
                s = MPIStatus.Get_source()
                if not self._queue:
                    # self._logger.debug("Master No job for now")
                    self._communicator.recv(source=s,
                                            tag=MPIScheduler.JOB_REQUEST)
                    self._communicator.send("No job for now",
                                            dest=s,
                                            tag=MPIScheduler.NO_JOB)
                else:
                    self._logger.debug("[host: " + socket.gethostname() + "] "
                                       + "Master send a Job !!!")
                    self._communicator.recv(
                        source=s, tag=MPIScheduler.JOB_REQUEST)
                    job_id = self._queue.pop(0)
                    job_list = [self._jobs[job_id]]
                    self._communicator.send(job_list, dest=s,
                                            tag=MPIScheduler.JOB_SENDING)
                    for j in job_list:
                        self._status[j.job_id] = constants.RUNNING
            elif t == MPIScheduler.JOB_RESULT:
                # self._logger.debug("Master received the JOB_RESULT signal")
                s = MPIStatus.Get_source()
                ended_jobs_info = self._communicator.recv(
                    source=s,
                    tag=MPIScheduler.JOB_RESULT)
                for job_id, end_info in six.iteritems(ended_jobs_info):
                    job_status, exit_info = end_info
                    ret_value = exit_info[1]
                    try_new_attempt = False
                    if ret_value != 0 and \
                       (job_id not in self._fail_count or
                            self._fail_count[job_id] < self._nb_attempt_per_job):
                        if job_id in self._fail_count:
                            self._fail_count[job_id] += 1
                        else:
                            self._fail_count[job_id] = 1

                        if self._fail_count[job_id] < self._nb_attempt_per_job:
                            try_new_attempt = True

                        self._logger.debug(
                            "[host: " + socket.gethostname() + "] "
                             + repr(self._fail_count[job_id])
                             + " fails for job " + repr(job_id)
                             + " (ret value " + repr(ret_value) + ")")

                    if try_new_attempt:
                        self._queue.insert(0, job_id)

                    else:
                        self._exit_info[job_id] = exit_info
                        self._status[job_id] = job_status
                        job = self._jobs[job_id]
                        if job.signal_end:
                            # trigger the event to the engine
                            self.jobs_finished_event.set()

            elif t == MPIScheduler.EXIT_SIGNAL:
                # self._logger.debug("Master received the EXIT_SIGNAL")
                self._stopped_slaves = self._stopped_slaves + 1
                if self._stopped_slaves == self._communicator.size - 1:
                    self.stop_thread_loop = True
            else:
                self._logger.critical("[host: " + socket.gethostname() + "] "
                                      + "Master unknown tag")

    def sleep(self):
        self.is_sleeping = True

    def wake(self):
        self.is_sleeping = False

    def clean(self):
        pass

    def queued_job_count(self):
        return len(self._queue)

    def job_submission(self, jobs):
        '''
        * job *EngineJob*
        * return: *string*
        Job id for the scheduling system (DRMAA for example)
        '''
        print('MPI job_submission:', len(jobs))
        for job in jobs:
            if not job.job_id or job.job_id == -1:
                raise Exception("Invalid job: no id")
        # self._logger.debug(">> job_submission wait lock")
        drmaa_ids = []

        with self._lock:
            # self._logger.debug(">> job_submission wait lock END")
            for job in jobs:
                self._queue.append(job.job_id)
                self._jobs[job.job_id] = job
                self._status[job.job_id] = constants.QUEUED_ACTIVE
                drmaa_ids.append(job.job_id)
            self._queue.sort(key=lambda job_id: self._jobs[job_id].priority,
                             reverse=True)
            self._logger.debug("[host: " + socket.gethostname() + "] "
                               + "%d Jobs were submitted." % len(jobs))
        return drmaa_ids

    def get_job_status(self, scheduler_job_id):
        '''
        * scheduler_job_id *string*
        Job id for the scheduling system (DRMAA for example)
        * return: *string*
        Job status as defined in constants.JOB_STATUS
        '''
        if not scheduler_job_id in self._status:
            raise Exception("Unknown job.")
        status = self._status[scheduler_job_id]
        return status

    def get_job_exit_info(self, scheduler_job_id):
        '''
        * scheduler_job_id *string*
        Job id for the scheduling system (DRMAA for example)
        * return: *tuple*
        exit_status, exit_value, term_sig, resource_usage
        '''
        with self._lock:
            exit_info = self._exit_info[scheduler_job_id]
            del self._exit_info[scheduler_job_id]
            del self._jobs[scheduler_job_id]
            del self._status[scheduler_job_id]
        return exit_info

    def kill_job(self, scheduler_job_id):
        '''
        * scheduler_job_id *string*
        Job id for the scheduling system (DRMAA for example)
        '''
        # TODO
        pass

    @classmethod
    def build_scheduler(cls, config):
        sch = MPIScheduler(MPI.COMM_WORLD)
        return sch
