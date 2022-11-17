# -*- coding: utf-8 -*-

#-----------------------------------------------------------------------------
# Imports
#-----------------------------------------------------------------------------

from __future__ import with_statement, print_function
from __future__ import absolute_import
from datetime import date, timedelta, datetime
import threading
import getpass
import os
import time
import logging
import stat
import hashlib
import operator
import itertools
import atexit
import six
import weakref
import sys
import json
import importlib

# import cProfile
# import traceback

from soma_workflow.engine_types import EngineJob, EngineWorkflow, EngineTransfer, EngineTemporaryPath, FileTransfer, SpecialPath, TemporaryPath
import soma_workflow.constants as constants
from soma_workflow.client import WorkflowController, EngineExecutionJob
from soma_workflow.errors import JobError, UnknownObjectError, EngineError, DRMError
from soma_workflow.transfer import RemoteFileController
from soma_workflow.configuration import Configuration
from soma_workflow.param_link_functions import *
from soma_workflow import utils

#-----------------------------------------------------------------------------
# Globals and constants
#-----------------------------------------------------------------------------

refreshment_interval = 1.  # seconds
# if the last status update is older than the refreshment_timeout
# the status is changed into WARNING
refreshment_timeout = 90  # seconds


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


def transformed_param_value(func, src_param, value, param, dval):
    if func is None:
        return value
    if isinstance(func, tuple):
        f = func[0]
        params = func[1:]
    else:
        f = func
        params = []
    mod_func = f.rsplit('.', 1)
    if len(mod_func) == 1:
        module = sys.modules[__name__]
        func_name = mod_func[0]
    else:
        mod_name, func_name = mod_func
        module = importlib.import_modulle(mod_name)
    func = getattr(module, func_name)
    return func(*params, src_param=src_param, value=value,
                dst_param=param, dst_value=dval)


#-----------------------------------------------------------------------------
# Classes and functions
#-----------------------------------------------------------------------------

class EngineLoopThread(threading.Thread):

    def __init__(self, engine_loop):
        super(EngineLoopThread, self).__init__()
        self.engine_loop = engine_loop
        self.time_interval = refreshment_interval
        atexit.register(EngineLoopThread.stop_loop,
                        weakref.ref(self.engine_loop))

    def __del__(self):
        self.stop()

    def run(self):
        # cProfile.runctx("self.engine_loop.start_loop(self.time_interval)",
        # globals(), locals(), "/tmp/profile_loop_thread")
        self.engine_loop.start_loop(self.time_interval)

    def stop(self):
        self.engine_loop.stop_loop()
        # when python3 exits, join() may block on its lock indefinitely
        # so we have to use a timeout.
        self.join(timeout=3)
        # print("Soma workflow engine thread ended nicely.")

    @staticmethod
    def stop_loop(loop_ref):
        loop_thread = loop_ref()
        if loop_thread is not None:
            loop_thread.stop_loop()


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
    _user_login = None
    # for each namespace a dictionary holding the traduction
    #  (association uuid => engine path)
    # dictionary, namespace => uuid => path
    _path_translation = None
    # max number of job for some queues
    # dictionary, queue name (str) => max nb of job (int)
    _queue_limits = None
    # max number of running (+queued) job for some queues
    # dictionary, queue name (str) => max nb of job (int)
    _running_jobs_limits = None
    # Submission pending queues.
    # For each limited queue, a submission pending queue is needed to store the
    # jobs that couldn't be submitted.
    # Dictionary queue name (str) => pending jobs (list)
    _pending_queues = None
    # boolean
    _running = None

    _lock = None

    logger = None

    def __init__(self,
                 database_server,
                 scheduler,
                 path_translation=None,
                 queue_limits={},
                 running_jobs_limits={}):

        self.logger = logging.getLogger('engine.WorkflowEngineLoop')

        self._jobs = {}
        self._workflows = {}

        self._database_server = database_server

        self._scheduler = scheduler

        self._path_translation = path_translation

        self._queue_limits = queue_limits

        self._running_jobs_limits = running_jobs_limits

        self.logger.debug('queue_limits ' + repr(self._queue_limits))
        self.logger.debug(
            'running_jobs_limits ' + repr(self._running_jobs_limits))

        self._pending_queues = {}

        # The running flag is set to True at the beginning, not in start_loop(),
        # to overcome race conditions which may occur in this situation:
        # * intantiate a WorkflowEngineThread (wet)
        # * wet.start() it -> a thread is created for it
        # * wet.stop() it immediately. _running is set to False
        # * the thread may execute later, effectively starting the loop. But
        #   stop() has already been called, and nobody can know about it, so the
        #   loop starts, and never stops.
        #
        # Note: this is not an ideal solution either:
        # calling start_loop(), then stop_loop(), then start_loop() again
        # will not restart the loop unless _running has manually be reset to
        # True.
        self._running = True

        try:
            userLogin = getpass.getuser()
        except Exception as e:
            self.logger.critical(
                "Couldn't identify user %s: %s \n" % (type(e), e))
            raise EngineError(
                "Couldn't identify user %s: %s \n" % (type(e), e))

        self._user_id = self._database_server.register_user(userLogin)
        self._user_login = userLogin
        self.logger.debug("user_id : " + repr(self._user_id))

        self._lock = threading.RLock()
        # counter which may be used to synchronize things
        self._loop_count = 0

    def are_jobs_and_workflow_done(self):
        with self._lock:
            ended = len(self._jobs) == 0 and len(self._workflows) == 0
            return ended

    def start_loop(self, time_interval):
        '''
        Start the workflow engine loop. The loop will run until stop() is
        called.
        '''
        # one_wf_processed = False
        # Modif: don't set the running flag here, because the loop may be
        # already
        # stopped from another thread (typically the main thread) when we
        # get here (typically in a secondary thread)
        # self._running = True

        drms_error_jobs = {}
        idle_cmpt = 0
        prev_jobs_status = {}

        while True:
            if not self._running:
                break
            with self._lock:
                #print('engine loop 0')
                # clear scheduler events
                self._scheduler.jobs_finished_event.clear()

                ended_jobs = drms_error_jobs  # {}
                wf_to_inspect = set()  # set of workflow id
                for job in six.itervalues(drms_error_jobs):
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

                # --- 1. Jobs and workflow deletion and kill ------------------
                # Get the jobs and workflow with the status DELETE_PENDING
                # and KILL_PENDING
                #print('engine loop 1')
                jobs_to_delete = []
                jobs_to_kill = []
                (jobs_to_delete, jobs_to_kill) \
                    = self._database_server.jobs_to_delete_and_kill(
                        self._user_id)
                wf_to_delete = []
                wf_to_kill = []
                if self._workflows:
                    (wf_to_delete, wf_to_kill) \
                        = self._database_server.workflows_to_delete_and_kill(
                            self._user_id)
                # Delete and kill properly the jobs and workflows in _jobs and
                # _workflows
                for job_id in jobs_to_kill + jobs_to_delete:
                    job = self._jobs.get(job_id)
                    if job is None:
                        # look in workflows
                        for wf in six.itervalues(self._workflows):
                            jobs = [j
                                    for jid, j
                                    in six.iteritems(wf.registered_jobs)
                                    if jid == job_id]
                            if len(jobs) == 1:
                                job = jobs[0]
                                break
                    if job is not None:
                    # if job_id in self._jobs:
                        self.logger.debug(" stop job " + repr(job_id))
                        try:
                            stopped = self._stop_job(job_id,  job)
                        except DRMError as e:
                            # TBI how to communicate the error ?
                            self.logger.error(
                                "!!!ERROR!!! stop job %s :%s" % (type(e), e))
                        if job_id in jobs_to_delete and job_id in self._jobs:
                            self.logger.debug("Delete job : " + repr(job_id))
                            self._database_server.delete_job(job_id)
                            del self._jobs[job_id]
                        else:
                            self._database_server.set_job_status(job_id,
                                                                 job.status,
                                                                 force=True)
                            if stopped:
                                ended_jobs[job_id] = job
                                if job.workflow_id != -1:
                                    wf_to_inspect.add(job.workflow_id)

                for wf_id in wf_to_kill + wf_to_delete:
                    if wf_id in self._workflows:
                        self.logger.debug("Kill workflow : " + repr(wf_id))
                        ended_jobs_in_wf = self._stop_wf(wf_id)
                        if wf_id in wf_to_delete:
                            self.logger.debug(
                                "Delete workflow : " + repr(wf_id))
                            self._database_server.delete_workflow(wf_id)
                            del self._workflows[wf_id]
                        else:
                            ended_jobs.update(ended_jobs_in_wf)
                            wf_to_inspect.add(wf_id)

                # --- 2. Update job status from the scheduler -----------------
                # get back the termination status and terminate the jobs which
                # ended
                #print('engine loop 2')
                wf_jobs = {}
                wf_transfers = {}
                wf_tmp = {}
                for wf in six.itervalues(self._workflows):
                    # one_wf_processed = True
                    # TBI add a condition on the workflow status
                    wf_jobs.update(wf.registered_jobs)
                    wf_transfers.update(wf.registered_tr)
                    wf_tmp.update(wf.registered_tmp)

                for job in itertools.chain(six.itervalues(self._jobs),
                                           six.itervalues(wf_jobs)):
                    if job.exit_status == None and job.drmaa_id != None:
                        try:
                            job.status = self._scheduler.get_job_status(
                                job.drmaa_id)
                        except DRMError as e:
                            self.logger.info(
                                "!!!ERROR!!! get_job_status %s: %s" % (type(e), e))
                            job.status = constants.FAILED
                            job.exit_status = constants.EXIT_ABORTED
                            stderr_file = open(job.stderr_file, "wa")
                            stderr_file.write(
                                "Error while requesting the job status %s: %s \nWarning: the job may still be running.\n" % (type(e), e))
                            stderr_file.close()
                            drms_error_jobs[job.job_id] = job
                        self.logger.debug(
                            "job " + repr(job.job_id) + " : " + job.status)
                        if job.status == constants.DONE \
                                or job.status == constants.FAILED:
                            self.logger.debug(
                                "End of job %s, drmaaJobId = %s, status= %s",
                                job.job_id, job.drmaa_id, repr(job.status))
                            try:
                                (job.exit_status,
                                 job.exit_value,
                                 job.terminating_signal,
                                 job.str_rusage) \
                                    = self._scheduler.get_job_exit_info(
                                        job.drmaa_id)
                            except Exception as e:
                                self.logger.error(
                                    'exception in get_job_exit_info: %s' % repr(e))
                                raise

                            self.logger.debug("  after get_job_exit_info ")
                            self.logger.debug(
                                "  => exit_status " + repr(job.exit_status))
                            self.logger.debug(
                                "  => exit_value " + repr(job.exit_value))
                            self.logger.debug(
                                "  => signal " + repr(job.terminating_signal))
                            self.logger.debug(
                                "  => rusage " + repr(job.str_rusage))

                            if job.workflow_id != -1:
                                wf_to_inspect.add(job.workflow_id)
                            if job.status == constants.DONE:
                                for ft in job.referenced_output_files:
                                    if isinstance(ft, FileTransfer):
                                        transfer_id = job.transfer_mapping[
                                            ft].transfer_id
                                        self._database_server.set_transfer_status(
                                            transfer_id,
                                            constants.FILES_ON_CR)
                                    else:
                                        # TemporaryPath
                                        temp_path_id = job.transfer_mapping[
                                            ft].temp_path_id
                                        self._database_server.set_temporary_status(
                                            temp_path_id,
                                            constants.FILES_ON_CR)
                                self.read_job_output_dict(job)

                            ended_jobs[job.job_id] = job
                            self.logger.debug(
                                "  => exit_status " + repr(job.exit_status))
                            self.logger.debug(
                                "  => exit_value " + repr(job.exit_value))
                            self.logger.debug(
                                "  => signal " + repr(job.terminating_signal))

                # --- 3. Get back transfered status ---------------------------
                #print('engine loop 3')
                for transfer_id, transfer in six.iteritems(wf_transfers):
                    try:
                        status = self._database_server.get_transfer_status(
                            transfer_id,
                            self._user_id)
                        transfer.status = status
                    except Exception:
                        self.logger.exception('WorkflowEngineLoop')
                for tmp_id, tmp in six.iteritems(wf_tmp):
                    try:
                        status = self._database_server.get_temporary_status(
                            tmp_id,
                            self._user_id)
                        tmp.status = status
                    except Exception:
                        logger.exception()

                for wf_id in six.iterkeys(self._workflows):
                    if self._database_server.pop_workflow_ended_transfer(wf_id):
                        self.logger.debug(
                            "ended transfer for the workflow " + repr(wf_id))
                        wf_to_inspect.add(wf_id)

                # --- 4. Inspect workflows ------------------------------------
                #print('engine loop 4')
                self.logger.debug("wf_to_inspect " + repr(wf_to_inspect))
                for wf_id in wf_to_inspect:
                    (to_run,
                     aborted_jobs,
                     status) = self._workflows[wf_id].find_out_jobs_to_process()
                    self.logger.debug(
                        "to_run=" + repr(to_run) + " aborted_jobs=" + repr(aborted_jobs))
                    self._workflows[wf_id].status = status
                    self.logger.debug(
                        "NEW status wf " + repr(wf_id) + " " + repr(status))
                    ended_jobs.update(aborted_jobs)
                    self._pend_for_submission(to_run)

                # --- 5. Check if pending jobs can now be submitted -----------
                #print('engine loop 5')
                self.logger.debug("Check pending jobs")
                jobs_to_run = self._get_pending_job_to_submit()
                self.logger.debug("jobs_to_run=" + repr(jobs_to_run))
                self.logger.debug("len(jobs_to_run)=" + repr(len(jobs_to_run)))

                # --- 6. Submit jobs ------------------------------------------
                #print('engine loop 6')
                drmaa_id_for_db_up = {}
                # set dynamic paramters from upstream outputs
                self.update_jobs_parameters(jobs_to_run)
                #print('engine loop 6.1')

                if jobs_to_run:
                    drmaa_ids = self._scheduler.job_submission(jobs_to_run)
                    for job, drmaa_id in zip(jobs_to_run, drmaa_ids):
                        if drmaa_id is None:
                            # Resubmission ?
                            # if job.queue in self._pending_queues:
                            #  self._pending_queues[job.queue].insert(0, job)
                            # else:
                            #  self._pending_queues[job.queue] = [job]
                            # job.status = constants.SUBMISSION_PENDING
                            self.logger.debug(
                                "job %s !!!ERROR!!! %s: %s"
                                % (repr(job.command),
                                                              type(e), e))
                            job.status = constants.FAILED
                            job.exit_status = constants.EXIT_ABORTED
                            stderr_file = open(job.stderr_file, "a")
                            self.logger.debug('Job fail, stderr opened')
                            stderr_file.write(
                                "Error while submitting the job %s:\n%s\n"
                                % (type(e), e))
                            stderr_file.close()
                            drms_error_jobs[job.job_id] = job
                        else:
                            job.drmaa_id = drmaa_id
                            drmaa_id_for_db_up[job.job_id] = job.drmaa_id
                            if job.is_engine_execution:
                                # Engine execution jobs immediately get the
                                # status
                                # DONE to avoid losing one time cycle
                                job.status = constants.DONE
                            else:
                                job.status = constants.UNDETERMINED

                #print('engine loop 6.2')
                if drmaa_id_for_db_up:
                    self._database_server.set_submission_information(
                        drmaa_id_for_db_up,
                        datetime.now())

                # --- 7. Update the workflow and jobs status to the database_server -
                #print('engine loop 7')
                ended_job_ids = []
                ended_wf_ids = []
                self.logger.debug("update job and wf status ~~~~~~~~~~~~~~~ ")
                job_status_for_db_up = {}
                new_jobs_status = {}
                for job_id, job in itertools.chain(six.iteritems(self._jobs),
                                                   six.iteritems(wf_jobs)):
                    prev_status = prev_jobs_status.get(job_id)
                    if prev_status != job.status:
                        job_status_for_db_up[job_id] = job.status
                    new_jobs_status[job_id] = job.status
                    if job_id in self._jobs and \
                        (job.status == constants.DONE or
                         job.status == constants.FAILED):
                        ended_job_ids.append(job_id)
                    self.logger.debug(
                        "job " + repr(job_id) + " " + repr(job.status))
                prev_jobs_status = new_jobs_status

                if job_status_for_db_up:
                    #print('update jobs status:', len(job_status_for_db_up))
                    self._database_server.set_jobs_status(job_status_for_db_up)

                if len(ended_jobs):
                    self._database_server.set_jobs_exit_info(ended_jobs)

                for wf_id, workflow in six.iteritems(self._workflows):
                    force = False
                    if wf_id in wf_to_kill + wf_to_delete:
                        force = True
                    self.logger.debug("set workflow status for: %s, status: %s"
                                      % (wf_id, workflow.status))
                    self._database_server.set_workflow_status(
                        wf_id, workflow.status,
                        force=force)
                    if workflow.status == constants.WORKFLOW_DONE:
                        ended_wf_ids.append(wf_id)
                    self.logger.debug(
                        "wf " + repr(wf_id) + " " + repr(workflow.status))
                self.logger.debug("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ")

                for job_id in ended_job_ids:
                    del self._jobs[job_id]
                for wf_id in ended_wf_ids:
                    del self._workflows[wf_id]

            # if len(self._workflows) == 0 and one_wf_processed:
            #  break
            self._loop_count += 1
            #print('engine loop 8')
            self._scheduler.jobs_finished_event.wait(time_interval)
            #print('engine loop 9')

    def read_job_output_dict(self, job):
        if job.has_outputs:
            output_dict = None
            if issubclass(job.job_class, EngineExecutionJob):
                output_dict = job.job_class.engine_execution(job)
            elif job.output_params_file is not None:
                if os.path.exists(
                        job.plain_output_params_file()):
                    try:
                        with open(job.plain_output_params_file()) as f:
                            output_dict = utils.from_json(json.load(f))
                    except Exception:
                        self.logger.info('unable to read output parameters '
                                         'file: %s' % job.plain_output_params_file())
            if not output_dict:
                return
            self._database_server.set_job_output_params(job.job_id,
                                                        output_dict,
                                                        self._user_id)
            for param, value in six.iteritems(output_dict):
                jvalue = job.param_dict.get(param)
                if jvalue and isinstance(jvalue, FileTransfer):
                    engine_transfer = job.transfer_mapping[jvalue]
                    engine_transfer.set_engine_path(
                        value,
                        *engine_transfer.map_client_path_to_engine(
                            value, job.param_dict))
                    # update database with modified values
                    self._database_server.set_transfer_paths(
                        engine_transfer.transfer_id, value,
                        engine_transfer.client_path,
                        engine_transfer.client_paths)

    def update_jobs_parameters(self, jobs):
        '''
        Set job parameters from upstream outputs, set the env variables
        SOMAWF_INPUT_PARAMS and / or SOMAWF_OUTPUT_PARAMS when they are used,
        and write the input parameters file if needed.

        If the job has a configuration dict, it will be included as a parameter
        named "configuration_dict".
        '''
        u_param_dicts = self._database_server.updated_jobs_parameters(
            [job.job_id for job in jobs])

        for job in jobs:
            u_param_dict = u_param_dicts.get(job.job_id)

            # I don't understand any longer what I have done.
            # we iterate over params (OK).
            #   then on each link for the value (value_tl):
            #      we get an output value and re-iterate over sub-values
            #      then set the whole param, forgetting all previous links
            # if this is right we may process only the last link ?
            # seems are done many times here...
            if u_param_dict:
                for param, value_tl in six.iteritems(u_param_dict):
                    #print('+' + '-' * 78 + '+')
                    #print('update_job_parameters', param, ':', value_tl)
                    #print('job:', job.name, job)
                    #print('+' + '-' * 78 + '+')
                    for value_t in value_tl:
                        # dval should be reread from job parameters dictionary
                        # as it could have been updated since previous iteration
                        dval = job.param_dict.get(param)
                        #print('dval:', repr(dval))
                        
                        func, src_param, value = value_t
                        nvalue = transformed_param_value(func, src_param,
                                                         value, param, dval)
                        #print('resulting nvalue', nvalue)
                        if isinstance(nvalue, list):
                            values = nvalue
                            dvals = dval
                        else:
                            values = [nvalue]
                            dvals = [dval]
                        new_values = []
                        for i in range(len(values)):
                            value = values[i]
                            if i < len(dvals):
                                val = dvals[i]
                                if isinstance(val, SpecialPath) \
                                        and not isinstance(val, TemporaryPath):
                                    # temp paths are replaced as normal files
                                    # if they happen to get a new value in an
                                    # output
                                    engine_transfer = job.transfer_mapping[val]
                                    former_path = engine_transfer.engine_path
                                    if former_path != value:
                                        engine_transfer.set_engine_path(
                                            value,
                                            *engine_transfer.map_client_path_to_engine(
                                                value, job.param_dict))
                                        # update database with modified values
                                        self._database_server.set_transfer_paths(
                                            engine_transfer.transfer_id, value,
                                            engine_transfer.client_path,
                                            engine_transfer.client_paths)
                                    new_values.append(val)
                                else:
                                    new_values.append(value)
                            else:
                                new_values.append(value)
                        if not isinstance(nvalue, list):
                            new_values = new_values[0]
                        job.param_dict[param] = new_values
                        
                        # This break lead to fail to update output lists
                        # when multiple outputs result in an input list value.
                        # With this break, only the first value of the input 
                        # list is updated. Break does not seem to be a good
                        # optimization
                        #break  # avoid doing this multiple times
                    #print('+' + '-' * 78 + '+')
                    #print()
            if u_param_dict or \
                    (job.configuration and not job.use_input_params_file):
                if job.configuration and not job.use_input_params_file:
                    job.param_dict['configuration_dict'] \
                        = json.dumps(job.configuration)
                    print('configuration_dict included', file=sys.stderr)
                self._database_server.update_job_command(job.job_id,
                                                        job.plain_command())
            if job.use_input_params_file and job.input_params_file:
                if job.env is None:
                    job.env = {}
                job.env['SOMAWF_INPUT_PARAMS'] = job.plain_input_params_file()
                job.write_input_params_file()
            if job.has_outputs and job.output_params_file:
                if job.env is None:
                    job.env = {}
                job.env['SOMAWF_OUTPUT_PARAMS'] = job.plain_output_params_file()

    def stop_loop(self):
        with self._lock:
            self._running = False

    def wait_one_loop(self):
        # wait one full loop. The counter is incremented at the end of
        # each loop, so we must wait for the current one to finish, then
        # increment, then do another one, and when it has incremented again
        # (current + 2) we can be sure a full loop has run
        time_interval = 0.1
        with self._lock:
            running = self._running
            current_count = self._loop_count
        if not running:
            # if the loop is not running, return immediately, otherwise we will
            # wait indefinitely.
            return
        next_count = current_count
        while next_count < current_count + 2:
            with self._lock:
                next_count = self._loop_count
            if next_count < current_count + 2:
                time.sleep(time_interval)

    def set_queue_limits(self, queue_limits):
        with self._lock:
            self._queue_limits = queue_limits

    def set_running_jobs_limits(self, running_jobs_limits):
        with self._lock:
            self._running_jobs_limits = running_jobs_limits

    def add_job(self, client_job, queue, container_command=None):
        # register
        engine_job = EngineJob(client_job=client_job,
                               queue=queue,
                               path_translation=self._path_translation,
                               container_command=container_command)

        engine_job = self._database_server.add_job(self._user_id, engine_job,
                                                   login=self._user_login)

        # create standard output files
        try:
            tmp = open(engine_job.stdout_file, 'w')
            tmp.close()
        except Exception as e:
            self._database_server.delete_job(engine_job.job_id)
            raise JobError("Could not create the standard output file "
                           "%s %s: %s \n" %
                           (repr(engine_job.stdout_file), type(e), e))
        if engine_job.stderr_file:
            try:
                tmp = open(engine_job.stderr_file, 'w')
                tmp.close()
            except Exception as e:
                self._database_server.delete_job(engine_job.job_id)
                raise JobError("Could not create the standard error file "
                               "%s: %s \n" % (type(e), e))

        for transfer in six.itervalues(engine_job.transfer_mapping):
            if transfer.client_paths \
                    and not os.path.isdir(transfer.engine_path):
                try:
                    os.mkdir(transfer.engine_path)
                except Exception as e:
                    self._database_server.delete_job(engine_job.job_id)
                    raise JobError("Could not create the directory %s %s: %s \n" %
                                   (repr(transfer.engine_path), type(e), e))

        # submit
        self._pend_for_submission(engine_job)
        # add to the engine managed job list
        with self._lock:
            self._jobs[engine_job.job_id] = engine_job

        return engine_job

    def _pend_for_submission(self, engine_jobs):
        '''
        All the job submission are actually done in the loop (start_loop method).
        The jobs to submit after add_job, add_workflow and restart_workflow are
        first stored in _pending_queues waiting to be submitted.
        '''
        if not isinstance(engine_jobs, (list, tuple, set)):
            engine_jobs = [engine_jobs]
        queues = set()
        with self._lock:
            for engine_job in engine_jobs:
                queue = engine_job.queue
                queues.add(queue)
                self._pending_queues.setdefault(queue, []).append(engine_job)
                engine_job.status = constants.SUBMISSION_PENDING
            # sort queues at the end
            for queue in queues:
                self._pending_queues[queue].sort(
                    key=lambda job: job.priority,
                    reverse=True)

    def _get_pending_job_to_submit(self):
        '''
        @rtype: list of EngineJob
        @return: the list of job to be submitted
        '''
        to_run = []
        for queue_name, jobs in six.iteritems(self._pending_queues):
            if jobs and queue_name in self._running_jobs_limits:
                self.logger.debug("queue " + repr(queue_name) + " is limited: " + repr(
                    self._running_jobs_limits[queue_name]))
                nb_running_jobs = self._database_server.nb_running_jobs(
                    self._user_id,
                    queue_name)
                nb_jobs_to_run = self._running_jobs_limits[
                    queue_name] - nb_running_jobs
                # limit also queue length
                if queue_name in self._queue_limits:
                    nb_jobs_to_run = min(nb_jobs_to_run,
                                         self._queue_limits[queue_name])
                self.logger.debug("queue " + repr(queue_name)
                                  + " nb_running_jobs "
                                  + repr(nb_running_jobs) + " nb_jobs_to_run "
                                  + repr(nb_jobs_to_run))
                while nb_jobs_to_run > 0 and \
                        len(self._pending_queues[queue_name]) > 0:
                    to_run.append(self._pending_queues[queue_name].pop(0))
                    nb_jobs_to_run = nb_jobs_to_run - 1
            elif jobs and queue_name in self._queue_limits:
                nb_queued_jobs = self._database_server.nb_queued_jobs(
                    self._user_id,
                    queue_name)
                nb_jobs_to_run = self._queue_limits[
                    queue_name] - nb_queued_jobs
                self.logger.debug("queue " + repr(queue_name) + " nb_queued_jobs " + repr(
                    nb_queued_jobs) + " nb_jobs_to_run " + repr(nb_jobs_to_run))
                while nb_jobs_to_run > 0 and \
                        len(self._pending_queues[queue_name]) > 0:
                    to_run.append(self._pending_queues[queue_name].pop(0))
                    nb_jobs_to_run = nb_jobs_to_run - 1
            else:
                to_run.extend(jobs)
                self._pending_queues[queue_name] = []
        # self.logger.debug("to_run " + repr(to_run))
        return to_run

    def add_workflow(self, client_workflow, expiration_date, name, queue,
                     container_command=None):
        '''
        Parameters
        ----------
        client_workflow: soma_workflow.client.Workflow
        expiration_date: datetime.datetime
        name: str
        queue: str
        container_command: list or None
        '''
        # register
        self.logger.debug("Within add_workflow")
        #print('add_workflow: create EngineWorkflow')
        engine_workflow = EngineWorkflow(client_workflow,
                                         self._path_translation,
                                         queue,
                                         expiration_date,
                                         name,
                                         container_command=container_command)
        #print('EngineWorkflow created')

        engine_workflow = self._database_server.add_workflow(
            self._user_id, engine_workflow, login=self._user_login)
        #print('added in DB')

        for job in six.itervalues(engine_workflow.job_mapping):
            try:
                tmp = open(job.stdout_file, 'w')
                tmp.close()
                self.logger.debug(
                    "OK we have checked the directory can be written.")
                break  # OK we have checked the directory can be written.
            except Exception as e:
                self.logger.exception(e)
                self._database_server.delete_workflow(engine_workflow.wf_id)
                raise JobError("Could not create the standard output file "
                               "%s %s: %s \n" %
                               (repr(job.stdout_file), type(e), e))
            # if job.stderr_file:
                # try:
                    # tmp = open(job.stderr_file, 'w')
                    # tmp.close()
                    # break # OK we have checked the directory can be witten.
                # except Exception as e:
                    # self._database_server.delete_workflow(
                        # engine_workflow.wf_id)
                    # raise JobError("Could not create the standard error file "
                                   #"%s %s: %s \n" %
                                   #(repr(job.stderr_file), type(e), e))

        for transfer in six.itervalues(engine_workflow.transfer_mapping):
            if hasattr(transfer, 'client_paths') and transfer.client_paths \
                    and not os.path.isdir(transfer.engine_path):
                try:
                    # self.logger.debug("Try to create directory: %s \n" %
                    # transfer.engine_path)
                    os.makedirs(transfer.engine_path)
                except Exception as e:
                    self.logger.debug(
                        "WARNING: could not create directory %s: \n" % transfer.engine_path)
                    self.logger.debug(
                        "You might not have enough space available.")
                    self._database_server.delete_workflow(
                        engine_workflow.wf_id)
                    raise JobError("Could not create the directory %s %s: %s \n" %
                                   (repr(transfer.engine_path), type(e), e))

        # submit independant jobs
        #print('submit jobs...')
        (jobs_to_run,
        engine_workflow.status) = engine_workflow.find_out_independant_jobs()
        #print('independent jobs:', len(jobs_to_run))
        with self._lock:
            self._pend_for_submission(jobs_to_run)
            # add to the engine managed workflow list
            self._workflows[engine_workflow.wf_id] = engine_workflow

        #print('add_workflow done.')
        return engine_workflow.wf_id

    def _stop_job(self, job_id, job):
        if job.status == constants.DONE or job.status == constants.FAILED:
            return False
        else:
            with self._lock:
                if job.drmaa_id:
                    self.logger.debug("Kill job " + repr(job_id) + " drmaa id: " + repr(
                        job.drmaa_id) + " status " + repr(job.status))
                    try:
                        self._scheduler.kill_job(job.drmaa_id)
                    except DRMError as e:
                        # TBI how to communicate the error
                        self.logger.error("!!!ERROR!!! %s:%s" % (type(e), e))
                elif job.queue in self._pending_queues and \
                        job in self._pending_queues[job.queue]:
                    self._pending_queues[job.queue].remove(job)
                if job.status in (
                    constants.RUNNING, constants.SYSTEM_SUSPENDED,
                    constants.USER_SUSPENDED,
                        constants.USER_SYSTEM_SUSPENDED):
                    # WARNING: check these status values
                    job.exit_status = constants.USER_KILLED
                else:
                    # in other cases the job has not actually run.
                    job.exit_status = constants.EXIT_NOTRUN
                job.status = constants.FAILED
                job.exit_value = None
                job.terminating_signal = None
                job.str_rusage = None

                return True

    def _stop_wf(self, wf_id):
        wf = self._workflows[wf_id]
        # self.logger.debug("wf.registered_jobs " + repr(wf.registered_jobs))
        ended_jobs = {}
        for job_id, job in six.iteritems(wf.registered_jobs):
            if self._stop_job(job_id, job):
                ended_jobs[job_id] = job
        # self._database_server.set_workflow_status(wf_id,
                                                      # constants.WORKFLOW_DONE,
                                                      # force = True)
        wf.status = constants.WORKFLOW_DONE
        return ended_jobs

    def restart_workflow(self, wf_id, queue):
        with self._lock:
            if wf_id in self._workflows:
                workflow = self._workflows[wf_id]
                workflow.queue = queue
                (jobs_to_run, status) \
                    = workflow.restart(self._database_server, queue)
                workflow.status = status
                self._pend_for_submission(jobs_to_run)
            else:
                workflow = self._database_server.get_engine_workflow(
                    wf_id, self._user_id)
                (jobs_to_run, status) = workflow.restart(
                    self._database_server, queue)
                workflow.status = status
                self._pend_for_submission(jobs_to_run)
                # add to the engine managed workflow list
                with self._lock:
                    self._workflows[wf_id] = workflow
            return status

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
        (job, workflow_id) = self._database_server.get_engine_job(
            job_id, self._user_id)
        if workflow_id == -1:
            job.status = status
            # submit
            self._pend_for_submission(job)
            # add to the engine managed job list
            with self._lock:
                self._jobs[job.job_id] = job
        else:
            self._database_server.set_job_status(
                job_id, constants.NOT_SUBMITTED)

    def stop_jobs(self, workflow_id, job_ids):
        (status, last_status_update) \
            = self._database_server.get_workflow_status(
                workflow_id, self._user_id)

        if status != constants.WORKFLOW_DONE:
            self._database_server.set_jobs_status(
                dict([(job_id, constants.KILL_PENDING) for job_id in job_ids]))

    def restart_jobs(self, wf_id, job_ids):
        with self._lock:
            if wf_id not in self._workflows:
                return

        with self._lock:
            workflow = self._workflows[wf_id]
        extended_job_ids = workflow.job_ids_which_can_rerun(job_ids)
        self.stop_jobs(wf_id, extended_job_ids)
        # wait for the loop to process KILL_PENDING demands and actually
        # kill jobs
        self.wait_one_loop()
        with self._lock:
            jobs_to_run = workflow.restart_jobs(
                self._database_server, extended_job_ids, check_deps=False)
            print('can re-run immediately:', [j.job_id for j in jobs_to_run])
            self._pend_for_submission(jobs_to_run)

    def drms_job_id(self, wf_id, job_id):
        engine_wf = self._workflows.get(wf_id)
        if engine_wf is None:
            return None
        engine_job = engine_wf.registered_jobs.get(job_id)
        if engine_job is None:
            return None
        return engine_job.drmaa_id


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
    container_command = None

    def __init__(self,
                 database_server,
                 scheduler,
                 path_translation=None,
                 queue_limits={},
                 running_jobs_limits={},
                 container_command=None):
        '''
        @type  database_server:
               L{soma_workflow.database_server.WorkflowDatabaseServer}
        @type  engine_loop: L{WorkflowEngineLoop}
        '''
        # TODO harmoniser les loggings que l'on ait
        # une logique coherente transverse au projet
        self.logger = logging.getLogger('engine.WorkflowEngine')

        self._database_server = database_server
        self.container_command = container_command

        try:
            user_login = getpass.getuser()
        except Exception as e:
            raise EngineError(
                "Couldn't identify user %s: %s \n" % (type(e), e))

        self.logger.debug("user_login: " + user_login)
        self._user_id = self._database_server.register_user(user_login)
        self.logger.debug("user_id : " + repr(self._user_id))
        self.logger.debug("container_command : "
                          + repr(self.container_command))
        self.engine_loop = WorkflowEngineLoop(database_server,
                                              scheduler,
                                              path_translation,
                                              queue_limits,
                                              running_jobs_limits)
        self.engine_loop_thread = EngineLoopThread(self.engine_loop)
        self.engine_loop_thread.daemon = True
        self.engine_loop_thread.start()
        self.logger.debug("WorkflowEngine init done.")

    def __del__(self):
        self.engine_loop_thread.stop()

    def stop(self):
        self.engine_loop_thread.stop()

    # FILE TRANSFER ###############################################

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

    def transfer_information(self, transfer_id):
        '''
        @rtype: tuple (string, string, date, int, sequence)
        @return: (transfer_id,
                  engine_file_path,
                  client_file_path,
                  expiration_date,
                  workflow_id,
                  client_paths,
                  transfer_type,
                  status)
        '''
        return self._database_server.get_transfer_information(transfer_id,
                                                              self._user_id)

    def set_transfer_type(self, transfer_id, transfer_type):

        self._database_server.set_transfer_type(transfer_id,
                                                transfer_type,
                                                self._user_id)

    def set_transfer_status(self, transfer_id, status):
        '''
        Set a transfer status.
        '''
        self._database_server.set_transfer_status(transfer_id, status)

    def delete_transfer(self, transfer_id):
        '''
        Implementation of soma_workflow.client.WorkflowController API
        '''

        self._database_server.remove_transfer(transfer_id, self._user_id)

    def signalTransferEnded(self,
                            transfer_id,
                            workflow_id):
        '''
        Has to be called each time a file transfer ends for the
        workflows to be proceeded.
        '''
        if workflow_id != -1:
            self._database_server.add_workflow_ended_transfer(
                workflow_id, transfer_id)

    # JOB SUBMISSION ##################################################
    def submit_job(self, job, queue):
        '''
        Submits a job to the system.

        @type  job: L{soma_workflow.client.Job}
        @param job: job informations
        '''
        engine_job = self.engine_loop.add_job(
            job, queue, container_command=self.container_command)

        return engine_job.job_id

    def delete_job(self, job_id, force=True):
        '''
        Implementation of soma_workflow.client.WorkflowController API
        '''
        status = self._database_server.get_job_status(job_id,
                                                      self._user_id)[0]
        if status == constants.DONE or status == constants.FAILED:
            self._database_server.delete_job(job_id)
            return True
        else:
            self._database_server.set_job_status(
                job_id, constants.DELETE_PENDING)
            if force and not self._wait_for_job_deletion(job_id):
                self.logger.critical(
                    "!! The job may not be properly deleted !!")
                self._database_server.delete_job(job_id)
                return False
            return True

    # WORKFLOW SUBMISSION ############################################

    def submit_workflow(self, workflow, expiration_date, name, queue):
        '''
        Implementation of soma_workflow.client.WorkflowController API
        '''
        logging.info("Receiving a workflow to treat: " + repr(workflow))
        if not expiration_date:
            logging.debug("No expiration date")
            expiration_date = datetime.now() + timedelta(days=7)
        logging.debug("Going to add a workflow")
        try:
            wf_id = self.engine_loop.add_workflow(
                workflow,
                expiration_date,
                name,
                queue,
                container_command=self.container_command)
        except Exception as e:
            logging.exception(
                "ERROR: in submit_worflow, an exception occurred when calling "
                "engine_loop.add_workflow")
            raise

        logging.debug("Workflow identifier is: " + str(wf_id))

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
                self.logger.critical(
                    "The workflow may not be properly deleted.")
                self._database_server.delete_workflow(workflow_id)
                return False

            self._database_server.set_workflow_status(workflow_id,
                                                      constants.DELETE_PENDING)
            if force and not self._wait_for_wf_deletion(workflow_id):
                self.logger.critical(
                    "The workflow may not be properly deleted.")
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
                self._database_server.set_workflow_status(
                    workflow_id, constants.KILL_PENDING)
                self._wait_wf_status_update(
                    workflow_id, expected_status=constants.WORKFLOW_DONE)

        return True

    def stop_jobs(self, workflow_id, job_ids):
        self.engine_loop.stop_jobs(workflow_id, job_ids)
        self._wait_jobs_status_update(job_ids)
        return True

    def restart_jobs(self, workflow_id, job_ids):
        print('restarting jobs:', job_ids)
        self.stop_jobs(workflow_id, job_ids)  # stop before re-running
        (status, last_status_update) \
            = self._database_server.get_workflow_status(
                workflow_id, self._user_id)

        self.engine_loop.restart_jobs(workflow_id, job_ids)
        # self._wait_jobs_status_update(
            # job_ids,
            # statuses=(constants.DONE, constants.FAILED, constants.RUNNING,
                       # constants.QUEUED, constants.))

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
        expected_status = self.engine_loop.restart_workflow(workflow_id, queue)
        self._wait_wf_status_update(workflow_id,
                                    expected_status=expected_status)
        return True

    # SERVER STATE MONITORING ########################################
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

    def get_engine_job(self, job_id):
        return self._database_server.get_engine_job(job_id, self._user_id)[0]

    def get_job_command(self, job_id):
        return self._database_server.get_job_command(job_id)

    def updated_job_parameters(self, job_id):
        job = self._database_server.get_engine_job(job_id, self._user_id)[0]
        u_param_dict = self._database_server.updated_job_parameters(job.job_id)
        param_dict = job.param_dict
        if u_param_dict:
            param_dict = {}
            for param, value_tl in six.iteritems(u_param_dict):
                dval = job.param_dict.get(param)
                if isinstance(dval, list):
                    # reset lists
                    dval = []

                for value_t in value_tl:
                    # dval should be reread from parameters dictionary
                    # as it could have been updated since previous iteration
                    dval = param_dict.get(param, dval)
                    func, src_param, value = value_t
                    value = transformed_param_value(func, src_param, value,
                                                    param, dval)
                    param_dict[param] = value

        return param_dict

    def get_job_output_params(self, job_id):
        return self._database_server.get_job_output_params(job_id,
                                                           self._user_id)

    def workflow_status(self, wf_id):
        '''
        Implementation of soma_workflow.client.WorkflowController API
        '''
        self.logger.debug("! Entering workflow_status")
        (status,
         last_status_update) = self._database_server.get_workflow_status(
            wf_id, self._user_id)
        self.logger.debug("!Getting workflow status: %s" % repr(status))
        if status and \
           not status == constants.WORKFLOW_DONE and \
           _out_to_date(last_status_update):
            return constants.WARNING

        return status

    def workflow_elements_status(self, wf_id, with_drms_id=True):
        '''
        Implementation of soma_workflow.client.WorkflowController API

        Parameters
        ----------
        wf_id: int
            workflow id
        with_drms_id: bool (optional, default=False)
            if True the DRMS id (drmaa_id) is also included in the returned
            tuple for each job. This info has been added in soma_workflow 3.0
            and is thus optional to avoid breaking compatibility with earlier
            versions.
        '''
        self.logger.debug("! Entering workflow_elements_status")
        (status,
         last_status_update) = self._database_server.get_workflow_status(
            wf_id, self._user_id)

        self.logger.debug("!Getting workflow elements status: %s"
                          % repr(status))

        wf_status = self._database_server.get_detailed_workflow_status(
            wf_id, with_drms_id=with_drms_id)
        if status and \
                status != constants.WORKFLOW_DONE and \
                _out_to_date(last_status_update):
            wf_status = wf_status[:2] + (constants.WARNING, ) + wf_status[3:]

        return wf_status

    def transfer_status(self, transfer_id):
        '''
        Implementation of soma_workflow.client.WorkflowController API
        '''
        transfer_status = self._database_server.get_transfer_status(
            transfer_id,
            self._user_id)
        return transfer_status

    def job_termination_status(self, job_id):
        '''
        Implementation of soma_workflow.client.WorkflowController API
        '''

        job_exit_info = self._database_server.get_job_exit_info(
            job_id, self._user_id)

        return job_exit_info

    def stdouterr_file_path(self, job_id):
        (stdout_file,
         stderr_file) = self._database_server.get_std_out_err_file_path(job_id,
                                                                        self._user_id)
        return (stdout_file, stderr_file)

    # JOB CONTROL VIA DRMS ########################################
    def wait_job(self, job_ids, timeout=-1):
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
                delta = datetime.now() - startTime
                while status and not status == constants.DONE and not status == constants.FAILED and (waitForever or delta < timedelta(seconds=timeout)):
                    time.sleep(refreshment_interval)
                    (status, last_status_update) = self._database_server.get_job_status(
                        jid, self._user_id)
                    self.logger.debug("wait        job %s status: %s last update %s,"
                                      " now %s",
                                      jid,
                                      status,
                                      repr(last_status_update),
                                      repr(datetime.now()))
                    delta = datetime.now() - startTime
                    if last_status_update and _out_to_date(last_status_update):
                        raise EngineError("wait_job: Could not wait for job %s. "
                                          "The process updating its status failed." % (jid))

    def wait_workflow(self, workflow_id, timeout=-1):
        '''
        Implementation of soma_workflow.client.WorkflowController API
        '''
        self.logger.debug("        waiting...")

        waitForever = timeout < 0
        startTime = datetime.now()
        (status,
         last_status_update) = self._database_server.get_workflow_status(
             workflow_id, self._user_id)
        initial_status, initial_date = status, last_status_update
        count = 0
        if status:
            self.logger.debug(
                "wait        workflow %s status: %s", workflow_id,
                status)
            delta = datetime.now() - startTime
            while status and not status == constants.WORKFLOW_DONE \
                    and (waitForever or delta < timedelta(seconds=timeout)):
                time.sleep(refreshment_interval)
                (status, last_status_update) \
                    = self._database_server.get_workflow_status(
                        workflow_id, self._user_id)
                self.logger.debug(
                    "wait        workflow %s status: %s last update %s,"
                    " now %s",
                    workflow_id,
                    status,
                    repr(last_status_update),
                    repr(datetime.now()))
                delta = datetime.now() - startTime
                if last_status_update and _out_to_date(last_status_update):
                    raise EngineError(
                        "wait_workflow: Could not wait for workflow %s. "
                        "The process updating its status failed.\n"
                        "status: %s, last update date: %s, now: %s\n"
                        "start time: %s, initial status: %s, initial time: %s"
                        "\nwait iterations: %d"
                        % (workflow_id, status, repr(last_status_update),
                           repr(datetime.now()), repr(startTime),
                           initial_status, repr(initial_date), count))
                count += 1

    def restart_job(self, job_id):
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

    def kill_job(self, job_id):
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
        return self._wait_jobs_status_update([job_ids])

    def _wait_jobs_status_update(self, job_ids,
                                 statuses=(constants.DONE, constants.FAILED)):
        self.logger.debug(">> _wait_jobs_status_update")
        try:
            status_date = self._database_server.get_jobs_status(
                job_ids, self._user_id)
            while not all([not status
                           or status in statuses
                           or _out_to_date(last_status_update)
                           for status, last_status_update in status_date]):
                time.sleep(refreshment_interval)
                status_date = self._database_server.get_jobs_status(
                    job_ids, self._user_id)
        except UnknownObjectError as e:
            pass
        self.logger.debug("<< _wait_jobs_status_update")

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
        except UnknownObjectError as e:
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

    def drms_job_id(self, wf_id, job_id):
        return self.engine_loop.drms_job_id(wf_id, job_id)


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
            queue_limits=config.get_queue_limits(),
            running_jobs_limits=config.get_running_jobs_limits(),
            container_command=config.get_container_command())

        self.config = config

        self.config.addObserver(self,
                                "update_from_config",
                                [Configuration.QUEUE_LIMITS_CHANGED])
        self.config.addObserver(self,
                                "update_from_config",
                                [Configuration.RUNNING_JOBS_LIMITS_CHANGED])
        # set temp path in EngineTemporaryPath
        EngineTemporaryPath.temporary_directory \
            = config.get_shared_temporary_directory()
        self.logger.debug('ConfiguredWorkflowEngine with container_command: %s'
                          % repr(self.container_command))

    def get_configuration(self):
        return self.config

    def update_from_config(self, observable, event, msg):
        if event == Configuration.QUEUE_LIMITS_CHANGED:
            self.engine_loop.set_queue_limits(self.config.get_queue_limits())
        elif event == Configuration.RUNNING_JOBS_LIMITS_CHANGED:
            self.engine_loop.set_running_jobs_limits(
                self.config.get_running_jobs_limits())
        self.config.save_to_file()
