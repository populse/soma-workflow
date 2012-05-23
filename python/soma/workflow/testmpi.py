from mpi4py import MPI
from soma.workflow import scheduler, constants
from soma.workflow.client import Job
from soma.workflow.engine_types import EngineJob
import time
import threading
import subprocess
import atexit


def create_process(engine_job):
    ''' @todo: dupliquer de LocalScheduler
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
                s = '%s: %s \n' % (type(e), e)
                stderr_file.write(s)
                stderr_file.close()
            else:
                stdout_file = open(stdout, "wb")
                s = '%s: %s \n' % (type(e), e)
                stdout_file.write(s)
                stdout_file.close()
            return None

    working_directory = engine_job.plain_working_directory()

    try:
        process = subprocess.Popen(command,
                                  stdin=stdin_file,
                                  stdout=stdout_file,
                                  stderr=stderr_file,
                                  cwd=working_directory)

    except Exception, e:
        if stderr:
            stderr_file = open(stderr, "wb")
            s = '%s: %s \n' % (type(e), e)
            stderr_file.write(s)
            stderr_file.close()
        else:
            stdout_file = open(stdout, "wb")
            s = '%s: %s \n' % (type(e), e)
            stdout_file.write(s)
            stdout_file.close()
        return None

    return process


def slave_loop(communicator, cpu_count=1):
    status = MPI.Status()
    while True:
        processes = {}
        communicator.send(cpu_count, dest=0,
                          tag=MPIScheduler.job_request)
        communicator.Probe(source=MPI.ANY_SOURCE,
                           tag=MPI.ANY_TAG, status=status)
        t = status.Get_tag()
        if t == MPIScheduler.job_sending:
            job_list = communicator.recv(source=0, tag=t)
            for j in job_list:
                process = create_process(j)
                processes[j.job_id] = process
        elif t == MPIScheduler.exit_signal:
            print "STOP received (slave %d)" % communicator.Get_rank()
            break
        else:
            raise Exception('Unknown tag')
        returns = {}
        for job_id, process in processes.iteritems():
            if process == None:
                returns[job_id] = None
            else:
                returns[job_id] = process.wait()
        communicator.send(returns, dest=0,
                          tag=MPIScheduler.job_result)


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

    job_request = 11
    job_sending = 12
    exit_signal = 13
    job_kill = 14
    job_result = 15

    def __init__(self, communicator, interval=1):
        super(MPIScheduler, self).__init__()

        self._communicator = communicator
        self.parallel_job_submission_info = None
        # self._proc_nb = proc_nb
        self._queue = []
        self._jobs = {}
        # self._processes = {}
        self._status = {}
        self._exit_info = {}
        self._lock = threading.RLock()
        self.stop_thread_loop = False
        self._interval = interval

        def loop(self):
            while not self.stop_thread_loop:
                with self._lock:
                    self._iterate()
                # time.sleep(self._interval)

        self._loop = threading.Thread(name="scheduler_loop",
                                      target=loop,
                                      args=[self])
        self._loop.setDaemon(True)
        self._loop.start()

        atexit.register(MPIScheduler.end_scheduler_thread, self)

    def end_scheduler_thread(self):
        with self._lock:
            self.stop_thread_loop = True
            self._loop.join()
            print "Soma scheduler thread ended nicely."

    def _iterate(self):
        if not self._queue:
            return
        MPIStatus = MPI.Status()
        if self._communicator.Iprobe(source=MPI.ANY_SOURCE,
                                     tag=MPI.ANY_TAG,
                                     status=MPIStatus):
            t = MPIStatus.Get_tag()
            if t == MPIScheduler.job_request:
                s = MPIStatus.Get_source()
                self._communicator.recv(source=s, tag=MPIScheduler.job_request)
                job_id = self._queue.pop(0)
                job_list = [self._jobs[job_id]]
                self._communicator.send(job_list, dest=s,
                                        tag=MPIScheduler.job_sending)
                for j in job_list:
                    self._status[j.job_id] = constants.RUNNING
            elif t == MPIScheduler.job_result:
                s = MPIStatus.Get_source()
                results = self._communicator.recv(source=s,
                                                  tag=MPIScheduler.job_result)
                for job_id, ret_value in results.iteritems():
                    if ret_value != None:
                        self._exit_info[job_id] = (
                                               constants.FINISHED_REGULARLY,
                                               ret_value, None, None)
                        self._status[job_id] = constants.DONE
                    else:
                        self._exit_info[job_id] = (constants.EXIT_ABORTED,
                                               None, None, None)
                        self._status[job_id] = constants.FAILED
        else:
            time.sleep(self._interval)

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
        if not job.job_id or job.job_id == -1:
            raise Exception("Invalid job: no id")
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
        return exit_info

    def kill_job(self, scheduler_job_id):
        '''
        * scheduler_job_id *string*
        Job id for the scheduling system (DRMAA for example)
        '''
        # TODO
        pass

if __name__ == '__main__':

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.size
# master code
if rank == 0:
    # on initie les differents process Engine, etc...
    sch = MPIScheduler(comm, interval=1)
    # a partir d'ici on gere le schedeling (ie. soumission, stop, status)
    import numpy as np
    max_elt = 10
    r_param = abs(np.random.randn(max_elt))
    tasks = []
    for r in r_param:
        mon_job = EngineJob(Job(command=['echo', '%f' % r], name="job"),
                            queue='toto')
        mon_job.job_id = '%s ' % mon_job.command
        tasks.append(mon_job)
        sch.job_submission(mon_job)
    # status = MPI.Status()

    # for task in tasks:
    #     # print 'ask me.'
    #     send_task = False
    #     while send_task == False:
    #         comm.Probe(source=MPI.ANY_SOURCE, tag=11, status=status)
    #         s = status.Get_source()
    #         data = comm.recv(source=s, tag=11)
    #         # print "receive %s from %d " % (data, s)
    #         if data == 'JOB_PLEASE':
    #             comm.send(task, dest=s, tag=12)
    #         else:
    #             print 'bad message from %d' % s
    #         send_task = True
    time.sleep(20)
    for slave in range(1, comm.size):
        comm.send('STOP', dest=slave, tag=13)
    print "### master ends ###"
# slave code
else:
    slave_loop(comm)
