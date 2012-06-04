
import time
import threading
import subprocess
import atexit
import logging
import sys

from mpi4py import MPI

from soma.workflow import scheduler, constants
from soma.workflow.client import Job
from soma.workflow.engine_types import EngineJob

def slave_loop(communicator, cpu_count=1):
    status = MPI.Status()
    rank = communicator.Get_rank()
    logger = logging.getLogger("TestMPI")
    while True:
        processes = {}
        communicator.send(cpu_count, dest=0,
                          tag=MPIScheduler.JOB_REQUEST)
        communicator.Probe(source=MPI.ANY_SOURCE,
                           tag=MPI.ANY_TAG, status=status)
        logger.debug("Slave " + repr(rank) + " job request")
        t = status.Get_tag()
        if t == MPIScheduler.JOB_SENDING:
            job_list = communicator.recv(source=0, tag=t)
            for j in job_list:
                logger.debug("Slave " + repr(rank) + " RUNS JOB")
                process = scheduler.LocalScheduler.create_process(j)
                processes[j.job_id] = process
        elif t == MPIScheduler.NO_JOB:
            communicator.recv(source=0, tag=t)
            logger.debug("Slave " + repr(rank) + " received no job " + repr(processes))
            time.sleep(1)
        elif t == MPIScheduler.EXIT_SIGNAL:
            communicator.send('STOP', dest=0, tag=MPIScheduler.EXIT_SIGNAL)
            logger.debug("Slave " + repr(rank) + "STOP !!!!! received")
            break
        else:
            raise Exception('Unknown tag')
        returns = {}
        for job_id, process in processes.iteritems():
            if process == None:
                returns[job_id] = None
            else:
                returns[job_id] = process.wait()
        if returns:
            logger.debug("Salve " + repr(rank) + " send JOB_RESULT")
            communicator.send(returns, dest=0,
                              tag=MPIScheduler.JOB_RESULT)


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

    JOB_REQUEST = 11
    JOB_SENDING = 12
    EXIT_SIGNAL = 13
    JOB_KILL = 14
    JOB_RESULT = 15
    NO_JOB = 16

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

        self._logger = logging.getLogger("testMPI")
        def master_loop(self):
            self._stopped_slaves = 0
            while not self.stop_thread_loop:
                with self._lock:
                    self._master_iteration()
                #time.sleep(0)#self._interval)

        self._loop = threading.Thread(name="scheduler_loop",
                                      target=master_loop,
                                      args=[self])
        self._loop.setDaemon(True)
        self._loop.start()

        #atexit.register(MPIScheduler.end_scheduler_thread, self)

    def end_scheduler_thread(self):
        with self._lock:
            self.stop_thread_loop = True
            self._loop.join()
            print "Soma scheduler thread ended nicely."

    def _master_iteration(self):
        MPIStatus = MPI.Status()
        #if not self._queue:
        #    return
        self._communicator.Probe(source=MPI.ANY_SOURCE,
                                     tag=MPI.ANY_TAG,
                                     status=MPIStatus)
        t = MPIStatus.Get_tag()
        if t == MPIScheduler.JOB_REQUEST:
            self._logger.debug("Master received the JOB_REQUEST signal")
            s = MPIStatus.Get_source()
            if not self._queue:
                self._logger.debug("Master No job for now")
                self._communicator.recv(source=s, 
                                        tag=MPIScheduler.JOB_REQUEST)
                self._communicator.send("No job for now", 
                                        dest=s,
                                        tag=MPIScheduler.NO_JOB)            
            else:
                self._logger.debug("Master send a Job !!!")
                self._communicator.recv(source=s, tag=MPIScheduler.JOB_REQUEST)
                job_id = self._queue.pop(0)
                job_list = [self._jobs[job_id]]
                self._communicator.send(job_list, dest=s,
                                      tag=MPIScheduler.JOB_SENDING)
                for j in job_list:
                    self._status[j.job_id] = constants.RUNNING
        elif t == MPIScheduler.JOB_RESULT:
            self._logger.debug("Master received the JOB_RESULT signal")
            s = MPIStatus.Get_source()
            results = self._communicator.recv(source=s,
                                              tag=MPIScheduler.JOB_RESULT)
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
        elif t == MPIScheduler.EXIT_SIGNAL:
            self._logger.debug("Master received the EXIT_SIGNAL")
            self._stopped_slaves = self._stopped_slaves + 1
            if self._stopped_slaves == self._communicator.size -1:
              self.stop_thread_loop = True
        else:
            self._logger.critical("Master unknown tag")

    def sleep(self):
        self.is_sleeping = True

    def wake(self):
        self.is_sleeping = False

    def clean(self):
        pass


    def queued_job_count(self):
        return len(self._queue)

    def job_submission(self, job):
        '''
        * job *EngineJob*
        * return: *string*
        Job id for the scheduling system (DRMAA for example)
        '''
        if not job.job_id or job.job_id == -1:
            raise Exception("Invalid job: no id")
        with self._lock:
            self._queue.append(job.job_id)
            self._jobs[job.job_id] = job
            self._status[job.job_id] = constants.QUEUED_ACTIVE
            self._queue.sort(key=lambda job_id: self._jobs[job_id].priority,
                             reverse=True)
            self._logger.debug("A Job was submitted " + repr(self._queue))
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
    

print rank
# master code
if rank == 0:
    from soma.workflow.engine import WorkflowEngine, ConfiguredWorkflowEngine
    from soma.workflow.database_server import WorkflowDatabaseServer
    from soma.workflow.client import Helper
    import soma.workflow.configuration

    if not len(sys.argv) == 3:
      raise Exception("Mandatory arguments: \n"
                      " 1. resource id. \n"
                      " 2. workflow file to run. \n")

    resource_id = sys.argv[1]
    workflow_file = sys.argv[2]

    config = soma.workflow.configuration.Configuration.load_from_file(resource_id)
    
    logging.basicConfig(filename="/tmp/logtestmpi",
                        format="%(asctime)s => %(module)s line %(lineno)s: %(message)s                 %(threadName)s)", 
                        level=logging.DEBUG)
    logger = logging.getLogger("testMPI")

    database_server = WorkflowDatabaseServer(config.get_database_file(),
                                             config.get_transfered_file_dir())

    sch = MPIScheduler(comm, interval=1)

    workflow_engine = ConfiguredWorkflowEngine(database_server,
                                               sch,
                                               config)
    
    workflow = Helper.unserialize(workflow_file)
    workflow_engine.submit_workflow(workflow,
                                    expiration_date=None,
                                    name=None,
                                    queue=None)
    
    #import numpy as np
    #max_elt = 10
    #r_param = abs(np.random.randn(max_elt))
    #tasks = []
    #for r in r_param:
    #
    #    mon_job = EngineJob(Job(command=['echo', '%f' % r], name="job"),
    #                        queue='toto')
    #    mon_job.job_id = '%s ' % mon_job.command
    #    tasks.append(mon_job)
    #    sch.job_submission(mon_job)
 
    #while sch.queued_job_count() > 0:
    #    time.sleep(2) 
   
 
    while not workflow_engine.engine_loop.are_jobs_and_workflow_done():
        time.sleep(2)
    for slave in range(1, comm.size):
        logger.debug("STOP STOP STOP STOP STOP STOP STOP STOP slave " + repr(slave))
        comm.send('STOP', dest=slave, tag=MPIScheduler.EXIT_SIGNAL)
    while not sch.stop_thread_loop:
        time.sleep(1)
    logger.debug("### master ends ###")
# slave code
else:
    slave_loop(comm)
