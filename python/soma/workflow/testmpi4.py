
import time
import threading
import subprocess
import atexit
import logging
import sys
import os
#import socket

from mpi4py import MPI

from soma.workflow import scheduler, constants
from soma.workflow.client import Job
from soma.workflow.engine_types import EngineJob


def slave_loop(communicator, cpu_count=1, logger=None):
    status = MPI.Status()
    rank = communicator.Get_rank()
    if not logger:
        logger = logging.getLogger("testMPI.slave")
    commands = {}
    max_nb_jobs = 1
    while True:
        ended_jobs_info = {} # job_id -> (job_status, job_exit_status)
        t = None
        if len(commands) < max_nb_jobs:    
            communicator.send(cpu_count, dest=0,
                              tag=MPIScheduler.JOB_REQUEST)

            logger.debug("Slave " + repr(rank) + " job request")
            communicator.Probe(source=MPI.ANY_SOURCE,
                               tag=MPI.ANY_TAG, status=status)
            t = status.Get_tag()
        elif communicator.Iprobe(source=MPI.ANY_SOURCE,
                               tag=MPI.ANY_TAG, status=status):
            t = status.Get_tag()
        if t != None:
            if t == MPIScheduler.JOB_SENDING:
                job_list = communicator.recv(source=0, tag=t)
                for j in job_list:
                    logger.debug("Slave " + repr(rank) + " RUNS JOB" + repr(j.job_id))
                    #process = scheduler.LocalScheduler.create_process(j)
                    separator = " "
                    command = separator.join(j.plain_command())
                    commands[j.job_id] = command
            elif t == MPIScheduler.NO_JOB:
                communicator.recv(source=0, tag=t)
                logger.debug("Slave " + repr(rank) + " "
                             "received no job " + repr(commands))
                #time.sleep(1)
            elif t == MPIScheduler.EXIT_SIGNAL:
                communicator.send('STOP', dest=0, tag=MPIScheduler.EXIT_SIGNAL)
                logger.debug("Slave " + repr(rank) + " STOP !!!!! received")
                break
            elif t == MPIScheduler.JOB_KILL:
                job_ids = communicator.recv(source=0, tag=t)
                for job_id in job_ids:
                    if job_id in commands.keys():
                        # TO DO: relevant exception type and message
                        raise Exception("The job " + repr(job_id) + " can not be killed")

            else:
                raise Exception('Unknown tag')
        for job_id, command in commands.iteritems():
            if command == None:
                ended_jobs_info[job_id] = (constants.FAILED,
                                           (constants.EXIT_ABORTED, None, 
                                            None, None))
            else:
                #ret_value = process.wait() # TO DO: wait works but not poll why ?
                #stdout_file = open(j.plain_stdout(), 'w')
                #stdout_file.write("hostname " + repr(socket.gethostname()) + "\n")
                #stdout_file.write("slave rank " + repr(rank) + "\n")
                #stdout_file.write("os.__file__ " + repr(os.__file__) + "\n")
                #stdout_file.close()
                if j.plain_stderr():
                    command = command + " >> " + repr(j.plain_stdout()) + " 2>> " + repr(j.plain_stderr())
                else:
                    command = command +  " >> " + repr(j.plain_stdout()) 
                #command = "which python >> " + repr(j.plain_stdout()) + " ; " + command
                logger.debug("command " + repr(command))
                ret_value = os.system(command)
                logger.debug("Slave " + repr(rank) + " " + repr(ret_value) + " " + repr(command))
                if ret_value != None:
                    ended_jobs_info[job_id] = (constants.DONE,
                                               (constants.FINISHED_REGULARLY, 
                                                ret_value, None, None))
     
        if ended_jobs_info:
            for job_id in ended_jobs_info.iterkeys():
                del commands[job_id]
            logger.debug("Slave " + repr(rank) + " send JOB_RESULT")
            communicator.send(ended_jobs_info, dest=0,
                              tag=MPIScheduler.JOB_RESULT)
        else:
            pass
            # TO DO send Slave is alive
        time.sleep(1)

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
        with self._lock:
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
                ended_jobs_info = self._communicator.recv(source=s,
                                                  tag=MPIScheduler.JOB_RESULT)
                for job_id, end_info in ended_jobs_info.iteritems():
                    job_status, exit_info = end_info
                    self._exit_info[job_id] = exit_info
                    self._status[job_id] = job_status
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
        self._logger.debug(">> job_submission wait lock")
        with self._lock:
            self._logger.debug(">> job_submission wait lock END")
            self._queue.append(job.job_id)
            self._jobs[job.job_id] = job
            self._status[job.job_id] = constants.QUEUED_ACTIVE
            self._queue.sort(key=lambda job_id: self._jobs[job_id].priority,
                             reverse=True)
            self._logger.debug("A Job was submitted.")
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
        
    # TODO change the path
    #log_file_handler = logging.FileHandler(os.path.expandvars("$WORKDIR/soma_workflow/server/logs/logtestmpi"))
    log_file_handler = logging.FileHandler(os.path.expandvars("$HOME/logtestmpi"))
    log_file_handler.setLevel(logging.DEBUG)
    log_formatter = logging.Formatter("%(asctime)s => %(module)s line %(lineno)s: %(message)s          %(threadName)s)")
    log_file_handler.setFormatter(log_formatter)

    print rank
    # master code
    if rank == 0:
        from soma.workflow.engine import WorkflowEngine, ConfiguredWorkflowEngine
        from soma.workflow.database_server import WorkflowDatabaseServer
        from soma.workflow.client import Helper
        import soma.workflow.configuration
               
        logger = logging.getLogger('testMPI')
        logger.setLevel(logging.DEBUG)
        logger.addHandler(log_file_handler)
 
        if not len(sys.argv) == 3:
          #TO DO: stopping slave procedure in a function
          logger.critical("Mandatory arguments: "
                          "1. resource id "
                          "2. workflow file to run or workflow id to restart.")
          for slave in range(1, comm.size):
              logger.debug("STOP !!!  slave " + repr(slave))
              comm.send('STOP', dest=slave, tag=MPIScheduler.EXIT_SIGNAL)
          logger.debug("######### master ends #############")
          raise Exception("Mandatory arguments: \n"
                          " 1. resource id. \n"
                          " 2. workflow file to run or workflow id to restart. \n")

        resource_id = sys.argv[1]
        wf_arg = sys.argv[2]
        
        try:
            config = soma.workflow.configuration.Configuration.load_from_file(resource_id)
            
            logger.info(" ")
            logger.info(" ")
            logger.info(" ")
            logger.info(" ")
            logger.info(" ")
            logger.info(" ")
            logger.info("################ MASTER STARTS ####################")
 
            database_server = WorkflowDatabaseServer(config.get_database_file(),
                                                     config.get_transfered_file_dir())
    
            sch = MPIScheduler(comm, interval=1)

            config.disable_queue_limits()    

            workflow_engine = ConfiguredWorkflowEngine(database_server,
                                                       sch,
                                                       config)
            if os.path.exists(wf_arg):
                workflow_file = wf_arg  
                logger.info(" ")
                logger.info("******* submission of worklfow **********")
                logger.info("workflow file: " + repr(workflow_file))

                workflow = Helper.unserialize(workflow_file)
                workflow_engine.submit_workflow(workflow,
                                                expiration_date=None,
                                                name=None,
                                                queue=None)
            else:
                workflow_id = int(wf_arg)
                logger.info(" ")
                logger.info("******* restart worklfow **********")
                logger.info("workflow if: " + repr(workflow_id))
                workflow_engine.restart_workflow(workflow_id, queue=None)
     
            while not workflow_engine.engine_loop.are_jobs_and_workflow_done():
                time.sleep(2)
            for slave in range(1, comm.size):
                logger.debug("STOP !!!  slave " + repr(slave))
                comm.send('STOP', dest=slave, tag=MPIScheduler.EXIT_SIGNAL)
            while not sch.stop_thread_loop:
                time.sleep(1)
            logger.debug("######### master ends #############")
        except Exception, e:
             for slave in range(1, comm.size):
                logger.debug("STOP !!!  slave " + repr(slave))
                comm.send('STOP', dest=slave, tag=MPIScheduler.EXIT_SIGNAL)
             raise e
    # slave code
    else:
        logger = logging.getLogger("testMPI.slave")
        logger.setLevel(logging.DEBUG)
        logger.addHandler(log_file_handler)
        slave_loop(comm, logger=logger)
