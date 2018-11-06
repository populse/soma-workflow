from __future__ import with_statement, print_function

'''
author: Benoit Da Mota
author: Soizic Laguitton

organization: I2BM, Neurospin, Gif-sur-Yvette, France
organization: CATI, France
organization: IFR 49
organization: PARIETAL, INRIA, Saclay, France

license: CeCILL version 2, http://www.cecill.info/licences/Licence_CeCILL_V2-en.html
'''
try:
    import subprocess32 as subprocess
except:
    import subprocess
    print('subprocess module will be used to start shell commands. Due to '
          'issues in this module this can lead to problems during execution. '
          'You should probably install subprocess32 module to avoid these '
          'problems.')
import sys
import time
import threading
import logging
import os
import socket
import shutil
import optparse
import tarfile
import six
from mpi4py import MPI
from soma_workflow import scheduler, constants
from soma_workflow.schedulers.mpi_scheduler import MPIScheduler

def slave_loop(communicator,
               logger=None,
               epd_to_deploy=None,
               untar_directory='/tmp'):
    status = MPI.Status()
    rank = communicator.Get_rank()

    if not logger:
        logger = logging.getLogger("testMPI.slave")
    commands = {}

    if epd_to_deploy != None:
        lock_file_path = os.path.join(untar_directory, "sw_deploy_lock")
        if not os.path.isfile(lock_file_path):
            try:
                lock_file = open(lock_file_path, "w")
                lock_file.write("locked \n")
                lock_file.close()
                epd_tar = tarfile.open(epd_to_deploy)
                epd_tar.extractall(path=untar_directory)
                # logger.debug('extract %s' %(epd_to_deploy))
            except IOError as e:
                logger.error("Could not deploy epd: %s" % (e))
                pass

    max_nb_jobs = 1

    ret_value = 0
    slave_on_hold = False
    while True:
        ended_jobs_info = {}  # job_id -> (job_status, job_exit_status)
        t = None
        if len(commands) < max_nb_jobs:
            communicator.send('Requesting a job', 
                              dest=0,
                              tag=MPIScheduler.JOB_REQUEST)

            #logger.debug("Slave " + repr(rank) + " job request")
            communicator.Probe(source=0,
                               tag=MPI.ANY_TAG, status=status)
            #logger.debug("Slave " + repr(rank) + " job request answered")
            t = status.Get_tag()
        elif communicator.Iprobe(source=0,
                                 tag=MPI.ANY_TAG, status=status):
            t = status.Get_tag()
        if t != None:
            
            if t == MPIScheduler.JOB_SENDING:
                #logger.debug("Slave " + repr(rank) + " receiving job")
                job_list = communicator.recv(source=0, tag=t)
                #logger.debug("Slave " + repr(rank) + " job list received")
                for j in job_list:
                    # process = scheduler.LocalScheduler.create_process(j)
                    separator = " "
                    if not j.command:  # barrier job
                        command = None
                    else:
                        #command = ""
                        #for command_el in j.plain_command():
                            #command = command + "\'" + command_el + "\' "
                        command = (j.plain_command(), 
                                   j.plain_stdout(), 
                                   j.plain_stderr())
                    # command = separator.join(j.plain_command())
                    #logger.debug("[host: " + socket.gethostname() + "] "
                             #+ "Slave " + repr(rank) + " RUNS JOB" 
                             #+ repr(j.job_id) + " " + str(command))
                    commands[j.job_id] = command
            elif t == MPIScheduler.NO_JOB:
                communicator.recv(source=0, tag=t)
                # logger.debug("Slave " + repr(rank) + " "
                #             "received no job " + repr(commands))
                # time.sleep(1)
            elif t == MPIScheduler.EXIT_SIGNAL:
                communicator.send('STOP', dest=0, tag=MPIScheduler.EXIT_SIGNAL)
                logger.debug("[host: " + socket.gethostname() + "] "
                             + "Slave " + repr(rank) + " STOP !!!!! received")
                break
            elif t == MPIScheduler.JOB_KILL:
                job_ids = communicator.recv(source=0, tag=t)
                for job_id in job_ids:
                    if job_id in commands.keys():
                        # TO DO: relevant exception type and message
                        raise Exception(
                            "The job " + repr(job_id) + " can not be killed")

            else:
                raise Exception('Unknown tag')
        for job_id, command in six.iteritems(commands):
            if command == None:
                # ended_jobs_info[job_id] = (constants.FAILED,
                                           #(constants.EXIT_ABORTED, None,
                                            # None, None))
                # normally a barrier job
                ended_jobs_info[job_id] = (constants.DONE,
                                           (constants.FINISHED_REGULARLY, 0,
                                            None, None))
            else:

                #if j.plain_stderr():
                    #command = command + " >> " + \
                        #j.plain_stdout() + " 2>> " + j.plain_stderr()
                #else:
                    #command = command + " >> " + j.plain_stdout()

                #ret_value = os.system(command)
                plain_command, plain_stdout, plain_stderr = command
                cmd_stdout, cmd_stderr = (None, None)
                            
                if plain_stdout:
                    cmd_stdout = open(plain_stdout, 'w+')
                    
                if plain_stderr:
                    cmd_stderr = open(plain_stderr, 'w+')
                
                logger.debug("[host: " + socket.gethostname() + "] "
                           + "Slave %s JOB%s STARTING SUBPROCESS COMMAND %s, " \
                             "stdout: %s, stderr: %s" \
                            % (repr(rank), repr(job_id), plain_command, 
                               plain_stdout, plain_stderr))
                            
                try:
                    ret_value = subprocess.call(plain_command, 
                                                stdout=cmd_stdout, 
                                                stderr=cmd_stderr)
                    #ret_value = subprocess.call(plain_command)
                    logger.debug("[host: " + socket.gethostname() + "] "
                               + "Slave %s JOB%s ENDED REGULARLY " \
                                 "(ret value %d), stdout: %s, stderr: %s" \
                                % (repr(rank), repr(job_id), ret_value,
                                   plain_stdout, plain_stderr))
                            
                except Exception as e:
                    ret_value = None
                    #import traceback
                    #exc_type, exc_value, exc_traceback = sys.exc_info()
                    #if hasattr(e, 'child_traceback'):
                        #logger.debug(
                        #traceback.print_tb(e.child_traceback)
                    logger.debug("[host: " + socket.gethostname() + "] "
                               + "Slave %s JOB%s RAISED ERROR, " \
                                 "stdout: %s, stderr: %s" \
                                % (repr(rank), repr(job_id),
                                   plain_stdout, plain_stderr), 
                                 exc_info=True)
                    ended_jobs_info[job_id] = (constants.FAILED,
                                               (constants.EXIT_ABORTED,
                                                None, None, None))
                    #traceback.format_exception(exc_type, exc_value, exc_traceback)
                    
                finally:
                    if cmd_stdout:
                        cmd_stdout.close()
                        
                    if cmd_stderr:
                        cmd_stderr.close()
                    
                if ret_value != None:
                    ended_jobs_info[job_id] = (constants.DONE,
                                               (constants.FINISHED_REGULARLY,
                                                ret_value, None, None))

        if ended_jobs_info:
            for job_id in six.iterkeys(ended_jobs_info):
                del commands[job_id]
            logger.debug("[host: " + socket.gethostname() + "] "
                         + "Slave " + repr(rank) + " send JOB_RESULT")
            communicator.send(ended_jobs_info, dest=0,
                              tag=MPIScheduler.JOB_RESULT)
        else:
            pass
            # TO DO send Slave is alive
        if slave_on_hold:
            logger.debug("[host: " + socket.gethostname() + "] "
                         + "Slave %d was punished !!!" % (rank))
            # time.sleep(1200) #20 mins
            break
        else:
            time.sleep(1)

    if epd_to_deploy != None:
        logger.debug("[host: " + socket.gethostname() + "] "
                     + "Slave %d cleaning ... \n" % (rank))
        if os.path.isfile(lock_file_path):
            try:
                os.remove(lock_file_path)
                archive_dir_path = os.path.join(untar_directory, 'epd')
                if os.path.isdir(archive_dir_path):
                    shutil.rmtree(archive_dir_path)
                    logger.debug("remove %s" % (archive_dir_path))
            except Exception as e:
                pass
        logger.debug("[host: " + socket.gethostname() + "] "
                     + "Slave %d: end of cleaning! \n" % (rank))
    logger.debug("[host: " + socket.gethostname() + "] "
                 + "Slave %d END!!! \n" % (rank))

if __name__ == '__main__':
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.size
    
    parser = optparse.OptionParser()
    parser.add_option('--workflow',
                      dest='workflow_file', default=None,
                      help="The workflow to run.")
    parser.add_option('--restart', '-r', type="int",
                      dest='wf_id_to_restart', default=None,
                      help="The workflow id to restart")
    parser.add_option('--nb_attempt_per_job', type="int",
                      dest='nb_attempt_per_job', default=1,
                      help="A job can be restarted several time if it fails. This option "
                           "specify the number of attempt per job. "
                           "By default, the jobs are not restarted.")
    group_alpha = optparse.OptionGroup(parser, "Alpha options")
    parser.add_option_group(group_alpha)
    group_alpha.add_option('--deploy_epd', dest='epd_to_deploy', default=None,
                           help="EPD tarball which will be inflated on each node using tar.")
    group_alpha.add_option(
        '--untar_dir', dest='untar_directory', default='/tmp',
        help="untar directory")

    options, args = parser.parse_args(sys.argv)
    
    if rank == 0:

        from soma_workflow.engine import WorkflowEngine, ConfiguredWorkflowEngine
        from soma_workflow.database_server import WorkflowDatabaseServer
        from soma_workflow.client import Helper
        import soma_workflow.configuration
    
        log_file_handler = logging.FileHandler(
            os.path.expandvars("$HOME/logtestmpi_master_%s" % socket.gethostname()))
        log_file_handler.setLevel(logging.DEBUG)
        log_formatter = logging.Formatter(
            "%(asctime)s => %(module)s line %(lineno)s: %(message)s          %(threadName)s)")
        log_file_handler.setFormatter(log_formatter)
    
        logger = logging.getLogger('testMPI')
        logger.setLevel(logging.DEBUG)
        logger.addHandler(log_file_handler)
        
        resource_id = args[1]  # sys.argv[1]

        if not len(args) == 2:
            # TO DO: stopping slave procedure in a function
            logger.critical("Mandatory argument: resource id.")
            for slave in range(1, comm.size):
                logger.debug("[host: " + socket.gethostname() + "] "
                             + "STOP !!! slave " + repr(slave))
                comm.send('STOP', dest=slave, tag=MPIScheduler.EXIT_SIGNAL)
            logger.debug("[host: " + socket.gethostname() + "] "
                         + "######### MASTER ENDS #############")
            raise Exception("Mandatory argument: resource id. \n")

        resource_id = args[1]

        try:
            config = soma_workflow.configuration.Configuration.load_from_file(
                resource_id)

            if config.get_scheduler_type() != soma_workflow.configuration.MPI_SCHEDULER:
                raise Exception("The resource id %s is not configured to use a MPI scheduler. "
                                "Configure a resource with SCHEDULER_TYPE = mpi." % (resource_id))

            logger.info(" ")
            logger.info(" ")
            logger.info(" ")
            logger.info(" ")
            logger.info(" ")
            logger.info(" ")
            logger.info("[host: " + socket.gethostname() + "] "
                        + "################ MASTER STARTS ####################")

            database_server = WorkflowDatabaseServer(
                config.get_database_file(),
                config.get_transfered_file_dir())

            logger.info("workflow_file " + repr(options.workflow_file))
            logger.info("wf_id_to_restart " + repr(options.wf_id_to_restart))
            logger.info(
                "nb_attempt_per_job " + repr(options.nb_attempt_per_job))
            logger.info("epd_to_deploy " + repr(options.epd_to_deploy))
            logger.info("untar_directory " + repr(options.untar_directory))
            sch = MPIScheduler(
                comm, interval=1, nb_attempt_per_job=options.nb_attempt_per_job)
            logger.info("scheduler started")
            config.disable_queue_limits()
            logger.info("queue limits disabled")
            workflow_engine = ConfiguredWorkflowEngine(database_server,
                                                       sch,
                                                       config)
            logger.info("engine initialized")
            if options.workflow_file and os.path.exists(options.workflow_file):
                workflow_file = options.workflow_file
                logger.info(" ")
                logger.info("******* submission of workflow **********")
                logger.info("workflow file: " + repr(workflow_file))
                
                if not os.path.exists(workflow_file):
                    logger.info("workflow file: " + repr(workflow_file) + 
                                "does not exist and can not be submitted")

                workflow = Helper.unserialize(workflow_file)
                workflow_engine.submit_workflow(workflow,
                                                expiration_date=None,
                                                name=None,
                                                queue=None)
            if options.wf_id_to_restart != None:
                workflow_id = options.wf_id_to_restart
                logger.info(" ")
                logger.info("******* restart workflow **********")
                logger.info("workflow if: " + repr(workflow_id))
                workflow_engine.stop_workflow(workflow_id)
                workflow_engine.restart_workflow(workflow_id, queue=None)

            while not workflow_engine.engine_loop.are_jobs_and_workflow_done():
                time.sleep(2)
            for slave in range(1, comm.size):
                logger.debug("[host: " + socket.gethostname() + "] "
                             + "STOP !!! slave " + repr(slave))
                comm.send('STOP', dest=slave, tag=MPIScheduler.EXIT_SIGNAL)
            while not sch.stop_thread_loop:
                time.sleep(1)
            logger.debug("######### master ends #############")
        except Exception as e:
            for slave in range(1, comm.size):
                logger.debug("STOP !!! " + socket.gethostname()
                             + " slave " + repr(slave))
                comm.send('STOP', dest=slave, tag=MPIScheduler.EXIT_SIGNAL)
            raise e
    # slave code
    else:
    
        log_file_handler = logging.FileHandler(
            os.path.expandvars("$HOME/logtestmpi_slave%d_%s" % (rank, socket.gethostname())))
        log_file_handler.setLevel(logging.DEBUG)
        log_formatter = logging.Formatter(
            "%(asctime)s => %(module)s line %(lineno)s: %(message)s          %(threadName)s)")
        log_file_handler.setFormatter(log_formatter)
        
        logger = logging.getLogger("testMPI.slave")
        logger.setLevel(logging.DEBUG)
        logger.addHandler(log_file_handler)
        logger.info("=====> [host: " + socket.gethostname() + "] "
                    + "slave starts " + repr(rank))
        
        slave_loop(comm,
                   logger=logger,
                   epd_to_deploy=options.epd_to_deploy,
                   untar_directory=options.untar_directory)
