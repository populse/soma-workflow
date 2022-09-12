# -*- coding: utf-8 -*-
'''
author: Benoit Da Mota
author: Soizic Laguitton

organization: I2BM, Neurospin, Gif-sur-Yvette, France
organization: CATI, France
organization: IFR 49
organization: PARIETAL, INRIA, Saclay, France

license: CeCILL version 2, http://www.cecill.info/licences/Licence_CeCILL_V2-en.html
'''

from __future__ import with_statement, print_function
from __future__ import absolute_import

from six.moves import range
from . import subprocess
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
import datetime
from mpi4py import MPI
from soma_workflow import scheduler, constants
from soma_workflow.schedulers.mpi_scheduler import MPIScheduler


def slave_loop(communicator,
               logger=None,
               epd_to_deploy=None,
               untar_directory='/tmp',
               job_timeout=None):
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

    interval = 0.01
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

            # logger.debug("Slave " + repr(rank) + " job request")
            communicator.Probe(source=0,
                               tag=MPI.ANY_TAG, status=status)
            # logger.debug("Slave " + repr(rank) + " job request answered")
            t = status.Get_tag()
        elif communicator.Iprobe(source=0,
                                 tag=MPI.ANY_TAG, status=status):
            t = status.Get_tag()
        if t != None:

            if t == MPIScheduler.JOB_SENDING:
                # logger.debug("Slave " + repr(rank) + " receiving job")
                job_list = communicator.recv(source=0, tag=t)
                # logger.debug("Slave " + repr(rank) + " job list received")
                for j in job_list:
                    # process = scheduler.LocalScheduler.create_process(j)
                    separator = " "
                    if not j.command:  # barrier job
                        command = None
                    else:
                        # command = ""
                        # for command_el in j.plain_command():
                            # command = command + "\'" + command_el + "\' "
                        command = (j.plain_command(),
                                   j.plain_stdout(),
                                   j.plain_stderr())
                    # command = separator.join(j.plain_command())
                    # logger.debug("[host: " + socket.gethostname() + "] "
                             #+ "Slave " + repr(rank) + " RUNS JOB"
                             #+ repr(j.job_id) + " " + str(command))
                    commands[j.job_id] = (command, j.env)
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
        for job_id, command_def in six.iteritems(commands):
            command, env = command_def
            if command == None:
                # ended_jobs_info[job_id] = (constants.FAILED,
                                           #(constants.EXIT_ABORTED, None,
                                            # None, None))
                # normally a barrier job
                ended_jobs_info[job_id] = (constants.DONE,
                                           (constants.FINISHED_REGULARLY, 0,
                                            None, None))
            else:

                # if j.plain_stderr():
                    # command = command + " >> " + \
                        # j.plain_stdout() + " 2>> " + j.plain_stderr()
                # else:
                    # command = command + " >> " + j.plain_stdout()

                # ret_value = os.system(command)
                plain_command, plain_stdout, plain_stderr = command
                # ensure each command argument is a string
                plain_command = [str(c) for c in plain_command]
                cmd_stdout, cmd_stderr = (None, None)

                if plain_stdout:
                    os.makedirs(os.path.dirname(plain_stdout), exist_ok=True)
                    cmd_stdout = open(plain_stdout, 'w+')

                if plain_stderr:
                    os.makedirs(os.path.dirname(plain_stderr), exist_ok=True)
                    cmd_stderr = open(plain_stderr, 'w+')

                logger.debug("[host: " + socket.gethostname() + "] "
                             + "Slave %s JOB%s STARTING SUBPROCESS COMMAND %s, "
                             "stdout: %s, stderr: %s"
                             % (repr(rank), repr(job_id), plain_command,
                                plain_stdout, plain_stderr))

                if env is not None:
                    env2 = dict(os.environ)
                    env2.update(env)
                    env = env2

                try:
                    ret_value = subprocess.call(plain_command,
                                                stdout=cmd_stdout,
                                                stderr=cmd_stderr,
                                                timeout=job_timeout,
                                                env=env)
                    # ret_value = subprocess.call(plain_command)
                    logger.debug("[host: " + socket.gethostname() + "] "
                                 + "Slave %s JOB%s ENDED REGULARLY "
                                 "(ret value %d), stdout: %s, stderr: %s"
                                 % (repr(rank), repr(job_id), ret_value,
                                    plain_stdout, plain_stderr))

                except Exception as e:
                    ret_value = None
                    # import traceback
                    # exc_type, exc_value, exc_traceback = sys.exc_info()
                    # if hasattr(e, 'child_traceback'):
                        # logger.debug(
                        # traceback.print_tb(e.child_traceback)
                    logger.debug("[host: " + socket.gethostname() + "] "
                                 + "Slave %s JOB%s RAISED ERROR, "
                                 "stdout: %s, stderr: %s"
                                 % (repr(rank), repr(job_id),
                                    plain_stdout, plain_stderr),
                                 exc_info=True)
                    ended_jobs_info[job_id] = (constants.FAILED,
                                               (constants.EXIT_ABORTED,
                                                None, None, None))
                    # traceback.format_exception(exc_type, exc_value,
                    # exc_traceback)

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
            time.sleep(interval)

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
    parser.add_option('--log_level', type='int', default=None,
                      help='override log level. The default is to use the '
                      'config file option ENGINE_LOG_LEVEL. Values are those '
                      'from the logging module: 50=CRITICAL, 40=ERROR, '
                      '30=WARNING, 20=INFO, 10=DEBUG, 0=NOTSET.')
    group_alpha = optparse.OptionGroup(parser, "Alpha options")
    parser.add_option_group(group_alpha)
    group_alpha.add_option('--deploy_epd', dest='epd_to_deploy', default=None,
                           help="EPD tarball which will be inflated on each node using tar.")
    group_alpha.add_option(
        '--untar_dir', dest='untar_directory', default='/tmp',
        help="untar directory")

    options, args = parser.parse_args(sys.argv)

    import soma_workflow.configuration
    from soma_workflow.configuration import (OCFG_MPI_LOG_FORMAT,
                                             OCFG_MPI_LOG_DIR,
                                             OCFG_MPI_JOB_TIMEOUT)

#if not len(args) == 2:
    ## TO DO: stopping slave procedure in a function
    #logger.critical("Mandatory argument: resource id.")
    #for slave in range(1, comm.size):
        #logger.error("[host: " + socket.gethostname() + "] "
                      #+ "STOP !!! slave " + repr(slave))
        #comm.send('STOP', dest=slave, tag=MPIScheduler.EXIT_SIGNAL)
    #logger.error("[host: " + socket.gethostname() + "] "
                  #+ "######### MASTER ENDS #############")
    #raise Exception("Mandatory argument: resource id. \n")

    if len(args) > 1:
        resource_id = args[1]  # sys.argv[1]
    else:
        resource_id = None  # local

    config = soma_workflow.configuration.Configuration.load_from_file(
        resource_id)

    log_level = options.log_level
    if log_level is None:
        _, _, log_level = config.get_engine_log_info()

    if config._config_parser is None:
        raise ValueError('resource %s is not configured.' % resource_id)

    if config._config_parser.has_option(resource_id, OCFG_MPI_LOG_DIR):
        mpi_log_dir = config._config_parser.get(resource_id,
                                                OCFG_MPI_LOG_DIR)
        mpi_log_dir = os.path.expandvars(mpi_log_dir)
    else:
        mpi_log_dir = os.path.join(config.get_soma_workflow_dir(),
                                    'logs', 'mpi_logs')
    if config._config_parser.has_option(resource_id,
                                        OCFG_MPI_LOG_FORMAT):
        log_format = config._config_parser.get(
            resource_id, OCFG_MPI_LOG_FORMAT, raw=1)
    else:
        log_format = "%(asctime)s => %(module)s line %(lineno)s: " \
                      "%(message)s          %(threadName)s)"

    if config._config_parser.has_option(resource_id,
                                        OCFG_MPI_JOB_TIMEOUT):
        job_timeout = config._config_parser.getint(
            resource_id, OCFG_MPI_JOB_TIMEOUT)
    else:
        job_timeout = None

    os.makedirs(mpi_log_dir, exist_ok=True)

    if rank == 0:

        log_file_handler = logging.FileHandler(
            os.path.join(mpi_log_dir,
                         "log_mpi_master_%s" % socket.gethostname()))
        log_file_handler.setLevel(log_level)

        log_formatter = logging.Formatter(log_format)
        log_file_handler.setFormatter(log_formatter)

        logger = logging.getLogger('testMPI')
        logger.setLevel(log_level)
        logger.addHandler(log_file_handler)


        os.makedirs(mpi_log_dir, exist_ok=True)


        from soma_workflow.engine import WorkflowEngine, ConfiguredWorkflowEngine
        from soma_workflow.database_server import WorkflowDatabaseServer
        from soma_workflow.client import Helper

        try:
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
            logger.info("comm.size (workers + scheduler): " + repr(comm.size))

            if comm.size < 2:
                msg = 'the MPI mode needs at least 2 MPI processes (1 ' \
                    'master + 1+ worker(s)) to be able to work. We do not ' \
                    'have enouth instances now.'
                logger.critical(msg)
                raise RuntimeError(msg)

            database_server = WorkflowDatabaseServer(
                config.get_database_file(),
                config.get_transfered_file_dir(),
                remove_orphan_files=config.get_remove_orphan_files())

            logger.info("workflow_file " + repr(options.workflow_file))
            logger.info("wf_id_to_restart " + repr(options.wf_id_to_restart))
            logger.info(
                "nb_attempt_per_job " + repr(options.nb_attempt_per_job))
            logger.info("epd_to_deploy " + repr(options.epd_to_deploy))
            logger.info("untar_directory " + repr(options.untar_directory))
            sch = MPIScheduler(
                comm, interval=0.01, nb_attempt_per_job=options.nb_attempt_per_job)
            logger.info("scheduler started")
            config.disable_queue_limits()
            logger.info("queue limits disabled")
            workflow_engine = ConfiguredWorkflowEngine(database_server,
                                                       sch,
                                                       config)
            logger.info("engine initialized")
            if options.workflow_file:
                workflow_file = options.workflow_file
                logger.info(" ")
                logger.info("******* submission of workflow **********")
                logger.info("workflow file: " + repr(workflow_file))

                if not os.path.exists(workflow_file):
                    logger.info("workflow file: " + repr(workflow_file) +
                                "does not exist and can not be submitted")

                t0 = datetime.datetime.now()
                print('start time:', t0)
                workflow = Helper.unserialize(workflow_file)
                t1 = datetime.datetime.now()
                print('load time:', t1 - t0)
                workflow_engine.submit_workflow(workflow,
                                                expiration_date=None,
                                                name=None,
                                                queue=None)
                t2 = datetime.datetime.now()
                print('submission time:', t2 - t1)
            if options.wf_id_to_restart != None:
                workflow_id = options.wf_id_to_restart
                logger.info(" ")
                logger.info("******* restart workflow **********")
                logger.info("workflow if: " + repr(workflow_id))
                t0 = datetime.datetime.now()
                print('start time:', t0)
                workflow_engine.stop_workflow(workflow_id)
                t1 = datetime.datetime.now()
                print('stop old workflow time:', t1 - t0)
                workflow_engine.restart_workflow(workflow_id, queue=None)
                t2 = datetime.datetime.now()
                print('restart time:', t2 - t1)

            while not workflow_engine.engine_loop.are_jobs_and_workflow_done():
                time.sleep(2)
            t3 = datetime.datetime.now()
            print('run time:', t3 - t2)  # May cause error if t2 is not defined
            logger.debug("******** workflow ends **********")
            for slave in range(1, comm.size):
                logger.debug("[host: " + socket.gethostname() + "] "
                             + "STOP !!! slave " + repr(slave))
                comm.send('STOP', dest=slave, tag=MPIScheduler.EXIT_SIGNAL)
            logger.debug("******** stop signal sent **********")
            while not sch.stop_thread_loop and comm.size > 1:
                time.sleep(1)
            logger.debug("######### master ends #############")

        except Exception as e:
            logger.error('exception occured: %s' % repr(e))
            for slave in range(1, comm.size):
                logger.debug("[host: " + socket.gethostname() + "] "
                             + "STOP !!! slave " + repr(slave))
                comm.send('STOP', dest=slave, tag=MPIScheduler.EXIT_SIGNAL)

            logger.debug("######### master ends with errors #############")
            raise e

    # slave code
    else:

        log_file_handler = logging.FileHandler(
            os.path.join(mpi_log_dir,
                         "logtestmpi_slave%d_%s" % (rank,
                                                    socket.gethostname())))
        log_file_handler.setLevel(log_level)
        log_formatter = logging.Formatter(log_format)
        log_file_handler.setFormatter(log_formatter)

        logger = logging.getLogger("testMPI.slave")
        logger.setLevel(log_level)
        logger.addHandler(log_file_handler)
        logger.info("=====> [host: " + socket.gethostname() + "] "
                    + "slave starts " + repr(rank))

        # logger.debug("=====> [host: " + socket.gethostname() + "] environment:")
        # for k,v in six.iteritems(os.environ):
            # logger.debug("%s=%s" % (k, v))

        slave_loop(comm,
                   logger=logger,
                   epd_to_deploy=options.epd_to_deploy,
                   untar_directory=options.untar_directory,
                   job_timeout=job_timeout)
