# -*- coding: utf-8 -*-

'''
author: Soizic Laguitton

organization: I2BM, Neurospin, Gif-sur-Yvette, France
organization: CATI, France

license: CeCILL version 2 <http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>
'''

from __future__ import absolute_import
from __future__ import print_function
if __name__ == "__main__":

    import sys
    import os
    import logging
    import threading

    import soma_workflow.zro as zro
    # import soma_workflow.sro as zro
    import zmq
    import soma_workflow.engine
    import soma_workflow.scheduler
    import soma_workflow.connection
    import soma_workflow.configuration
    from soma_workflow.errors import NoDrmaaLibError, EngineError
    from soma_workflow.database_server import WorkflowDatabaseServer
    from soma_workflow import scheduler
    import time
    import signal
    from soma_workflow import subprocess
    import json

    # DEBUGGING

    def debug_stack(sig, frame):
        """Interrupt running process, and provide a traceback in a file /tmp/
        traceback_<pid>."""
        import traceback
        import threading

        dump_filename = '/tmp/traceback_%d' % os.getpid()
        with open(dump_filename, 'w') as f:
            id2name = dict([(th.ident, th.name)
                            for th in threading.enumerate()])
            code = []
            for threadId, stack in sys._current_frames().items():
                code.append("\n=== Thread: %s(%d) ==="
                            % (id2name.get(threadId,""), threadId))
                for filename, lineno, name, line \
                        in traceback.extract_stack(stack):
                    code.append('File: "%s", line %d, in %s'
                                % (filename, lineno, name))
                    if line:
                        code.append("  %s" % (line.strip()))
            f.write("\n".join(code))

    if not sys.platform.startswith('win'):
        signal.signal(signal.SIGUSR1, debug_stack)


    class VersionError(Exception):

        def __init__(self, msg, py_ver):
            super(VersionError, self).__init__(msg)
            self.python_version = py_ver

    class Timeout(object):

        """Timeout class using ALARM signal."""

        def __init__(self, sec):
            self.sec = sec

        def __enter__(self):
            signal.signal(signal.SIGALRM, self.raise_timeout)
            signal.alarm(self.sec)

        def __exit__(self, *args):
            signal.alarm(0)    # disable alarm

        def raise_timeout(self, *args):
            raise Exception("Timeout exceeded")

    class DBEngineNotRunning(Exception):
        pass

    class ConfiguredWorkflowEngine(soma_workflow.engine.ConfiguredWorkflowEngine):

        def __init__(self, database_server, scheduler, config):
            soma_workflow.engine.ConfiguredWorkflowEngine.__init__(
                self,
                database_server,
                scheduler,
                config)

    class ConnectionChecker(soma_workflow.connection.ConnectionChecker):

        def __init__(self, interval=1, control_interval=3):
            """On client will sleep for interval time, on
            server side will sleep for control_interval"""
            soma_workflow.connection.ConnectionChecker.__init__(
                self,
                interval,
                control_interval)

    class Configuration(soma_workflow.configuration.Configuration):

        def __init__(self,
                     resource_id,
                     mode,
                     scheduler_type,
                     database_file,
                     transfered_file_dir,
                     submitting_machines=None,
                     cluster_address=None,
                     server_name=None,
                     queues=None,
                     queue_limits=None,
                     drmaa_implementation=None,
                     running_jobs_limits=None):
            soma_workflow.configuration.Configuration.__init__(
                self,
                resource_id,
                mode,
                scheduler_type,
                database_file,
                transfered_file_dir,
                submitting_machines,
                cluster_address,
                server_name,
                queues,
                queue_limits,
                drmaa_implementation,
                running_jobs_limits=running_jobs_limits,
            )

    class LocalSchedulerCfg(soma_workflow.configuration.LocalSchedulerCfg):

        def __init__(self, proc_nb=0, interval=1, max_proc_nb=0):
            soma_workflow.configuration.LocalSchedulerCfg.__init__(
                self,
                proc_nb=proc_nb,
                interval=interval,
            )

    def start_database_server(resource_id, logger):
        from soma_workflow import subprocess
        if logger:
            logger.info('Trying to start database server:' + resource_id)
            logger.debug(
                "Debug: Starting database server, isPython?: {}".format(sys.executable))
            logger.debug("Resource_id is: {}".format(resource_id))
            logger.info(os.path.basename(sys.executable) + ' -m' +
                        ' soma_workflow.start_database_server ' + resource_id)
        python_interpreter = sys.executable  # os.path.basename(sys.executable)
        print('start_database_server:\n', python_interpreter,
                                 '-m',
                                 'soma_workflow.start_database_server',
                                 resource_id)
        return subprocess.Popen([python_interpreter,
                                 '-m',
                                 'soma_workflow.start_database_server',
                                 resource_id],
                                close_fds=True,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)

    def get_database_server_proxy(config, logger):
        # Checking if the database server is running
        # if it is running we get its uri
        # else we launch it and get its uri

        path = os.path.split(config.get_server_log_info()[0])[0]
        # get URI filename with python version suffix
        full_file_name = os.path.join(path, "database_server_uri.py.txt")
        logger.info("DEBUG full file name: " + full_file_name)
        if os.path.exists(full_file_name):
            try:
                with open(full_file_name, 'r') as f:
                    uri_dict = json.load(f)
                uri = uri_dict.get('server_uri')
                logger.debug(uri)
                hotname = uri_dict.get('hostname')  # currently unused
                pid = uri_dict.get('pid')
                py_ver = [int(x)
                          for x in uri_dict.get('python_version').split('.')]

                if uri:
                    # Check that the database is running
                    # and add not been killed with a -9 signal for instance
                    # without removing the .txt file containing its uri
                    logger.info("Using the file to find the database server "
                                "that is running if it hasn't been stopped in "
                                "the meantime.")

                    command = 'ps ux | grep soma_workflow.start_database_server | grep %d | grep -v grep' % pid
                    try:
                        output = subprocess.check_output(
                            command, shell=True).decode(encoding='utf-8',
                                                        errors='ignore')
                        output = output.strip().split('\n')
                    except subprocess.CalledProcessError:
                        output = []

                    output = [o for o in output if o.split()[1] == str(pid)]
                    logger.debug("Output of grep is: " + repr(output))
                    # will always be true anyway since grep exit status is 1
                    # when there is no matching pattern and therefore
                    # check_output raises an exception.
                    if len(output) > 1:
                        logger.warning("Warning: several database server "
                                       "processes match the pid (should not "
                                       "happen...)")
                    if len(output) != 0:
                        if py_ver[0] != sys.version_info[0]:
                            raise VersionError('version mismatch', py_ver)
                        logger.info("Connection to an already opened database")
                        data_base_proxy = zro.Proxy(uri)
                        return data_base_proxy
            except VersionError as e:
                msg = "A database server has already been running with a " \
                      "different version of Python (%d), which is not " \
                      "allowed. You should shut the running server down, " \
                      "and remove the database file, before using this " \
                      "version of python (%d). If you need to run both, " \
                      "please setup and use a different configuration " \
                      "directory, using a different computing resource name." \
                      % (e.python_version[0], sys.version_info[0])
                logger.exception(msg)
                # also print on stdout, which the client will capture
                print(msg)
                raise
            except Exception:
                logger.exception("Warning: the database server has been shut "
                                 "down or died and the "
                                 "database_server_uri.py.txt file has not "
                                 "been removed.")

        logger.info(
            'Launching database server and getting a proxy object on it')
        # We don't need the handle since the database server will continue
        # to run indepently of the server engine.
        subprocess_db_server_handle = start_database_server(
            resource_id, logger)
        logger.debug(
            'Waiting for the database server process to write something')
        output = subprocess_db_server_handle.stdout.readline()
        output = output.strip().decode()

        (db_name, uri) = output.split(': ')

        logger.debug('Name of the database server is: ' + repr(db_name))
        logger.debug('Server URI: ' + repr(uri))

        database_server_proxy = zro.Proxy(uri)

        logger.debug("You should not have to erase the text file containing "
                     "the database server engine uri by hand, there is still"
                     "a bug to remove.")
        is_accessible = database_server_proxy.test()

        logger.debug('Database server is accessible: ' + repr(is_accessible))

        return database_server_proxy

    # main server program
    def main(resource_id, engine_name, log=""):

        database_server = None
        config = Configuration.load_from_file(resource_id)
        config.mk_config_dirs()

        (engine_log_dir,
         engine_log_format,
         engine_log_level) = config.get_engine_log_info()

        if engine_log_dir:
            logfilepath = os.path.join(os.path.abspath(engine_log_dir),
                                       "log_" + engine_name + log)
            if False:
                print('logs: ', logfilepath, engine_log_format,
                      engine_log_level)
            logging.basicConfig(
                filename=logfilepath,
                format=engine_log_format,
                level=eval("logging." + engine_log_level))
            logger = logging.getLogger('engine')  # TODO why??
            logger.info(" ")
            logger.info("****************************************************")
            logger.info("****************************************************")
        else:
            # logger = None
            logger = logging.getLogger('engine')

        sch = scheduler.build_scheduler(config.get_scheduler_type(), config)

        if config.get_scheduler_type() \
                == soma_workflow.configuration.MPI_SCHEDULER:
            logger.info("using MPI_SCHEDULER")
            sch = None
            database_server = WorkflowDatabaseServer(
                config.get_database_file(),
                config.get_transfered_file_dir(),
                remove_orphan_files=config.get_remove_orphan_files())
        else:
            database_server = get_database_server_proxy(config, logger)
            logger.debug("database_server launched")

        # initialisation of the zro object server.
        logger.info("Starting object server for the workflow engine")
        # logging.getLogger('zro.ObjectServer').setLevel(logging.DEBUG)
        obj_serv = zro.ObjectServer()

        logger.info("Instanciation of the workflow engine")
        workflow_engine = ConfiguredWorkflowEngine(database_server,
                                                   sch,
                                                   config)

        #
        # Register the objects as remote accessible objects
        #

        logger.info("Registering objects and sending their uri to the client.")
        uri_engine = obj_serv.register(workflow_engine)

        sys.stdout.write(engine_name + " " + str(uri_engine) + "\n")
        sys.stdout.flush()

        # connection checker
        connection_checker = ConnectionChecker()

        uri_cc = obj_serv.register(connection_checker)

        sys.stdout.write("connection_checker " + str(uri_cc) + "\n")
        sys.stdout.flush()

        # configuration
        uri_config = obj_serv.register(config)

        sys.stdout.write("configuration " + str(uri_config) + "\n")
        sys.stdout.flush()

        # scheduler configuration
        if config.get_scheduler_config():
            uri_sched_config = obj_serv.register(config.get_scheduler_config())

            sys.stdout.write("scheduler_config " + str(uri_sched_config)
                             + "\n")
        else:
            sys.stdout.write("scheduler_config None\n")
        # sys.stdout.flush()

        sys.stdout.write(
            "zmq " + zmq.__version__ + " " + repr(sys.path) + '\n')
        sys.stdout.flush()
        # print(sys.path, file=open('/tmp/WTF','a'))

        #
        # Daemon request loop thread
        #
        logging.info(
            "Launching a threaded request loop for the object server.")
        daemon_request_loop_thread = threading.Thread(name="zro_serve_forever",
                                                      target=obj_serv.serve_forever)

        daemon_request_loop_thread.daemon = True
        daemon_request_loop_thread.start()

        logging.debug(
            "Thread object server (obj_serv): " + str(daemon_request_loop_thread))

        logger.info("******** before client connection ******************")
        client_connected = False
        timeout = 40
        try:
            while not client_connected and timeout > 0:
                client_connected = connection_checker.isConnected()
                logger.debug(
                    "Client connection status: " + repr(client_connected))
                timeout = timeout - 1
                time.sleep(1)

            logger.info("******** first mode: client connection *************")
            while client_connected:
                logger.debug(
                    "client is connected we sleep multiple times one second")
                client_connected = connection_checker.isConnected()
                time.sleep(1)

            logger.info("******** client disconnection **********************")

            # obj_serv.sock.close()  # free the port

            # TODO add a destructor if necessary.
            # del (daemon)

            logger.info(
                "******** second mode: wait for jobs to finish ********")
            jobs_running = True
            while jobs_running:
                jobs_running = not workflow_engine.engine_loop.are_jobs_and_workflow_done(
                )
                time.sleep(1)

            logger.info(
                "******** jobs are done ! Shuting down workflow engine ***************************")
            workflow_engine.engine_loop_thread.stop()
            obj_serv.stop()
            if hasattr(database_server, 'interrupt_after'):
                # interrupt / disconnect the database server if it's a proxy
                database_server.interrupt_after(0)
        except Exception as e:
            logger.exception(e)

        sch.clean()
        sys.exit()

    if not len(sys.argv) == 3 and not len(sys.argv) == 4:
        sys.stdout.write("start_workflow_engine takes 2 arguments:\n")
        sys.stdout.write("   1. resource id \n")
        sys.stdout.write("   2. name of the engine object. \n")
    else:
        resource_id = sys.argv[1]
        engine_name = sys.argv[2]
        if len(sys.argv) == 3:
            main(resource_id, engine_name)
        if len(sys.argv) == 4:
            main(resource_id, engine_name, sys.argv[3])
