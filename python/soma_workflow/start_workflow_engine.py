#!/usr/bin/env python

'''
@author: Soizic Laguitton

@organization: I2BM, Neurospin, Gif-sur-Yvette, France
@organization: CATI, France
@organization: U{IFR 49<http://www.ifr49.org>}

@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''


if __name__ == "__main__":

    import sys
    import threading
    import time
    import logging
    import os

    import Pyro.naming
    import Pyro.core
    from Pyro.errors import PyroError, NamingError, ProtocolError

    import soma_workflow.engine
    import soma_workflow.scheduler
    import soma_workflow.connection
    import soma_workflow.configuration
    from soma_workflow.errors import NoDrmaaLibError, EngineError
    from soma_workflow.database_server import WorkflowDatabaseServer
    from soma_workflow.scheduler import ConfiguredLocalScheduler
    import time

    # WorkflowEngine pyro object
    class ConfiguredWorkflowEngine(
            Pyro.core.SynchronizedObjBase,
            soma_workflow.engine.ConfiguredWorkflowEngine):

        def __init__(self, database_server, scheduler, config):
            Pyro.core.SynchronizedObjBase.__init__(self)
            soma_workflow.engine.ConfiguredWorkflowEngine.__init__(
                self,
                database_server,
                scheduler,
                config)
        pass

    class ConnectionChecker(Pyro.core.ObjBase,
                            soma_workflow.connection.ConnectionChecker):

        def __init__(self, interval=1, control_interval=3):
            Pyro.core.ObjBase.__init__(self)
            soma_workflow.connection.ConnectionChecker.__init__(
                self,
                interval,
                control_interval)
        pass

    class Configuration(Pyro.core.ObjBase,
                        soma_workflow.configuration.Configuration):

        def __init__(self,
                     resource_id,
                     mode,
                     scheduler_type,
                     database_file,
                     transfered_file_dir,
                     submitting_machines=None,
                     cluster_address=None,
                     name_server_host=None,
                     server_name=None,
                     queues=None,
                     queue_limits=None,
                     drmaa_implementation=None,
                     running_jobs_limits=None):
            Pyro.core.ObjBase.__init__(self)
            soma_workflow.configuration.Configuration.__init__(
                self,
                resource_id,
                mode,
                scheduler_type,
                database_file,
                transfered_file_dir,
                submitting_machines,
                cluster_address,
                name_server_host,
                server_name,
                queues,
                queue_limits,
                drmaa_implementation,
                running_jobs_limits=running_jobs_limits,
            )
            pass

    class LocalSchedulerCfg(Pyro.core.ObjBase,
                            soma_workflow.configuration.LocalSchedulerCfg):

        def __init__(self, proc_nb=0, interval=1, max_proc_nb=0):
            Pyro.core.ObjBase.__init__(self)
            soma_workflow.configuration.LocalSchedulerCfg.__init__(
                self,
                proc_nb=proc_nb,
                interval=interval,
            )


    def start_database_server(resource_id, logger):
        import subprocess
        if logger:
            logger.info('Trying to start database server:'
                        + resource_id)
        subprocess.Popen(
            [sys.executable,
              '-m', 'soma_workflow.start_database_server', resource_id],
            close_fds=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


    def get_database_server_proxy(config, logger):
        name_server_host = config.get_name_server_host()
        starting_server = False
        try:
            server_name = config.get_server_name()

            Pyro.core.initClient()
            locator = Pyro.naming.NameServerLocator()
            if name_server_host == 'None':
                ns = locator.getNS()
            else:
                ns = locator.getNS(host=name_server_host)

            uri = ns.resolve(server_name)
            logger.info('Server URI:' + repr(uri))

        except:
            # First try to launch the database server
            import subprocess
            logger.info('Trying to start database server:' + resource_id)
            start_database_server(resource_id, logger)
            name_server_host = 'localhost'
            starting_server = True

        timeout = 35
        start_time = time.time()
        started = False
        while not started and time.time() - start_time < timeout:

            try:
                if name_server_host == 'None':
                    ns = locator.getNS()
                else:
                    ns = locator.getNS(host=name_server_host)

                uri = ns.resolve(server_name)
                logger.info('Server URI:' + repr(uri))

                database_server = Pyro.core.getProxyForURI(uri)

                started = database_server.test()

            except:
                if starting_server:
                    logger.info(
                        'Database server:' + resource_id
                        + ' was not started. Waiting 1 sec...')
                else:
                    # try to launch the database server
                    start_database_server(resource_id, logger)
                    name_server_host = 'localhost'
                    starting_server = True
                time.sleep(1)

        if not started:
            raise EngineError(
                "The database server might not be running. "
                "Run the following command on %s to start the server: \n"
                "python -m soma_workflow.start_database_server %s"
                % (name_server_host, resource_id))
        else:
            # enter the server loop.
            logger.info('Database server object ready for URI:' + repr(uri))

        return database_server

    # main server program
    def main(resource_id, engine_name, log=""):

        config = Configuration.load_from_file(resource_id)
        config.mk_config_dirs()

        (engine_log_dir,
         engine_log_format,
         engine_log_level) = config.get_engine_log_info()
        if engine_log_dir:
            logfilepath = os.path.join(os.path.abspath(engine_log_dir),
                                       "log_" + engine_name + log)
            logging.basicConfig(
                filename=logfilepath,
                format=engine_log_format,
                level=eval("logging." + engine_log_level))
            logger = logging.getLogger('engine')
            logger.info(" ")
            logger.info("****************************************************")
            logger.info("****************************************************")
        else:
            logger = None

        if config.get_scheduler_type() \
                == soma_workflow.configuration.DRMAA_SCHEDULER:

            if not soma_workflow.scheduler.DRMAA_LIB_FOUND:
                raise NoDrmaaLibError

            sch = soma_workflow.scheduler.DrmaaCTypes(
                config.get_drmaa_implementation(),
                config.get_parallel_job_config(),
                os.path.expanduser("~"),
                configured_native_spec=config.get_native_specification())
            database_server = get_database_server_proxy(config, logger)

        elif config.get_scheduler_type() \
                == soma_workflow.configuration.LOCAL_SCHEDULER:
            local_scheduler_cfg_file_path \
                = LocalSchedulerCfg.search_config_path()
            if local_scheduler_cfg_file_path:
                local_scheduler_config = LocalSchedulerCfg.load_from_file(
                    local_scheduler_cfg_file_path)
            else:
                local_scheduler_config = LocalSchedulerCfg()
            sch = ConfiguredLocalScheduler(local_scheduler_config)
            database_server = get_database_server_proxy(config, logger)
            config.set_scheduler_config(local_scheduler_config)

        elif config.get_scheduler_type() \
                == soma_workflow.configuration.MPI_SCHEDULER:
            sch = None
            database_server = WorkflowDatabaseServer(
                config.get_database_file(),
                config.get_transfered_file_dir())

        # Pyro.config.PYRO_MULTITHREADED = 0
        Pyro.core.initServer()
        daemon = Pyro.core.Daemon()
        # locate the NS
        locator = Pyro.naming.NameServerLocator()
        #print('searching for Name Server...')

        try:
            name_server_host = config.get_name_server_host()
            if name_server_host == 'None':
                ns = locator.getNS()
            else:
                ns = locator.getNS(host=name_server_host)
            daemon.useNameServer(ns)
        except:
             print('Name Server not found.')
             raise

        workflow_engine = ConfiguredWorkflowEngine(database_server,
                                                   sch,
                                                   config)

        # connection to the pyro daemon and output its URI
        try:
            ns.unregister(engine_name)
        except NamingError:
            pass
        uri_engine = daemon.connect(workflow_engine, engine_name)
        sys.stdout.write(engine_name + " " + str(uri_engine) + "\n")
        sys.stdout.flush()

        logger.info('Pyro object ' + engine_name + ' is ready.')

        # connection check
        connection_checker = ConnectionChecker()
        try:
            ns.unregister('connection_checker')
        except NamingError:
            pass
        uri_cc = daemon.connect(connection_checker, 'connection_checker')
        sys.stdout.write("connection_checker " + str(uri_cc) + "\n")
        sys.stdout.flush()

        # configuration
        try:
            ns.unregister('configuration')
        except NamingError:
            pass
        uri_config = daemon.connect(config, 'configuration')
        sys.stdout.write("configuration " + str(uri_config) + "\n")
        sys.stdout.flush()

        # local scheduler config
        try:
            ns.unregister('scheduler_config')
        except NamingError:
            pass
        if config.get_scheduler_config():
            uri_sched_config = daemon.connect(config.get_scheduler_config(),
                                              'scheduler_config')
            sys.stdout.write("scheduler_config " + str(uri_sched_config)
                             + "\n")
        else:
            sys.stdout.write("scheduler_config None\n")
        sys.stdout.flush()

        # Daemon request loop thread
        logger.info("daemon port = " + repr(daemon.port))
        daemon_request_loop_thread = threading.Thread(name="pyro_request_loop",
                                                      target=daemon.requestLoop)

        daemon_request_loop_thread.daemon = True
        daemon_request_loop_thread.start()

        logger.info("******** before client connection ******************")
        client_connected = False
        timeout = 40
        while not client_connected and timeout > 0:
            client_connected = connection_checker.isConnected()
            timeout = timeout - 1
            time.sleep(1)

        logger.info("******** first mode: client connection *************")
        while client_connected:
            client_connected = connection_checker.isConnected()
            time.sleep(1)

        logger.info("******** client disconnection **********************")
        daemon.shutdown(disconnect=True)  # stop the request loop
        daemon.sock.close()  # free the port

        del(daemon)

        logger.info("******** second mode: waiting for jobs to finish****")
        jobs_running = True
        while jobs_running:
            jobs_running = not workflow_engine.engine_loop.are_jobs_and_workflow_done(
            )
            time.sleep(1)

        logger.info("******** jobs are done ! ***************************")
        workflow_engine.engine_loop_thread.stop()

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
