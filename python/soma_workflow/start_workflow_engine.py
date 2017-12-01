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

    import Pyro4

    import soma_workflow.engine
    import soma_workflow.scheduler
    import soma_workflow.connection
    import soma_workflow.configuration
    from soma_workflow.errors import NoDrmaaLibError, EngineError
    from soma_workflow.database_server import WorkflowDatabaseServer
    from soma_workflow.scheduler import ConfiguredLocalScheduler
    import time


    @Pyro4.expose
    class ConfiguredWorkflowEngine(soma_workflow.engine.ConfiguredWorkflowEngine):

        def __init__(self, database_server, scheduler, config):
            soma_workflow.engine.ConfiguredWorkflowEngine.__init__(
                self,
                database_server,
                scheduler,
                config)


    @Pyro4.expose
    class ConnectionChecker(soma_workflow.connection.ConnectionChecker):

        def __init__(self, interval=1, control_interval=3):
            soma_workflow.connection.ConnectionChecker.__init__(
                self,
                interval,
                control_interval)

        pass


    @Pyro4.expose
    class Configuration(soma_workflow.configuration.Configuration):

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


    @Pyro4.expose
    class LocalSchedulerCfg(soma_workflow.configuration.LocalSchedulerCfg):

        def __init__(self, proc_nb=0, interval=1, max_proc_nb=0):
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
        ##print("Debug: Starting database server, isPython?: {}".format(sys.executable))
        ##print("resource_id is: {}".format(resource_id))
        subprocess.Popen(
            [sys.executable,
             '-m', 'soma_workflow.start_database_server', resource_id],
            close_fds=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


    def get_database_server_proxy(config, logger):
        name_server_host = config.get_name_server_host()
        ###TODO added print()
        ##print("Debug: name_server_host: {}".format(name_server_host))

        starting_server = False
        #TODO bancal
        started = False

        try:
            server_name = config.get_server_name()


            ##print("Debug: server_name is: {}".format(server_name))
            ###TODO
            ###This is to init a client which means that
            ###both client and server code are in this file
            #

            ###
            # Pyro.core.initClient()
            # locator = Pyro.naming.NameServerLocator()
            # if name_server_host == 'None':
            #     ns = locator.getNS()
            # else:
            #     ns = locator.getNS(host=name_server_host)
            #
            # uri = ns.resolve(server_name)

            uri = ""

            with Pyro4.locateNS() as ns:
                uri = ns.list()[server_name]

            logger.info('Server URI:' + repr(uri))
            ##print("Debug: server URI is: {}".format(repr(uri)))

            #TODO bancal, que se passe-t-il quand le serveur n'est pas
            #déjà lancé
            database_server = Pyro4.Proxy(uri)

            started = database_server.test()


        except Exception as e:
            # First try to launch the database server
            ##print(e)
            import subprocess
            logger.info('Trying to start database server:' + resource_id)
            ##print('Trying to start database server: ' + resource_id)
            start_database_server(resource_id, logger)
            name_server_host = 'localhost'
            starting_server = True

        #TODO test
        #assert starting_server == True

        timeout = 35
        start_time = time.time()
        while not started and time.time() - start_time < timeout:

            try:
                ###TODO?
                # if name_server_host == 'None':
                #     ns = locator.getNS()
                # else:
                #     ns = locator.getNS(host=name_server_host)
                #
                # uri = ns.resolve(server_name)
                # logger.info('Server URI:' + repr(uri))
                #
                # ###TODO
                # database_server = Pyro.core.getProxyForURI(uri)

                uri = ""

                with Pyro4.locateNS() as ns:
                    uri = ns.list()[server_name]

                logger.info('Server URI:' + repr(uri))

                database_server = Pyro4.Proxy(uri)

                started = database_server.test()

            except:
                if starting_server:
                    logger.info(
                        'Database server:' + resource_id
                        + ' was not started. Waiting 1 sec...')
                    ##print("waiting for the databaser server to start")
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

            ##print("DRMAA_SCHEDULER")
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

            ##print("LOCAL_SCHEDULER")
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

            ##print("MPI_SCHEDULER")
            sch = None
            database_server = WorkflowDatabaseServer(
                config.get_database_file(),
                config.get_transfered_file_dir())

        # Pyro.config.PYRO_MULTITHREADED = 0
        ####TODO ?
        ###initialisation of the Pyro server.
        # Pyro.core.initServer()
        # daemon = Pyro.core.Daemon()
        daemon = Pyro4.Daemon()

        # locate the NS
        # locator = Pyro.naming.NameServerLocator()
        # #print('searching for Name Server...')
        #
        # #with Pyro4.locateNS() as ns:
        # try:
        #     ###TODO?
        #     name_server_host = config.get_name_server_host()
        #     if name_server_host == 'None':
        #         ns = locator.getNS()
        #     else:
        #         ns = locator.getNS(host=name_server_host)
        #     ###TODO unecessary just need to register objects or URI?
        #     daemon.useNameServer(ns)
        # except:
        #      print('Name Server not found.')
        #      raise

        ##print("Is database_server accessible: {}".format(database_server.test()))
        #TODO test
        #database_server.clean()

        workflow_engine = ConfiguredWorkflowEngine(database_server,
                                                   sch,
                                                   config)

        # connection to the pyro daemon and output its URI
        ###TODO
        # try:
        #     ns.unregister(engine_name)
        # except NamingError:
        #     pass
        # uri_engine = daemon.connect(workflow_engine, engine_name)

        uri_engine = daemon.register(workflow_engine, engine_name)
        with Pyro4.locateNS() as ns:
            ns.remove(engine_name) #in case it was left by someone else.
            #What if multiple people are using the same name server?!?
            ns.register(engine_name, uri_engine)

        sys.stdout.write(engine_name + " " + str(uri_engine) + "\n")
        sys.stdout.flush()

        logger.info('Pyro object ' + engine_name + ' is ready.')

        # connection check
        ###TODO
        connection_checker = ConnectionChecker()
        # try:
        #     ns.unregister('connection_checker')
        # except NamingError:
        #     pass
        # uri_cc = daemon.connect(connection_checker, 'connection_checker')

        uri_cc = daemon.register(connection_checker, 'connection_checker')
        with Pyro4.locateNS() as ns:
            ns.register('connection_checker', uri_cc)

        sys.stdout.write("connection_checker " + str(uri_cc) + "\n")
        sys.stdout.flush()

        # configuration
        ###TODO
        # try:
        #     ns.unregister('configuration')
        # except NamingError:
        #     pass
        # uri_config = daemon.connect(config, 'configuration')

        uri_config = daemon.register(config, 'configuration')
        with Pyro4.locateNS() as ns:
            ns.register('configuration', uri_config)

        sys.stdout.write("configuration " + str(uri_config) + "\n")
        sys.stdout.flush()

        # local scheduler config
        ###TODO
        # try:
        #     ns.unregister('scheduler_config')
        # except NamingError:
        #     pass

        if config.get_scheduler_config():
            # uri_sched_config = daemon.connect(config.get_scheduler_config(),
            #                                  'scheduler_config')
            uri_sched_config = daemon.register(config.get_sheduler_config(), 'scheduler_config')
            with Pyro4.locateNS() as ns:
                ns.register('scheduler_config', uri_sched_config)

            sys.stdout.write("scheduler_config " + str(uri_sched_config)
                             + "\n")
        else:
            sys.stdout.write("scheduler_config None\n")
        sys.stdout.flush()

        # Daemon request loop thread
        ###TODO no need of this information
        #logger.info("daemon port = " + repr(daemon.port))
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
        ###TODO ?
        # seems the daemon is on only for a short period of time
        # why is that so????

        daemon.shutdown()
        #cleaning up the nameserver:
        with Pyro4.locateNS() as ns:
            ns.remove('scheduler_config')
            ns.remove('configuration')
            ns.remove('connection_checker')
            ns.remove(engine_name)


        #daemon.shutdown(disconnect=True)  # stop the request loop
        #daemon.sock.close()  # free the port

        del (daemon)

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
