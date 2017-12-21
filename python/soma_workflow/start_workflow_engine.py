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

    import zro

    import soma_workflow.engine
    import soma_workflow.scheduler
    import soma_workflow.connection
    import soma_workflow.configuration
    from soma_workflow.errors import NoDrmaaLibError, EngineError
    from soma_workflow.database_server import WorkflowDatabaseServer
    from soma_workflow.scheduler import ConfiguredLocalScheduler
    import time

    class ConfiguredWorkflowEngine(soma_workflow.engine.ConfiguredWorkflowEngine):

        def __init__(self, database_server, scheduler, config):
            soma_workflow.engine.ConfiguredWorkflowEngine.__init__(
                self,
                database_server,
                scheduler,
                config)

    class ConnectionChecker(soma_workflow.connection.ConnectionChecker):

        def __init__(self, interval=1, control_interval=3):
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
            logger.info('Trying to start database server:' + resource_id)
            logger.debug("Debug: Starting database server, isPython?: {}".format(sys.executable))
            logger.debug("Resource_id is: {}".format(resource_id))
        return subprocess.Popen([sys.executable,
           '-m', 'soma_workflow.start_database_server', resource_id],
           close_fds=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


    def get_database_server_proxy(config, logger):
        name_server_host = config.get_name_server_host()
        logger.debug("Debug: name_server_host: {}".format(name_server_host))

        #Checking if the database server is running
        #if it is running we get its uri
        #else we launch it and get its uri

        try:
            path = os.path.split(config.get_server_log_info()[0])[0]
            full_file_name = os.path.join(path, "database_server_uri.txt")
            logger.debug("DEBUG full file name: " + full_file_name)
            f = open(full_file_name, 'r')
            uri = f.readline().strip()
            logger.debug(uri)
            f.close()
            if uri:
                #create proxy and return
                return zro.Proxy(uri)
        except IOError:
            pass #file does not exist continue
        except Exception as e:
            print(e)

        logger.info('Launching database server and getting a proxy object on it')
        # We don't need the handle since the database server will continue
        # to run indepently of the server engine.
        subprocess_db_server_handle = start_database_server(resource_id, logger)
        logger.debug('Waiting for the database server process to write something')
        output = subprocess_db_server_handle.stdout.readline()

        (db_name, uri) = output.strip().split(': ')

        logger.debug('Name of the database server is: ' + db_name)
        logger.debug('Server URI: ' + repr(uri))

        database_server_proxy = zro.Proxy(uri)

        return database_server_proxy #, subprocess_db_server_handle


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
            logger.info("using DRMAA_SCHEDULER")
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
            logger.info("using LOCAL_SCHEDULER")
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
            logger.info("using MPI_SCHEDULER")
            sch = None
            database_server = WorkflowDatabaseServer(
                config.get_database_file(),
                config.get_transfered_file_dir())

        # initialisation of the zro object server.
        daemon = zro.ObjectServer()

        workflow_engine = ConfiguredWorkflowEngine(database_server,
                                                   sch,
                                                   config)

        ################################################################################
        # Register the objects as remote accessible objects
        ################################################################################
        # connection to the pyro daemon and output its URI

        uri_engine = daemon.register(workflow_engine) #, engine_name)

        sys.stdout.write(engine_name + " " + str(uri_engine) + "\n")
        sys.stdout.flush()

        #logger.info('Pyro object ' + engine_name + ' is ready.')

        # connection checker
        connection_checker = ConnectionChecker()

        uri_cc = daemon.register(connection_checker) #, 'connection_checker')

        sys.stdout.write("connection_checker " + str(uri_cc) + "\n")
        sys.stdout.flush()

        # configuration
        uri_config = daemon.register(config) #, 'configuration')

        sys.stdout.write("configuration " + str(uri_config) + "\n")
        sys.stdout.flush()

        # scheduler configuration
        if config.get_scheduler_config():
            uri_sched_config = daemon.register(config.get_sheduler_config()) #, 'scheduler_config')

            sys.stdout.write("scheduler_config " + str(uri_sched_config)
                             + "\n")
        else:
            sys.stdout.write("scheduler_config None\n")
        sys.stdout.flush()

        ################################################################################
        # Daemon request loop thread
        ################################################################################

        daemon_request_loop_thread = threading.Thread(name="zro_serve_forever",
                                                      target=daemon.serve_forever())
#
#            (name="pyro_request_loop",
#                                                      target=daemon.requestLoop)

        daemon_request_loop_thread.daemon = True
        daemon_request_loop_thread.start()

        logging.debug("Thread pyro principale (daemon): " + str(daemon_request_loop_thread))

        logger.info("******** before client connection ******************")
        client_connected = False
        timeout = 40
        while not client_connected and timeout > 0:
            client_connected = connection_checker.isConnected()  # seem useless since it will be false
            timeout = timeout - 1
            time.sleep(1)

        logger.debug("==>Is client connected?" + str(client_connected))
        logger.debug("Is pyro thread alive? " + str(daemon_request_loop_thread.isAlive()))
        logger.debug("Thread count: " + str(threading.activeCount()))

        logger.info("******** first mode: client connection *************")
        while client_connected:
            logger.debug("client is connected we sleep multiple times one second")
            # TODO here we could check the status of the pyro thread
            # that has been called from the client, how to identify it
            # are they all active?
            client_connected = connection_checker.isConnected()
            time.sleep(1)

        logger.info("******** client disconnection **********************")

        #TODO shutdown cleanly might change serve_forever to serveLoop or
        #sth like this
        #daemon.shutdown()

        #daemon.shutdown(disconnect=True)  # stop the request loop
        #daemon.sock.close()  # free the port

        #TODO add a destructor if necessary.
        #del (daemon)

        logger.info("******** second mode: wait for jobs to finish ********")
        jobs_running = True
        while jobs_running:
            jobs_running = not workflow_engine.engine_loop.are_jobs_and_workflow_done(
                         )
            time.sleep(1)

        logger.info("******** jobs are done ! Shuting down workflow engine ***************************")
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
