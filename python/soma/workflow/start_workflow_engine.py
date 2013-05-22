#!/usr/bin/env python

'''
@author: Soizic Laguitton

@organization: I2BM, Neurospin, Gif-sur-Yvette, France
@organization: CATI, France
@organization: U{IFR 49<http://www.ifr49.org>}

@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''



if __name__=="__main__":

  import sys
  import threading
  import time 
  import logging
  import os

  import Pyro.naming
  import Pyro.core
  from Pyro.errors import PyroError, NamingError, ProtocolError
  
  import soma.workflow.engine
  import soma.workflow.scheduler
  import soma.workflow.connection 
  import soma.workflow.configuration
  from soma.workflow.errors import NoDrmaaLibError, EngineError



  ###### WorkflowEngine pyro object
  class ConfiguredWorkflowEngine(Pyro.core.SynchronizedObjBase, 
                       soma.workflow.engine.ConfiguredWorkflowEngine):
    def __init__(self, database_server, scheduler, config):
      Pyro.core.SynchronizedObjBase.__init__(self)
      soma.workflow.engine.ConfiguredWorkflowEngine.__init__(self, 
                                                   database_server, 
                                                   scheduler,
                                                   config)
    pass
    
  class ConnectionChecker(Pyro.core.ObjBase, 
                          soma.workflow.connection.ConnectionChecker):
    def __init__(self, interval=1, control_interval=3):
      Pyro.core.ObjBase.__init__(self)
      soma.workflow.connection.ConnectionChecker.__init__(self, 
                                                          interval, 
                                                          control_interval)
    pass


  class Configuration(Pyro.core.ObjBase, 
                      soma.workflow.configuration.Configuration):

    def __init__( self,
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
                  drmaa_implementation=None):
      Pyro.core.ObjBase.__init__(self)
      soma.workflow.configuration.Configuration.__init__(self,
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
                                                         drmaa_implementation)
      pass

  def get_database_server_proxy(config, logger):
    try:
      Pyro.core.initClient()
      locator = Pyro.naming.NameServerLocator()
      name_server_host = config.get_name_server_host() 
      if name_server_host == 'None':
        ns = locator.getNS()
      else: 
        ns = locator.getNS(host=name_server_host)
    except Exception, e:
      raise EngineError("Could not find the Pyro name server.")

    server_name = config.get_server_name()

    try:
      uri = ns.resolve(server_name)
      logger.info('Server URI:'+ repr(uri))
    except NamingError,e:
      raise EngineError("%s %s \n"   
                        "Could not find soma-workflow database server. "
                        "Run the following command on %s to start the server: \n"
                        "python -m soma.workflow.start_database_server %s" %(type(e), e, name_server_host, resource_id))

    database_server = Pyro.core.getProxyForURI(uri)
	
    try:
      database_server.test()
    except ProtocolError,e:
      raise EngineError("%s %s \n"
	                "The database server might not be running. "
                        "Run the following command on %s to start the server: \n"
                        "python -m soma.workflow.start_database_server %s" %(type(e), e, name_server_host, resource_id))
    return database_server

    
  
  ###### main server program
  def main(resource_id, engine_name, log = ""):
    
    config = Configuration.load_from_file(resource_id)

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
 
    if config.get_scheduler_type() == soma.workflow.configuration.DRMAA_SCHEDULER:
        
      if not soma.workflow.scheduler.DRMAA_LIB_FOUND:
          raise NoDrmaaLibError
          
      sch = soma.workflow.scheduler.DrmaaCTypes(config.get_drmaa_implementation(), 
                                    config.get_parallel_job_config(),
                                    os.path.expanduser("~"),
                                    configured_native_spec=config.get_native_specification())
      database_server = get_database_server_proxy(config, logger)

    elif config.get_scheduler_type() == soma.workflow.configuration.LOCAL_SCHEDULER:
      from soma.workflow.scheduler import ConfiguredLocalScheduler
      local_scheduler_cfg_file_path = soma.workflow.configuration.LocalSchedulerCfg.search_config_path()
      if local_scheduler_cfg_file_path:
        local_scheduler_config = soma.workflow.configuration.LocalSchedulerCfg.load_from_file(local_scheduler_cfg_file_path)
      else:
      	local_scheduler_config = soma.workflow.configuration.LocalSchedulerCfg()      
      sch = ConfiguredLocalScheduler(local_scheduler_config)
      database_server = get_database_server_proxy(config, logger)

    elif config.get_scheduler_type() == soma.workflow.configuration.MPI_SCHEDULER:
      sch = None 
      database_server = WorkflowDatabaseServer(config.get_database_file(),
                                               config.get_transfered_file_dir())


    #Pyro.config.PYRO_MULTITHREADED = 0
    Pyro.core.initServer()
    daemon = Pyro.core.Daemon()

    workflow_engine = ConfiguredWorkflowEngine(database_server, 
                                               sch,
                                               config)

    # connection to the pyro daemon and output its URI 
    uri_engine = daemon.connect(workflow_engine, engine_name)
    sys.stdout.write(engine_name + " " + str(uri_engine) + "\n")
    sys.stdout.flush()
  
    logger.info('Pyro object ' + engine_name + ' is ready.')
    
    # connection check
    connection_checker = ConnectionChecker()
    uri_cc = daemon.connect(connection_checker, 'connection_checker')
    sys.stdout.write("connection_checker " + str(uri_cc) + "\n")
    sys.stdout.flush() 

    # configuration
    uri_config = daemon.connect(config, 'configuration')
    sys.stdout.write("configuration " + str(uri_config) + "\n")
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
    daemon.shutdown(disconnect=True) #stop the request loop
    daemon.sock.close() # free the port
    
    del(daemon) 
    
    logger.info("******** second mode: waiting for jobs to finish****")
    jobs_running = True
    while jobs_running:
      jobs_running = not workflow_engine.engine_loop.are_jobs_and_workflow_done()
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
