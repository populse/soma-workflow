'''
@author: Soizic Laguitton
@organization: U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''


if __name__=="__main__":

  import sys
  import threading
  import time 
  import logging
  import os
  import ConfigParser

  import Pyro.naming
  import Pyro.core
  from Pyro.errors import PyroError, NamingError
  
  import soma.workflow.engine
  import soma.workflow.connection 
  import soma.workflow.constants as constants

        
  ###### WorkflowEngine pyro object
  class WorkflowEngine(Pyro.core.SynchronizedObjBase, 
                       soma.workflow.engine.WorkflowEngine):
    def __init__(self, database_server, drmaa_workflow_engine):
      Pyro.core.SynchronizedObjBase.__init__(self)
      soma.workflow.engine.WorkflowEngine.__init__(self, 
                                                   database_server, 
                                                   drmaa_workflow_engine)
    pass
    
  class ConnectionChecker(Pyro.core.ObjBase, 
                          soma.workflow.connection.ConnectionChecker):
    def __init__(self, interval=1, control_interval=3):
      Pyro.core.ObjBase.__init__(self)
      soma.workflow.connection.ConnectionChecker.__init__(self, 
                                                          interval, 
                                                          control_interval)
    pass
  
  ###### main server program
  def main(resource_id, engine_name, log = ""):
    
    #########################
    # reading configuration 
    config = ConfigParser.ConfigParser()
    config_file_path = os.environ['SOMA_WORKFLOW_CONFIG']
    config.read(config_file_path)
    section = resource_id
    
    ###########
    # log file 
    if not config.get(section, constants.OCFG_ENGINE_LOG_DIR) == 'None':
      logfilepath = config.get(section, 
                               constants.OCFG_ENGINE_LOG_DIR)+ "log_" + \
                               engine_name + log
      logging.basicConfig(
        filename=logfilepath,
        format=config.get(section, constants.OCFG_ENGINE_LOG_FORMAT, 1),
        level=eval("logging." + \
                   config.get(section, constants.OCFG_ENGINE_LOG_LEVEL)))
    
    logger = logging.getLogger('ljp')
    logger.info(" ")
    logger.info("****************************************************")
    logger.info("****************************************************")
   
    ###########################
    # Looking for the database_server
    Pyro.core.initClient()
    locator = Pyro.naming.NameServerLocator()
    name_server_host = config.get(section, constants.CFG_NAME_SERVER_HOST)
    if name_server_host == 'None':
      ns = locator.getNS()
    else: 
      ns = locator.getNS(host=name_server_host)

    server_name = config.get(section, constants.CFG_SERVER_NAME)

    try:
      uri = ns.resolve(server_name)
      logger.info('Server URI:'+ repr(uri))
    except NamingError,x:
      logger.critical('Couldn\'t find' + server_name + ' nameserver says:',x)
      raise SystemExit
    database_server = Pyro.core.getProxyForURI(uri)
    
    

    ###########################
    # Parallel job specific information
    parallel_config= {}
    for parallel_info in constants.PARALLEL_JOB_ENV + constants.PARALLEL_DRMAA_ATTRIBUTES + constants.PARALLEL_CONFIGURATIONS:
      if config.has_option(section, parallel_info):
        parallel_config[parallel_info] = config.get(section, 
                                                    parallel_info)
    #############################
    
    #############################
    # Translation files specific information 
    path_translation = None
    if config.has_option(section, constants.OCFG_PATH_TRANSLATION_FILES):
      path_translation={}
      translation_files_str = config.get(section, 
                                         constants.OCFG_PATH_TRANSLATION_FILES)
      logger.info("Path translation files configured:")
      for ns_file_str in translation_files_str.split():
        ns_file = ns_file_str.split("{")
        namespace = ns_file[0]
        filename = ns_file[1].rstrip("}")
        logger.info(" -namespace: " + namespace + \
                    ", translation file: " + filename)
        try: 
          f = open(filename, "r")
        except IOError, e:
          logger.info("Couldn't read the translation file: " + filename)
        else:
          if not namespace in path_translation.keys():
            path_translation[namespace] = {}
          line = f.readline()
          while line:
            splitted_line = line.split(None,1)
            if len(splitted_line) > 1:
              uuid = splitted_line[0]
              contents = splitted_line[1].rstrip()
              logger.info("      uuid: " + uuid + "   translation:" + contents)
              path_translation[namespace][uuid] = contents
            line = f.readline()
          f.close()
      logger.info("End of path translation list")
    #############################

    #Pyro.config.PYRO_MULTITHREADED = 0
    Pyro.core.initServer()
    daemon = Pyro.core.Daemon()


    
    drmaa_engine = soma.workflow.engine.DrmaaWorkflowEngine(database_server, 
                                                            parallel_config, 
                                                            path_translation)
    
    engine = WorkflowEngine(database_server, drmaa_engine)
    engine_lock = threading.Lock()
    
    # connection to the pyro daemon and output its URI 
    uri_engine = daemon.connect(engine, engine_name)
    sys.stdout.write(engine_name + " " + str(uri_engine) + "\n")
    sys.stdout.flush()
  
    logger.info('Pyro object ' + engine_name + ' is ready.')
    

    # connection check
    connection_checker = ConnectionChecker()
    uri_cc = daemon.connect(connection_checker, 'connection_checker')
    sys.stdout.write("connection_checker " + str(uri_cc) + "\n")
    sys.stdout.flush() 
    
    # Daemon request loop thread
    logger.info("daemon port = " + repr(daemon.port))
    daemon_request_loop_thread = threading.Thread(name = "daemon.requestLoop", 
                                                  target = daemon.requestLoop) 
  
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
    del(engine)
    
    logger.info("******** second mode: waiting for jobs to finish****")
    jobs_running = True
    while jobs_running:
      jobs_running = not drmaa_engine.areJobsDone()
      time.sleep(1)
    
    logger.info("******** jobs are done ! ***************************")
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
