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
    def __init__(self, database_server, engine_loop):
      Pyro.core.SynchronizedObjBase.__init__(self)
      soma.workflow.engine.WorkflowEngine.__init__(self, 
                                                   database_server, 
                                                   engine_loop)
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
    config_path = os.getenv('SOMA_WORKFLOW_CONFIG')
    if not os.path.isfile(config_path):
      config_path = os.path.expanduser("~/.soma-workflow.cfg")
    if not os.path.isfile(config_path):
      config_path = os.path.dirname(__file__)
      config_path = os.path.dirname(__file__)
      config_path = os.path.dirname(__file__)
      config_path = os.path.dirname(__file__)
      config_path = os.path.join(config_path, "etc/soma-workflow.cfg")
    if not os.path.isfile(config_path):
      config_path = "/etc/soma-workflow.cfg"
    if not os.path.isfile(config_path):
      raise Exception("Can't find the soma-workflow configuration file \n")

    config = ConfigParser.ConfigParser()
    config.read(config_path)
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
    
    logger = logging.getLogger('engine')
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
      raise Exception('Couldn\'t find' + server_name + ' nameserver says:',x)
    database_server = Pyro.core.getProxyForURI(uri)
    

    ###########################
    # Parallel job specific information
    parallel_config= {}
    for parallel_info in constants.PARALLEL_JOB_ENV + constants.PARALLEL_DRMAA_ATTRIBUTES + constants.PARALLEL_CONFIGURATIONS:
      if config.has_option(section, parallel_info):
        parallel_config[parallel_info] = config.get(section, 
                                                    parallel_info)
    #############################
    # Drmaa implementation
    drmaa_implem = None
    if config.has_option(section, constants.OCFG_DRMAA_IMPLEMENTATION):
      drmaa_implem = config.get(section, constants.OCFG_DRMAA_IMPLEMENTATION)

    #############################
    # Job limits per queue
    queue_limits = {}
    if config.has_option(section, constants.OCFG_MAX_JOB_IN_QUEUE):
      logger.info("Job limits per queue:")
      queue_limits_str = config.get(section, constants.OCFG_MAX_JOB_IN_QUEUE)
      for info_str in queue_limits_str.split():
        info = info_str.split("{")
        if len(info[0]) == 0:
          queue_name = None
        else:
          queue_name = info[0]
        max_job = int(info[1].rstrip("}"))
        queue_limits[queue_name] = max_job
        logger.info(" " + repr(queue_name) + " " 
                    " => " + repr(max_job))
      logger.info("End of queue limit list")
    else:
      logger.info("No job limit on queues")

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
        logger.info(" * " + namespace + \
                    " : " + filename)
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
              logger.info("  uuid: " + uuid + " => " + contents)
              path_translation[namespace][uuid] = contents
            line = f.readline()
          f.close()
      logger.info("End of path translation list")
    #############################

    #Pyro.config.PYRO_MULTITHREADED = 0
    Pyro.core.initServer()
    daemon = Pyro.core.Daemon()

    drmaa = soma.workflow.engine.Drmaa(drmaa_implem, 
                                       parallel_config)

    engine_loop = soma.workflow.engine.WorkflowEngineLoop(database_server,
                                                          drmaa,
                                                          path_translation,
                                                          queue_limits)
    
    workflow_engine = WorkflowEngine(database_server, 
                                     engine_loop)

    engine_loop_thread = soma.workflow.engine.EngineLoopThread(engine_loop)
    engine_loop_thread.setDaemon(True)
    engine_loop_thread.start()
    
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
    del(workflow_engine)
    
    logger.info("******** second mode: waiting for jobs to finish****")
    jobs_running = True
    while jobs_running:
      jobs_running = not engine_loop.are_jobs_and_workflow_done()
      time.sleep(1)
    
    logger.info("******** jobs are done ! ***************************")
    engine_loop_thread.stop()
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
