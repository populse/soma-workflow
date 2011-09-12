#!/usr/bin/env python

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

  import Pyro.naming
  import Pyro.core
  from Pyro.errors import PyroError, NamingError
  
  import soma.workflow.engine
  import soma.workflow.scheduler
  import soma.workflow.connection 
  from soma.workflow.configuration import Configuration
  from soma.workflow.errors import EngineError


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
    
   config = Configuration(resource_id)

   (engine_log_dir,
   engine_log_format,
   engine_log_level) = config.get_engine_log_info()
   if engine_log_dir:
    logfilepath = os.path.join(os.path.abspath(engine_log_dir),  "log_" + engine_name + log)
    logging.basicConfig(
          filename=logfilepath,
          format=engine_log_format,
          level=eval("logging." + engine_log_level))
    logger = logging.getLogger('engine')
    logger.info(" ")
    logger.info("****************************************************")
    logger.info("****************************************************")

   
    ###########################
    # Looking for the database_server
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
      raise EngineError("Could not find the database server supposed to be "
                        "registered on the Pyro name server: %s %s" %(type(e), e))

    database_server = Pyro.core.getProxyForURI(uri)
    
    #Pyro.config.PYRO_MULTITHREADED = 0
    Pyro.core.initServer()
    daemon = Pyro.core.Daemon()

    drmaa = soma.workflow.scheduler.Drmaa(config.get_drmaa_implementation(), 
                                       config.get_parallel_job_config())

    engine_loop = soma.workflow.engine.WorkflowEngineLoop(database_server,
                                             drmaa,
                                             config.get_path_translation(),
                                             config.get_queue_limits())
    
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
