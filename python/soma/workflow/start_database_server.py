
if __name__ == '__main__':
  import ConfigParser
  import sys
  import os
  import logging

  import Pyro.naming
  import Pyro.core
  from Pyro.errors import PyroError, NamingError
  
  import soma.workflow.database_server
  import soma.workflow.constants as constants
  
  class WorkflowDatabaseServer(Pyro.core.ObjBase, 
                               soma.workflow.database_server.WorkflowDatabaseServer):
    def __init__(self, 
                 database_file, 
                 tmp_file_dir_path):
      Pyro.core.ObjBase.__init__(self)
      soma.workflow.database_server.WorkflowDatabaseServer.__init__(self, 
                                                           database_file, 
                                                           tmp_file_dir_path)
    pass
  
  if not len(sys.argv) == 2:
    sys.stdout.write("start_database_server takes 1 argument: resource id. \n")
    sys.exit(1)
  
  print "Ressource: " + sys.argv[1]

  #########################
  # reading configuration 
  config_path = os.getenv('SOMA_WORKFLOW_CONFIG')
  if not config_path or not os.path.isfile(config_path):
    config_path = os.path.expanduser("~/.soma-workflow.cfg")
  if not config_path or not os.path.isfile(config_path):
    config_path = os.path.dirname(__file__)
    config_path = os.path.dirname(__file__)
    config_path = os.path.dirname(__file__)
    config_path = os.path.dirname(__file__)
    config_path = os.path.join(config_path, "etc/soma-workflow.cfg")
  if not config_path or not os.path.isfile(config_path):
    config_path = "/etc/soma-workflow.cfg"
  if not config_path or not os.path.isfile(config_path):
    raise Exception("Can't find the soma-workflow configuration file \n")

  config = ConfigParser.ConfigParser()
  print "configuration file " + config_path
  config.read(config_path)
  section = sys.argv[1]
 
  ###########
  # log file 
  log_file_path = config.get(section, constants.OCFG_SERVER_LOG_FILE)
  if log_file_path != 'None':  
    logging.basicConfig(
      filename = log_file_path,
      format = config.get(section, constants.OCFG_SERVER_LOG_FORMAT, 1),
      level = eval("logging."+ config.get(section, 
                                          constants.OCFG_SERVER_LOG_LEVEL)))
  
  ########################
  # Pyro server creation 
  Pyro.core.initServer()
  daemon = Pyro.core.Daemon()
  # locate the NS 
  locator = Pyro.naming.NameServerLocator()
  print 'searching for Name Server...'
  name_server_host = config.get(section, constants.CFG_NAME_SERVER_HOST)
  if name_server_host == 'None':
    ns = locator.getNS()
  else: 
    ns = locator.getNS(host= name_server_host )
  daemon.useNameServer(ns)

  # connect a new object implementation (first unregister previous one)
  server_name = config.get(section, constants.CFG_SERVER_NAME)
  try:
    ns.unregister(server_name)
  except NamingError:
    pass

  # connect new object implementation
  server = WorkflowDatabaseServer(config.get(section, 
                                             constants.CFG_DATABASE_FILE), 
                                  config.get(section, 
                                             constants.CFG_TRANSFERED_FILES_DIR))
  daemon.connect(server, server_name)
  print "port = " + repr(daemon.port)
  
  # enter the server loop.
  print 'Server object ' + server_name + ' ready.'

  ########################
  # Request loop
  daemon.requestLoop()