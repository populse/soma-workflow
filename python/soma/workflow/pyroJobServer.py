import soma.jobs.jobServer
import Pyro.naming
import Pyro.core
from Pyro.errors import PyroError, NamingError
import os
import logging
import soma.jobs.constants as constants

class JobServer(Pyro.core.ObjBase, soma.jobs.jobServer.JobServer):
  def __init__(self, database_file, tmp_file_dir_path):
    Pyro.core.ObjBase.__init__(self)
    soma.jobs.jobServer.JobServer.__init__(self, database_file, tmp_file_dir_path)
  pass


if __name__ == '__main__':
  import Pyro.naming
  import Pyro.core
  from Pyro.errors import PyroError,NamingError
  import ConfigParser
  import sys

  if not len(sys.argv) == 2:
    sys.stdout.write("jobServer takes 1 argument: resource id. \n")
    sys.exit(1)
  
  print "Ressource: " + sys.argv[1]

  #########################
  # reading configuration 
  config = ConfigParser.ConfigParser()
  config_file_path = os.environ['SOMA_JOBS_CONFIG']
  config.read(config_file_path)
  section = sys.argv[1]
 
  ###########
  # log file 
  log_file_path = config.get(section, constants.OCFG_JOB_SERVER_LOG_FILE)
  if log_file_path != 'None':  
    logging.basicConfig(
      filename = log_file_path,
      format = config.get(section, constants.OCFG_JOB_SERVER_LOG_FORMAT, 1),
      level = eval("logging."+config.get(section, constants.OCFG_JOB_SERVER_LOG_LEVEL)))
  
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
  job_server_name = config.get(section, constants.CFG_JOB_SERVER_NAME)
  try:
    ns.unregister(job_server_name)
  except NamingError:
    pass

  # connect new object implementation
  jobServer = JobServer(config.get(section, constants.CFG_DATABASE_FILE) , 
                        config.get(section, constants.CFG_TMP_FILE_DIR_PATH) )
  daemon.connect(jobServer,job_server_name)
  print "port = " + repr(daemon.port)
  
  # enter the server loop.
  print 'Server object ' + job_server_name + ' ready.'

  ########################
  # Request loop
  daemon.requestLoop()