
import Pyro.naming
import Pyro.core
from Pyro.errors import PyroError, NamingError
import soma.jobs.jobScheduler
import soma.jobs.connection 
from soma.jobs.constants import *
import sys
import threading
import time 
import logging

import os
  
        
###### JobScheduler pyro object

#class JobScheduler(Pyro.core.ObjBase, soma.jobs.jobScheduler.JobScheduler):
  #def __init__(self, job_server, drmaa_job_scheduler):
    #Pyro.core.ObjBase.__init__(self)
    #soma.jobs.jobScheduler.JobScheduler.__init__(self, job_server, drmaa_job_scheduler)
  #pass

class JobScheduler(Pyro.core.SynchronizedObjBase, soma.jobs.jobScheduler.JobScheduler):
  def __init__(self, job_server, drmaa_job_scheduler):
    Pyro.core.SynchronizedObjBase.__init__(self)
    soma.jobs.jobScheduler.JobScheduler.__init__(self, job_server, drmaa_job_scheduler)
  pass
  
class ConnectionChecker(Pyro.core.ObjBase, soma.jobs.connection.ConnectionChecker):
  def __init__(self, interval = 1, controlInterval = 3):
    Pyro.core.ObjBase.__init__(self)
    soma.jobs.connection.ConnectionChecker.__init__(self, interval, controlInterval)
  pass

###### main server program

def main(resource_id, jobScheduler_name, log = ""):
  
  import ConfigParser

  #########################
  # reading configuration 
  config = ConfigParser.ConfigParser()
  config_file_path = os.environ['SOMA_JOBS_CONFIG']
  config.read(config_file_path)
  section = resource_id
  
  ###########
  # log file 
  if not config.get(section, OCFG_LOCAL_PROCESSES_LOG_DIR) == 'None':
    logfilepath =  config.get(section, OCFG_LOCAL_PROCESSES_LOG_DIR)+ "log_"+jobScheduler_name+log#+time.strftime("_%d_%b_%I:%M:%S", time.gmtime())
    logging.basicConfig(
      filename = logfilepath,
      format = config.get(section, OCFG_LOCAL_PROCESSES_LOG_FORMAT, 1),
      level = eval("logging."+config.get(section, OCFG_LOCAL_PROCESSES_LOG_LEVEL)))
  
  logger = logging.getLogger('ljp')
  logger.info(" ")
  logger.info("****************************************************")
  logger.info("****************************************************")

  ###########################
  # looking for the JobServer
  Pyro.core.initClient()
  locator = Pyro.naming.NameServerLocator()
  name_server_host = config.get(section, CFG_NAME_SERVER_HOST)
  if name_server_host == 'None':
    ns = locator.getNS()
  else: 
    ns = locator.getNS(host= name_server_host )

  job_server_name = config.get(section, CFG_JOB_SERVER_NAME)
  try:
      URI=ns.resolve(job_server_name)
      logger.info('JobServer URI:'+ repr(URI))
  except NamingError,x:
      logger.critical('Couldn\'t find' + job_server_name + ' nameserver says:',x)
      raise SystemExit
  
  jobServer= Pyro.core.getProxyForURI( URI )
  
  ###########################
  # Parallel job specific information
  parallel_job_submission_info= {}
  for parallel_info in PARALLEL_JOB_ENV + PARALLEL_DRMAA_ATTRIBUTES + PARALLEL_CONFIGURATIONS:
    if config.has_option(section, parallel_info):
      parallel_job_submission_info[parallel_info] = config.get(section, parallel_info)
  #############################

  #Pyro.config.PYRO_MULTITHREADED = 0

  Pyro.core.initServer()
  daemon = Pyro.core.Daemon()
  
  # instance of drmaaJobScheduler
  drmaaJobScheduler = soma.jobs.jobScheduler.DrmaaJobScheduler(jobServer, parallel_job_submission_info)
  
  # instance of jobScheduler
  jobScheduler = JobScheduler(jobServer, drmaaJobScheduler)
  jsc_lock = threading.Lock()
  
  # connection to the pyro daemon and output its URI 
  uri_jsc = daemon.connect(jobScheduler,jobScheduler_name)
  sys.stdout.write(jobScheduler_name+ " " + str(uri_jsc) + "\n")
  sys.stdout.flush()
 
  logger.info('Server object ' + jobScheduler_name + ' is ready.')
  
  # connection check
  connectionChecker = ConnectionChecker()
  uri_cc = daemon.connect(connectionChecker, 'connectionChecker')
  sys.stdout.write("connectionChecker " + str(uri_cc) + "\n")
  sys.stdout.flush() 
  
  # Daemon request loop thread
  logger.info("daemon port = " + repr(daemon.port))
  daemonRequestLoopThread = threading.Thread(name = "daemon.requestLoop", 
                              target = daemon.requestLoop) 

  daemonRequestLoopThread.daemon = True
  daemonRequestLoopThread.start() 

  
  logger.info("******** before client connection ******************")
  client_connected = False
  timeout = 40
  while not client_connected and timeout > 0:
    client_connected = connectionChecker.isConnected()
    timeout = timeout - 1
    time.sleep(1)
    
  logger.info("******** first mode: client connection *************")
  while client_connected:
    client_connected = connectionChecker.isConnected()
    time.sleep(1)
    
  logger.info("******** client disconnection **********************")
  daemon.shutdown(disconnect=True) #stop the request loop
  daemon.sock.close() # free the port
  del(daemon) 
  del(jobScheduler)
  
  logger.info("******** second mode: waiting for jobs to finish****")
  jobs_running = True
  while jobs_running:
    jobs_running = not drmaaJobScheduler.areJobsDone()
    time.sleep(1)
  
  logger.info("******** jobs are done ! ***************************")
  sys.exit()
  

if __name__=="__main__":
  if not len(sys.argv) == 3 and not len(sys.argv) == 4:
    sys.stdout.write("localJobProcess takes 2 arguments:\n")
    sys.stdout.write("   1. resource id \n")
    sys.stdout.write("   2. name of the JobScheduler object. \n")
  else:  
    resource_id = sys.argv[1]
    jobScheduler_name = sys.argv[2]
    if len(sys.argv) == 3:
      main(resource_id, jobScheduler_name)
    if len(sys.argv) == 4:
      main(resource_id, jobScheduler_name, sys.argv[3])
