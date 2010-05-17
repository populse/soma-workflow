
import Pyro.naming
import Pyro.core
from Pyro.errors import PyroError, NamingError
import soma.jobs.jobScheduler
import soma.jobs.connection 
from soma.jobs.jobServer import JobServer # for constants only
import sys
import threading
import time 
import logging

import os
  
        
###### JobScheduler pyro object

class JobScheduler(Pyro.core.ObjBase, soma.jobs.jobScheduler.JobScheduler):
  def __init__(self, job_server, drmaa_job_scheduler):
    Pyro.core.ObjBase.__init__(self)
    soma.jobs.jobScheduler.JobScheduler.__init__(self, job_server, drmaa_job_scheduler)
  pass
  
class ConnectionChecker(Pyro.core.ObjBase, soma.jobs.connection.ConnectionChecker):
  def __init__(self, interval = 1, controlInterval = 3):
    Pyro.core.ObjBase.__init__(self)
    soma.jobs.connection.ConnectionChecker.__init__(self, interval, controlInterval)
  pass

###### main server program

def main(jobScheduler_name, log = ""):
  
  import ConfigParser

  #########################
  # reading configuration 
  config = ConfigParser.ConfigParser()
  config_file_path = os.environ['SOMA_JOBS_CONFIG']
  config.read(config_file_path)
  section = 'neurospin_test_cluster'
  #section = 'soizic_home_cluster'  
  
  
  ###########
  # log file 
  if not config.get(section, 'job_processes_log_dir_path') == 'None':
    logfilepath =  config.get(section, 'job_processes_log_dir_path')+ "log_"+jobScheduler_name+log#+time.strftime("_%d_%b_%I:%M:%S", time.gmtime())
    logging.basicConfig(
      filename = logfilepath,
      format = config.get(section, 'job_processes_logging_format', 1),
      level = eval("logging."+config.get(section, 'job_processes_logging_level')))
  
  logger = logging.getLogger('ljp')
  logger.info(" ")
  logger.info("****************************************************")
  logger.info("****************************************************")

  ###########################
  # looking for the JobServer
  Pyro.core.initClient()
  locator = Pyro.naming.NameServerLocator()
  name_server_host = config.get(section, 'name_server_host')
  if name_server_host == 'None':
    ns = locator.getNS()
  else: 
    ns = locator.getNS(host= name_server_host )

  job_server_name = config.get(section, 'job_server_name')
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
  for drmaa_job_attribute in ["drmaa_job_category", "drmaa_native_specification"]:
    if config.has_option(section, drmaa_job_attribute):
      parallel_job_submission_info[drmaa_job_attribute] = config.get(section, drmaa_job_attribute)

  for parallel_config in JobServer.PARALLEL_CONFIGURATIONS:
    if config.has_option(section, parallel_config):
      parallel_job_submission_info[parallel_config] = config.get(section, parallel_config)
  
  #############################

  Pyro.core.initServer()
  daemon = Pyro.core.Daemon()
  
  # instance of drmaaJobScheduler
  drmaaJobScheduler = soma.jobs.jobScheduler.DrmaaJobScheduler(jobServer, parallel_job_submission_info)
  
  # instance of jobScheduler
  jobScheduler = JobScheduler(jobServer, drmaaJobScheduler)
  jsc_lock = threading.Lock()
  
  # connection to the pyro daemon and output its URI 
  uri_jsc = daemon.connect(jobScheduler,jobScheduler_name)
  sys.stdout.write(jobScheduler_name+ " URI: " + str(uri_jsc) + "\n")
  sys.stdout.flush() 
 
  logger.info('Server object ' + jobScheduler_name + ' is ready.')
  
  # connection check
  connectionChecker = ConnectionChecker()
  uri_cc = daemon.connect(connectionChecker, 'connectionChecker')
  sys.stdout.write(jobScheduler_name+ " connectionChecker URI: " + str(uri_cc) + "\n")
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
  if not len(sys.argv) == 2 and not len(sys.argv) == 3:
    sys.stdout.write("PyroJobScheduler takes 1 argument: name of the JobScheduler object. \n")
  else:  
    if len(sys.argv) == 2:
      jobScheduler_name = sys.argv[1]
      main(jobScheduler_name)
    if len(sys.argv) == 3:
      jobScheduler_name = sys.argv[1]
      main(jobScheduler_name, sys.argv[2])