
import Pyro.naming
import Pyro.core
from Pyro.errors import PyroError,NamingError
import soma.jobs.jobScheduler
from soma.jobs.drmaaJobScheduler import DrmaaJobScheduler
import soma.jobs.connectionCheck
import sys
import threading
import time 


import os
  
        
###### JobScheduler pyro object

class JobScheduler(Pyro.core.ObjBase, soma.jobs.jobScheduler.JobScheduler):
  def __init__(self, drmaa_job_scheduler):
    Pyro.core.ObjBase.__init__(self)
    soma.jobs.jobScheduler.JobScheduler.__init__(self, drmaa_job_scheduler)
  pass
  
class ConnectionChecker(Pyro.core.ObjBase, soma.jobs.connectionCheck.ConnectionChecker):
  def __init__(self, interval = 1, controlInterval = 3):
    Pyro.core.ObjBase.__init__(self)
    soma.jobs.connectionCheck.ConnectionChecker.__init__(self, interval, controlInterval)
  pass

###### main server program

def main(jobScheduler_name):
  
  Pyro.core.initServer()
  daemon = Pyro.core.Daemon()
  
  # instance of drmaaJobScheduler
  drmaaJobScheduler = DrmaaJobScheduler()
  
  # instance of jobScheduler
  jobScheduler = JobScheduler(drmaaJobScheduler)
  jsc_lock = threading.Lock()
  
  # connection to the pyro daemon and output its URI 
  ## >> for test purpose only:
  #locator = Pyro.naming.NameServerLocator()
  #ns = locator.getNS(host='is143016')
  #daemon.useNameServer(ns)
  #try:
    #ns.unregister(jobScheduler_name)
  #except NamingError:
    #pass
  ## << end for test purpose only
  uri_jsc = daemon.connect(jobScheduler,jobScheduler_name)
  sys.stdout.write(jobScheduler_name+ " URI: " + str(uri_jsc) + "\n")
  sys.stdout.flush() 
  print 'Server object ' + jobScheduler_name + ' is ready.'
  
  
  # connection check
  ## >> for test purpose only:
  #try:
    #ns.unregister('connectionChecker')
  #except NamingError:
    #pass
  ## << end for test purpose only
  connectionChecker = ConnectionChecker()
  uri_cc = daemon.connect(connectionChecker, 'connectionChecker')
  sys.stdout.write(jobScheduler_name+ " connectionChecker URI: " + str(uri_cc) + "\n")
  sys.stdout.flush() 
  
  
  # Daemon request loop thread
  print "daemon port = " + repr(daemon.port)
  daemonRequestLoopThread = threading.Thread(name = "daemon.requestLoop", 
                              target = daemon.requestLoop) 

  daemonRequestLoopThread.daemon = True
  daemonRequestLoopThread.start() 

  
  
  
  print "******** before client connection ******************"
  client_connected = False
  timeout = 40
  while not client_connected and timeout > 0:
    client_connected = connectionChecker.isConnected()
    timeout = timeout - 1
    time.sleep(1)
    
  print "******** first mode: client connection *************"
  while client_connected:
    client_connected = connectionChecker.isConnected()
    time.sleep(1)
    
  print "******** client disconnection **********************"
  daemon.shutdown(disconnect=True) #stop the request loop
  daemon.sock.close() # free the port
  del(daemon) 
  del(jobScheduler)
  
  print "******** second mode: waiting for jobs to finish****"
  jobs_running = True
  while jobs_running:
    jobs_running = not drmaaJobScheduler.areJobsDone()
    time.sleep(1)
  
  print "******** jobs are done ! ***************************"
  sys.exit()
  



if __name__=="__main__":
  if not len(sys.argv) == 2 :
    sys.stdout.write("PyroJobScheduler takes 1 argument: name of the JobScheduler object. \n")
  else:  
    jobScheduler_name = sys.argv[1]
    main(jobScheduler_name)