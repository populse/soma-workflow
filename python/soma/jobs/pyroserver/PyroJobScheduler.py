import Pyro.core
import soma.jobs.jobScheduler
import sys
import time

###### DrmaaJobs pyro object

class JobScheduler(Pyro.core.ObjBase, soma.jobs.jobScheduler.JobScheduler):
  def __init__(self):
    Pyro.core.ObjBase.__init__(self)
    soma.jobs.jobScheduler.JobScheduler.__init__(self)
  pass

###### main server program

if not len(sys.argv) == 2 :
  sys.stdout.write("PyroJobScheduler takes 1 argument: name of the object. \n")
  sys.exit()
  
object_name = sys.argv[1]

Pyro.core.initServer()
daemon = Pyro.core.Daemon()


#connect new object implementation
uri = daemon.connect(JobScheduler(),object_name)
sys.stdout.write(object_name+ " URI: " + str(uri) + "\n")
sys.stdout.flush() 
 
# enter the server loop.
print 'Server object ' + object_name + ' is ready.'

#REQUESTLOOP
daemon.requestLoop()
