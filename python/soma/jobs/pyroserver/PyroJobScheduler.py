import Pyro.naming
import Pyro.core
from Pyro.errors import PyroError,NamingError
import soma.jobs.jobScheduler
import sys

###### DrmaaJobs pyro object

class JobScheduler(Pyro.core.ObjBase, soma.jobs.jobScheduler.JobScheduler):
  def __init__(self):
    Pyro.core.ObjBase.__init__(self)
    soma.jobs.jobScheduler.JobScheduler.__init__(self)
  pass

###### main server program

if not len(sys.argv) == 3 :
  sys.stdout.write("PyroJobScheduler takes 2 arguments: \n")
  sys.stdout.write("    1. The name of the object on the pyro name server. \n")
  sys.stdout.write("    2. The address of the name server. \n")
  sys.exit()
  
object_name = sys.argv[1]
ns_address = sys.argv[2]


Pyro.core.initServer()
daemon = Pyro.core.Daemon()
  
# locate the NS
locator = Pyro.naming.NameServerLocator()
print 'searching for Name Server...'
ns = locator.getNS(host=ns_address)

daemon.useNameServer(ns)

# connect a new object implementation (first unregister previous one)
try:
  ns.unregister(object_name)
except NamingError:
  pass

#connect new object implementation
daemon.connect(JobScheduler(),object_name)
 
# enter the server loop.
print 'Server object ' + object_name + ' is ready.'

#REQUESTLOOP
daemon.requestLoop()
