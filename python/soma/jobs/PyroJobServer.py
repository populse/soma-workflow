import Pyro.naming
import Pyro.core
from Pyro.errors import PyroError,NamingError
import jobServer

###### JobServer pyro object
class JobServer(Pyro.core.ObjBase, jobServer.JobServer):
  def __init__(self):
    Pyro.core.ObjBase.__init__(self)
    jobServer.JobServer.__init__(self)
  pass

###### main server program
def main():
  Pyro.core.initServer()
  daemon = Pyro.core.Daemon()
  # locate the NS
  locator = Pyro.naming.NameServerLocator()
  print 'searching for Name Server...'
  ns = locator.getNS()
  daemon.useNameServer(ns)

  #######################JOBSESSION#################################################
  # connect a new object implementation (first unregister previous one)
  try:
    ns.unregister('JobServer')
  except NamingError:
    pass

  # connect new object implementation
  daemon.connect(JobServer(),'JobServer')
  
  # enter the server loop.
  print 'Server object "JobServer" ready.'

  #######################REQUESTLOOP################################################# 
  daemon.requestLoop()

if __name__=="__main__":
        main()