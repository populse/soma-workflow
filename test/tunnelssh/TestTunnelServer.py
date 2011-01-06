import Pyro.naming
import Pyro.core
from Pyro.errors import PyroError, NamingError
import soma.jobs.connection
import sys


import logging

logging.basicConfig(filename = "/home/sl225510/logTunnel", 
		   format = "%(asctime)s : %(message)s",
		   level = logging.DEBUG)

logger = logging.getLogger('tunnel')
logger.debug("TestTunnelServer started !")



class ConnectionChecker(Pyro.core.ObjBase, soma.jobs.connection.ConnectionChecker):
  def __init__(self, interval = 1, controlInterval = 3):
    Pyro.core.ObjBase.__init__(self)
    soma.jobs.connection.ConnectionChecker.__init__(self, interval, controlInterval)
  pass

Pyro.core.initServer()
daemon = Pyro.core.Daemon()

# connection check
connectionChecker = ConnectionChecker()
uri_cc = daemon.connect(connectionChecker, 'connectionChecker')
#sys.stdout.write(" connectionChecker URI: " + str(uri_cc) + "\n")
sys.stdout.write(str(uri_cc) + "\n")
sys.stdout.flush() 


daemon.requestLoop()
 
