
import paramiko
import socket
import soma.jobs.connection
import time
import Pyro.naming, Pyro.core
from Pyro.errors import NamingError
import sys
import threading

import select
import SocketServer
import logging

g_verbose = True

class Tunnel(threading.Thread):
  class ForwardServer (SocketServer.ThreadingTCPServer):
      daemon_threads = True
      allow_reuse_address = True
      
  class Handler (SocketServer.BaseRequestHandler):
  
      def handle(self):
          try:
              chan = self.ssh_transport.open_channel('direct-tcpip',
                                                    (self.chain_host, self.chain_port),
                                                    self.request.getpeername())
          except Exception, e:
              verbose('Incoming request to %s:%d failed: %s' % (self.chain_host,
                                                                self.chain_port,
                                                                repr(e)))
              return
          if chan is None:
              verbose('Incoming request to %s:%d was rejected by the SSH server.' %
                      (self.chain_host, self.chain_port))
              return
  
          verbose('Connected!  Tunnel open %r -> %r -> %r' % (self.request.getpeername(),
                                                              chan.getpeername(), (self.chain_host, self.chain_port)))
          while True:
              r, w, x = select.select([self.request, chan], [], [])
              if self.request in r:
                  data = self.request.recv(1024)
                  if len(data) == 0:
                      break
                  chan.send(data)
              if chan in r:
                  data = chan.recv(1024)
                  if len(data) == 0:
                      break
                  self.request.send(data)
          chan.close()
          self.request.close()
          verbose('Tunnel closed from %r' % (self.request.getpeername(),))

  def __init__(self, port, host, hostport, transport):
    threading.Thread.__init__(self)
    self.__port = port 
    self.__host = host
    self.__hostport = hostport
    self.__transport = transport
    self.setDaemon(True)
    
  def run(self):
    host = self.__host
    hostport = self.__hostport
    transport = self.__transport
    port = self.__port
    class SubHander (Tunnel.Handler):
        chain_host = host
        chain_port = hostport
        ssh_transport = transport
    Tunnel.ForwardServer(('', port), SubHander).serve_forever()


def verbose(s):
    if g_verbose:
        print s
        logging.getLogger('tunnel').debug(s)




def searchAvailablePort():
  s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # Create a TCP socket
  s.bind(('localhost',0)) #try to bind to the port 0 so that the system will find an available port
  available_port = s.getsockname()[1]
  s.close()
  return available_port 



if __name__=="__main__":
  if len(sys.argv) != 3:
    sys.stdout.write("Argument 1 : Connection checker Pyro uri \n")
    sys.stdout.write("Argument 2 : password \n")
    sys.exit(1)
   
  connection_checker_uri = sys.argv[1]
  hostport = Pyro.core.processStringURI(connection_checker_uri).port
  print "Pyro object port: " + repr(hostport)
  
  host = 'gabriel.intra.cea.fr'
  login = 'sl225510'
  password = sys.argv[2]

  logging.basicConfig(
        filename = "/home/sl225510/tunnellog",
        format = "%(asctime)s %(threadName)s: %(message)s",
        level = logging.DEBUG)


  # find an available port              #
  port = searchAvailablePort()
  print "client pyro object port: " + repr(port)
  
  verbose('Connecting to ssh host %s:%d ...' % (host, 22))
  
  transport = paramiko.Transport((host, 22))
  transport.connect(username = login, password = password)
  
  #transport = paramiko.Transport(("is143016", 22))
  #transport.connect(username="sl225510",
  #                   password="Curly!Cat4")
  
  verbose('Now forwarding port %d to %s:%d ...' % (port, host, hostport))
  
  tunnel = Tunnel(port, 
                  host, 
                  hostport,
                  transport)
  tunnel.start()
  
  print "tunnel created! "
  connection_checker = Pyro.core.getAttrProxyForURI(connection_checker_uri)
  
  # setting the proxies to use the tunnel  #
  connection_checker.URI.port = port
  connection_checker.URI.address = 'localhost'
  
  connection_holder = soma.jobs.connection.ConnectionHolder(connection_checker)
  connection_holder.start()
  
  time.sleep(10)
  print 'Port forwarding stopped.'
  sys.exit(0)
