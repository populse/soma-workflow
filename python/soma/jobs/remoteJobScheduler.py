'''
@author: Soizic Laguitton
@organization: U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''

import pexpect
import Pyro.naming, Pyro.core
from Pyro.errors import NamingError
import time
import socket

__docformat__ = "epytext en"

'''
The remoteJobScheduler module makes it possible to submit jobs from a machine which
is not a submitting machine of the pool and doesn't share a file system with these 
machines. The fonction L{getJobScheduler} gets back a proxy of a L{JobScheduler}  
object. 
The connection between the remote machine and the pool is done via ssh using port 
forwarding (tunneling).
The protocol used inside the tunnel is Pyro's protocol.

requirements: Pyro must be installed on the remote machine.
'''


global pyro_ns_tunnel_child, pyro_server_child, pyro_object_tunnel_child

def getJobScheduler(login, 
                    password, 
                    server_address="is143016",
                    pyro_ns_address="is143016",
                    server_pyro_ns_port = 9090, 
                    client_pyro_ns_port = 8080,
                    src_server_path = "/neurospin/tmp/Soizic/jobFiles/srcServers"):
  '''
  Returns a L(JobScheduler) proxy which interface can be used to submit jobs to 
  the pool.
  
  @type  login: string
  @param login: user's login on the pool 
  @type  password: string
  @param password: associted password
  @type  server_address: string
  @param server_address: address of a submitting machine of the pool.
  @type  pyro_ns_address: string
  @param pyro_ns_address: address of the Pyro name server machine
  @type  server_pyro_ns_port: int
  @param server_pyro_ns_port: port of the pyro name server. It depends of
  the configuration of Pyro and is equal to the configuration item: PYRO_NS_PORT
  @type  client_pyro_ns_port: int 
  @param client_pyro_ns_port: value of the Pyro configuration item PYRO_NS_PORT on
  the remote machine (from which the user wants to submit jobs).
  @type  src_server_path: string
  @param src_server_path: path to the directory containing the souce code for the 
  PyroJobScheduler module. 

  @rtype  : Pyro proxy to a L{JobScheduler} object
  @return : proxy to a JobScheduler object.
  '''
  
  global pyro_ns_tunnel_child, pyro_server_child, pyro_object_tunnel_child

  def createTunnel(port, host, hostport, login, server_address, password):
    command = "ssh -N -L %s:%s:%s %s@%s" %(port, host, hostport, login, server_address)
    print command
    child = pexpect.spawn(command) 
    child.expect('.ssword:*')
    child.sendline(password)
    time.sleep(2)
    return child

  def searchAvailablePort():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # Create a TCP socket
    s.bind(('localhost',0)) #try to bind to the port 0 so that the system will find an available port
    available_port = s.getsockname()[1]
    s.close()
    return available_port 

  ########################################
  # my data                              #
  ########################################
  
  _login = login
  _password = password

  pyro_objet_name = "jobScheduler_" + _login

  #################################################
  # run jobScheduler server and get back its port #
  #################################################

  command = "ssh %s@%s python %s/PyroJobScheduler.py %s" %(_login, 
                                                           server_address, 
                                                           src_server_path, 
                                                           pyro_objet_name) 
  print command
  pyro_server_child = pexpect.spawn(command)
  pyro_server_child.expect('.ssword:*')
  pyro_server_child.sendline(password)
  pyro_server_child.expect(pyro_objet_name + " URI: ")
  server_pyro_object_uri = pyro_server_child.readline()

  server_pyro_object_port = Pyro.core.processStringURI(server_pyro_object_uri).port
  print "Pyro object port: " + repr(server_pyro_object_port)
 

  #######################################
  # find an available port              #
  #######################################

  client_pyro_object_port = searchAvailablePort()
  print "client pyro object port: " + repr(client_pyro_object_port)

  
  ########################################
  # port forwarding for the pyro objet   #
  ########################################

  pyro_object_tunnel_child = createTunnel(client_pyro_object_port, 
                                          server_address, 
                                          server_pyro_object_port, 
                                          _login, 
                                          server_address, 
                                          _password)
  
 
  #######################################
  # get the proxy                       #
  #######################################

  # create a proxy for the Pyro object, and return that
  jobScheduler = Pyro.core.getProxyForURI(server_pyro_object_uri)#.getProxyForURI(URI)
  
 
  ########################################
  # setting the proxy to use the tunnel  #
  ########################################

  jobScheduler.URI.port = client_pyro_object_port
  jobScheduler.URI.address = 'localhost'

  return jobScheduler



