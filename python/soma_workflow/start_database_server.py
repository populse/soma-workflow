#!/usr/bin/env python

'''
@author: Soizic Laguitton

@organization: I2BM, Neurospin, Gif-sur-Yvette, France
@organization: CATI, France
@organization: U{IFR 49<http://www.ifr49.org>}

@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''




if __name__ == '__main__':
 
  import sys
  import logging

  import Pyro.naming
  import Pyro.core
  from Pyro.errors import PyroError, NamingError
  
  import soma.workflow.database_server
  from soma.workflow.configuration import Configuration
  
  class WorkflowDatabaseServer(Pyro.core.ObjBase, 
                               soma.workflow.database_server.WorkflowDatabaseServer):
    def __init__(self, 
                 database_file, 
                 tmp_file_dir_path):
      Pyro.core.ObjBase.__init__(self)
      soma.workflow.database_server.WorkflowDatabaseServer.__init__(self, 
                                                           database_file, 
                                                           tmp_file_dir_path)
    pass

    def test(self):
      return True
  
  if not len(sys.argv) == 2:
    sys.stdout.write("start_database_server takes 1 argument: resource id. \n")
    sys.exit(1)
  
  ressource_id = sys.argv[1]
  print "Ressource: " + ressource_id

  config = Configuration.load_from_file(ressource_id)

  (server_log_file,
   server_log_format,
   server_log_level) = config.get_server_log_info()

  if server_log_file:
    logging.basicConfig(
      filename = server_log_file,
      format = server_log_format,
      level = eval("logging."+ server_log_level))

  
  ########################
  # Pyro server creation 
  Pyro.core.initServer()
  daemon = Pyro.core.Daemon()
  # locate the NS 
  locator = Pyro.naming.NameServerLocator()
  print 'searching for Name Server...'
  name_server_host = config.get_name_server_host()
  if name_server_host == 'None':
    ns = locator.getNS()
  else: 
    ns = locator.getNS(host=name_server_host )
  daemon.useNameServer(ns)

  # connect a new object implementation (first unregister previous one)
  server_name = config.get_server_name()
  try:
    ns.unregister(server_name)
  except NamingError:
    pass

  # connect new object implementation
  server = WorkflowDatabaseServer(config.get_database_file(), 
                                  config.get_transfered_file_dir())
  daemon.connect(server, server_name)
  print "port = " + repr(daemon.port)
  
  # enter the server loop.
  print 'Server object ' + server_name + ' ready.'

  ########################
  # Request loop
  daemon.requestLoop()
