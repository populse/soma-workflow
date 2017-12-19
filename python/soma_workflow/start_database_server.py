#!/usr/bin/env python

'''
@author: Soizic Laguitton

@organization: I2BM, Neurospin, Gif-sur-Yvette, France
@organization: CATI, France
@organization: U{IFR 49<http://www.ifr49.org>}

@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''

from __future__ import print_function

if __name__ == '__main__':

    import sys
    import logging
    import Pyro4

    import soma_workflow.database_server
    from soma_workflow.configuration import Configuration
    from soma_workflow.errors import EngineError

    @Pyro4.expose
    class WorkflowDatabaseServer(soma_workflow.database_server.WorkflowDatabaseServer):

        def __init__(self,
                     database_file,
                     tmp_file_dir_path,
                     shared_tmp_dir=None,
                     logging_configuration=None):

            soma_workflow.database_server.WorkflowDatabaseServer.__init__(
                self,
                database_file,
                tmp_file_dir_path,
                shared_tmp_dir,
                logging_configuration)

    if not len(sys.argv) == 2:
        sys.stdout.write(
            "start_database_server takes 1 argument: resource id. \n")
        sys.exit(1)

    ressource_id = sys.argv[1]

    config = Configuration.load_from_file(ressource_id)
    config.mk_config_dirs()

    (server_log_file,
     server_log_format,
     server_log_level) = config.get_server_log_info()

    if server_log_file:
        logging.basicConfig(
            filename=server_log_file,
            format=server_log_format,
            level=eval("logging." + server_log_level))

    # Pyro server creation
    daemon = Pyro4.Daemon()  # specify hostIP and hostPort

    logging.info("Launching the database process")

    server_name = config.get_server_name()

    logging.debug("Server name is: " + server_name)

    server = WorkflowDatabaseServer(config.get_database_file(),
                                    config.get_transfered_file_dir(),
                                    config.get_shared_temporary_directory(),
                                    config.get_server_log_info())

    server_uri = daemon.register(server)

    #Write the uri into a file
    file_path = os.path.join(server_log_file, "database_server_uri.txt")

    with open(file_path, "w") as f:
        f.write(server_uri)

    logging.info("Writting the uri of the database server.")
    sys.stdout.write(str(server_name) + ": " + str(server_uri) + '\n')
    sys.stdout.flush()

    # enter the server loop.
    logging.info('SUCCESS: Server object ' + server_name + ' ready.')
    #
    # Request loop
    try:
        daemon.requestLoop()
    except:
        with open(file_path, "w") as f:
            f.write("") #empty file
