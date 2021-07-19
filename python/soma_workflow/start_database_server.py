# -*- coding: utf-8 -*-

'''
@author: Soizic Laguitton

@organization: I2BM, Neurospin, Gif-sur-Yvette, France
@organization: CATI, France
@organization: U{IFR 49<http://www.ifr49.org>}

@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''

from __future__ import print_function, with_statement
from __future__ import absolute_import
import os
import signal
import sys
import logging
import json
import socket
import soma_workflow.zro as zro
# import soma_workflow.sro as zro

if __name__ == '__main__':

    import soma_workflow.database_server
    from soma_workflow.configuration import Configuration
    from soma_workflow.errors import EngineError

    class WorkflowDatabaseServer(soma_workflow.database_server.WorkflowDatabaseServer):

        def __init__(self,
                     database_file,
                     tmp_file_dir_path,
                     shared_tmp_dir=None,
                     logging_configuration=None,
                     remove_orphan_files=True):
            soma_workflow.database_server.WorkflowDatabaseServer.__init__(
                self,
                database_file,
                tmp_file_dir_path,
                shared_tmp_dir,
                logging_configuration,
                remove_orphan_files)

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

    DEBUG_LOGGING = False
    if DEBUG_LOGGING:
        print("logging configuration: ", server_log_file,
              server_log_format, server_log_level,
              file=open('/tmp/db_server_dbg', 'a'))

    if server_log_file:
        logging.basicConfig(
            filename=server_log_file,
            format=server_log_format,
            level=eval("logging." + server_log_level))
    logger = logging.getLogger('database.ObjectServer')

    obj_serv = zro.ObjectServer()

    logging.info("Launching the database process")

    server_name = config.get_server_name()

    logging.debug("Server name is: " + server_name)

    server = WorkflowDatabaseServer(config.get_database_file(),
                                    config.get_transfered_file_dir(),
                                    config.get_shared_temporary_directory(),
                                    config.get_server_log_info(),
                                    config.get_remove_orphan_files())

    logging.debug("The server has been instantiated ")

    server_uri = obj_serv.register(server)
    logging.debug("server_uri: " + str(server_uri))
    # Write the uri into a file
    (dir, file) = os.path.split(server_log_file)
    file_path = os.path.join(dir, "database_server_uri.py.txt")
    logging.debug(file_path)

    logging.info('SUCCESS: Server object ' + server_name + ' ready.')

    # There could be a problem if multiple servers are running:
    # closing one will remove the reference of the object server
    # even if we are not closing the one holding this object server.
    # Nonetheless, there should not be multiple servers running.

    def handler(signum, frame):
        with open(file_path, "w") as f:
            print('empty file: ' + file_path)
            f.write("")  # empty file

    # signal.signal(signal.SIGTERM, handler)

    # TODO for some reason there is a problem if we handle this signal:
    # the workflow engine does not start normally
    # signal.signal(signal.SIGKILL, handler)
    # signal.signal(signal.SIGINT, handler)

    uri_dict = {
        'hostname': socket.gethostname(),
        'pid': os.getpid(),
        'server_uri': str(server_uri),
        'python_version': '%d.%d.%d' % sys.version_info[:3]
    }
    with open(file_path, 'w') as f:
        json.dump(uri_dict, f)

    logging.info("Writting the uri of the database server.")
    sys.stdout.write(str(server_name) + ": " + str(server_uri) + '\n')
    sys.stdout.flush()

    # Enter the server loop.
    try:
        obj_serv.serve_forever()
    except Exception as e:
        logging.exception(e)
