# -*- coding: utf-8 -*-
from __future__ import with_statement, print_function
from __future__ import absolute_import

'''
@author: Soizic Laguitton

@organization: I2BM, Neurospin, Gif-sur-Yvette, France
@organization: CATI, France
@organization: U{IFR 49<http://www.ifr49.org>}

@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''


#-------------------------------------------------------------------------------
# Imports
#-------------------------------------------------------------------------
import os
import sys
import socket
import six.moves.configparser as configparser

from soma_workflow.errors import ConfigurationError
import soma_workflow.observer as observer
from soma_workflow.info import DB_VERSION

import six
from six.moves import range


#-----------------------------------------------------------------------------
# Globals and constants
#-----------------------------------------------------------------------------

LIGHT_MODE = 'light'
REMOTE_MODE = 'remote'
LOCAL_MODE = 'local'
MODES = [LIGHT_MODE,
         REMOTE_MODE,
         LOCAL_MODE]

LOCAL_SCHEDULER = 'local_basic'
DRMAA_SCHEDULER = 'drmaa'
MPI_SCHEDULER = 'mpi'

# configuration variables ------------------------------------------------

'''
CFG => Mandatory items
OCFG => Optional
'''
CFG_CLUSTER_ADDRESS = 'CLUSTER_ADDRESS'
CFG_SUBMITTING_MACHINES = 'SUBMITTING_MACHINES'
OCFG_DRMAA_IMPLEMENTATION = 'DRMAA_IMPLEMENTATION'
OCFG_SCHEDULER_TYPE = 'SCHEDULER_TYPE'


# OCFG_QUEUES is a list of queue name separated by white spaces.
# ex: "queue1 queue2"
OCFG_QUEUES = 'QUEUES'

OCFG_LOGIN = 'LOGIN'

OCFG_SSHPort = 'SSHPort'
OCFG_INSTALLPATH = 'INSTALLPATH'

# OCFG_MAX_JOB_IN_QUEUE allow to specify a maximum number of job N which can be
# in the queue for one user. The engine won't submit more than N jobs at once.
# Also wait for the job to leave the queue before submitting new jobs.
# syntax: "{default_queue_max_nb_jobs} queue1{max_nb_jobs1}
# queue2{max_nb_job2}"
OCFG_MAX_JOB_IN_QUEUE = 'MAX_JOB_IN_QUEUE'
# OCFG_MAX_JOB_RUNNING allow to specify a maximum number of job N which can be
# running or in the queue for one user. The engine won't submit more than
# N jobs at once.
OCFG_MAX_JOB_RUNNING = 'MAX_JOB_RUNNING'

# Set timeout for jobs. Useful if jobs may run indefinitely
OCFG_MPI_JOB_TIMEOUT = 'MPI_JOB_TIMEOUT'

# database server
CFG_DATABASE_FILE = 'DATABASE_FILE'
CFG_TRANSFERED_FILES_DIR = 'TRANSFERED_FILES_DIR'
CFG_SERVER_NAME = 'SERVER_NAME'

# OCFG_REMOVE_ORPHAN_FILES allow to disable search for orphan files (i.e. files
# that exist in transfered files directory but that are not registered in
# database anymore) at connection time. This mechanism can be really slow if a
# large number of files exist in the transfered files directory.
OCFG_REMOVE_ORPHAN_FILES = 'REMOVE_ORPHAN_FILES'
OCFG_SERVER_LOG_FILE = 'SERVER_LOG_FILE'
OCFG_SERVER_LOG_LEVEL = 'SERVER_LOG_LEVEL'
OCFG_SERVER_LOG_FORMAT = 'SERVER_LOG_FORMAT'

# Engine
OCFG_ENGINE_LOG_DIR = 'ENGINE_LOG_DIR'
OCFG_ENGINE_LOG_LEVEL = 'ENGINE_LOG_LEVEL'
OCFG_ENGINE_LOG_FORMAT = 'ENGINE_LOG_FORMAT'
OCFG_SHARED_TEMPORARY_DIR = 'SHARED_TEMPORARY_DIR'

OCFG_MPI_LOG_FORMAT = 'MPI_LOG_FORMAT'
OCFG_MPI_LOG_DIR = 'MPI_LOG_DIR'

# Shared resource path translation files
# specify the translation files (if any) associated to a namespace
# eg. translation_files =
# brainvisa{/home/toto/.brainvisa/translation.sjtr}
# namespace2{path/translation1.sjtr} namespace2{path/translation2.sjtr}
OCFG_PATH_TRANSLATION_FILES = 'PATH_TRANSLATION_FILES'

# Soma-workflow light mode.
# Define this item to use soma-workflow in the light mode.
# This mode doesn't require a database server to run. It can not be used on
# remote computing resource. The client application can not be closed before the
# workflows and jobs are done.
OCFG_LIGHT_MODE = 'LIGHT_MODE'

# Parallel job configuration :
# DRMAA attributes used in parallel job submission (their value depends on
# the cluster and DRMS)
OCFG_PARALLEL_COMMAND = "drmaa_native_specification"
OCFG_PARALLEL_JOB_CATEGORY = "drmaa_job_category"
PARALLEL_DRMAA_ATTRIBUTES = [OCFG_PARALLEL_COMMAND, OCFG_PARALLEL_JOB_CATEGORY]
# kinds of parallel jobs (items can be added by administrator)
OCFG_PARALLEL_PC_MPI = "MPI"
OCFG_PARALLEL_PC_OPEN_MP = "OpenMP"
OCFG_PARALLEL_PC_NATIVE = "native"
PARALLEL_CONFIGURATIONS = [OCFG_PARALLEL_PC_MPI, OCFG_PARALLEL_PC_OPEN_MP,
                           OCFG_PARALLEL_PC_NATIVE]
# parallel job environment variables for the execution machine (items can
# be added by administrators)
OCFG_PARALLEL_ENV_MPI_BIN = 'PARALLEL_ENV_MPI_BIN'
OCFG_PARALLEL_ENV_NODE_FILE = 'PARALLEL_ENV_NODE_FILE'
PARALLEL_JOB_ENV = [OCFG_PARALLEL_ENV_MPI_BIN, OCFG_PARALLEL_ENV_NODE_FILE]

# Native_specification for all jobs
OCFG_NATIVE_SPECIFICATION = 'NATIVE_SPECIFICATION'

# Container (docker / singularity...) prefix prepended to all commands in jobs
OCFG_CONTAINER_COMMAND = 'CONTAINER_COMMAND'

# Python version filtering
OCFG_ALLOWED_PYTHON_VERSIONS = 'ALLOWED_PYTHON_VERSIONS'
OCFG_PYTHON_COMMAND = 'PYTHON_COMMAND'

# local sheduler configuration -------------------------------------------

OCFG_SCDL_CPU_NB = "CPU_NB"
OCFG_SCDL_MAX_CPU_NB = "MAX_CPU_NB"
OCFG_SCDL_INTERVAL = "SCHEDULER_INTERVAL"
OCFG_SWF_DIR = "SOMA_WORKFLOW_DIR"


#-------------------------------------------------------------------------------
# Classes and functions
#-------------------------------------------------------------------------
class Configuration(observer.Observable):

    # path of the configuration file
    _config_path = None

    # config parser object
    _config_parser = None

    _resource_id = None

    _mode = None

    _scheduler_type = None

    _scheduler_config = None

    _database_file = None

    _transfered_file_dir = None

    _submitting_machines = None

    _cluster_address = None

    _server_name = None

    _queue_limits = None

    _running_jobs_limits = None

    _queues = None

    _drmaa_implementation = None

    _login = None

    _native_specification = None

    _sshport = 22

    _res_install_path = None

    _shared_temporary_dir = None

    _remove_orphan_files = None

    parallel_job_config = None

    path_translation = None

    QUEUE_LIMITS_CHANGED = 0
    RUNNING_JOBS_LIMITS_CHANGED = 1

    def __init__(self,
                 resource_id,
                 mode,
                 scheduler_type,
                 database_file,
                 transfered_file_dir,
                 submitting_machines=None,
                 cluster_address=None,
                 server_name=None,
                 queues=None,
                 queue_limits=None,
                 drmaa_implementation=None,
                 login=None,
                 native_specification=None,
                 sshport=22,
                 res_install_path=None,
                 running_jobs_limits=None,
                 ):
        '''
        * resource_id *string*
          Identifier of the computing resource.

        * mode *string*
          A mode among the existing modes defined in configuration.MODES

        * scheduler_type *string*
          A scheduler type among the existing schedulers defined in the
          schedulers submodules

        * database_file *string*
          Path of the database_file.

        * transfered_file_dir *string*
          Path of the directory where the transfered files are copied. Mandatory
          in every mode (even local or light), the directory must exist.

        * submitting_machines *list of string*
          List of submitting machines. Mandatory in the REMOTE_MODE for the remote
          ssh connection.

        * cluster_address *string*
          Address of the cluster. Mandatory in the REMOTE_MODE for the remote
          ssh connection.

        * server_name *string*
          Name of the database server regitered on the Pyro name server. Mandatory
          in the REMOTE_MODE to connect to the database_server.

        * queues *list of string*
          List of the available queues. This item is only used in the GUI to make
          easier the selection of the queue when submitting a workflow.

        * queue_limits *dictionary: string -> int*
          Maximum number of job in each queue (dictionary: queue name -> limit).
          If a queue does not appear here, soma-workflow considers that there is
          no limitation.

        * drmaa_implementation *string*
          Set this item to "PBS" if you use FedStage PBS DRMAA 1.0 implementation,
          otherwise it does not has to be set.

        * login *string*
          Configure a login to avoid some typing in the GUI.

        * native_specification *string*
          Native specification applied to every jobs submitted to the resource
          unless a different value is specified in the Job attribute
          native_specification.

        * sshport *int*
          ssh port to remote machine.

        * res_install_path *string*
          soma-workflow installation path on the resource server

        * running_jobs_limits *dictionary: string -> int*
          Maximum number of job running in each queue
          (dictionary: queue name -> limit).
          If a queue does not appear here, soma-workflow considers that there
          is no limitation.

        '''

        super(Configuration, self).__init__()

        self._config_path = None
        self._config_parser = None
        self._resource_id = resource_id
        self._mode = mode
        self._scheduler_type = scheduler_type
        self._database_file = database_file
        self._transfered_file_dir = transfered_file_dir
        self._submitting_machines = submitting_machines
        self._cluster_address = cluster_address
        self._server_name = server_name
        self._login = login
        self._native_specification = native_specification
        if queues == None:
            self._queues = []
        else:
            self._queues = queues
        if queue_limits == None:
            self._queue_limits = {}
        else:
            self._queue_limits = queue_limits
        if running_jobs_limits == None:
            self._running_jobs_limits = {}
        else:
            self._running_jobs_limits = running_jobs_limits
        self._queue_limits_disabled = False
        self._drmaa_implementation = drmaa_implementation
        self.parallel_job_config = None
        self.path_translation = None

        self._sshport = sshport
        self._res_install_path = res_install_path
        self._scheduler_config = None

    @staticmethod
    def get_home_dir():
        homedir = os.path.expanduser('~')
        if homedir == '~':
            return ''  # backward-compatible behaviour of get_home_dir()
        return homedir

    @classmethod
    def load_from_file(cls,
                       resource_id=None,
                       config_file_path=None):
        '''
        Load config from a file, and return it

        Parameters
        ----------
        resource_id: string
            computing resource identifier to get specific config for
        config_file_path: string or file object
            filename or file object for config to be read.

        Returns
        -------
        Configuration object
        '''

        if config_file_path:
            config_path = config_file_path
        else:
            config_path = Configuration.search_config_path()

        config = None
        home_dir = Configuration.get_home_dir()

        if resource_id is None:
            resource_id = Configuration.get_local_resource_id(
                config_file_path=config_file_path)

        config_parser = None
        if config_path is not None:
            config_parser = configparser.ConfigParser()
            if hasattr(config_path, 'readline'):
                # tha API of configparser is somewhat moving...
                if hasattr(config_parser, 'read_file'):
                    config_parser.read_file(config_path)
                else:
                    config_parser.readfp(config_path)
            else:
                config_parser.read(config_path)

        if config_path is None \
                or Configuration.is_local_resource(config_parser, resource_id):

            # scheduler local on the local machine
            if resource_id is None:
                resource_id = socket.gethostname()
            mode = LIGHT_MODE
            scheduler_type = LOCAL_SCHEDULER

            swf_dir = os.path.join(home_dir, ".soma-workflow")
            database_file = None

            if config_path is not None:
                if config_parser.has_section(resource_id):
                    if config_parser.has_option(resource_id,
                                                OCFG_SWF_DIR):
                        swf_dir = config_parser.get(resource_id,
                                                    OCFG_SWF_DIR)

                    if config_parser.has_option(resource_id,
                                                CFG_DATABASE_FILE):
                        database_file = config_parser.get(resource_id,
                                                          CFG_DATABASE_FILE)

            if database_file is None:
                database_file = os.path.join(
                    swf_dir, "soma_workflow-%s.db" % DB_VERSION)
            else:
                dot = database_file.rfind('.')
                database_file = database_file[:dot] + ('-%s' % DB_VERSION) \
                    + database_file[dot:]

            config = cls(resource_id=resource_id,
                         mode=mode,
                         scheduler_type=scheduler_type,
                         database_file=database_file,
                         transfered_file_dir=None)

            if config_path is not None \
                    and config_parser.has_section(resource_id):
                config._config_parser = config_parser
                config._config_path = config_path

            try:
                transfered_file_dir = config.get_transfered_file_dir()
            except Exception:
                transfered_file_dir = os.path.join(swf_dir, "transfered_files")
                config._transfered_file_dir = transfered_file_dir

            if not os.path.isdir(swf_dir):
                if os.path.islink(swf_dir):
                    swf_dir = os.readlink(swf_dir)
                try:
                    os.mkdir(swf_dir)
                except OSError:
                    pass  # ignore failed mkdir
            if not os.path.isdir(transfered_file_dir):
                try:
                    os.mkdir(transfered_file_dir)
                except OSError:
                    pass  # ignore failed mkdir

            return config

        else:

            if config_path == None:
                raise ConfigurationError("A configuration file is required to connect "
                                         "to " + repr(resource_id)
                                         + ": the soma-workflow "
                                         "configuration file could not be found.")
            if not config_parser.has_section(resource_id):
                raise ConfigurationError("Can not find section " + repr(resource_id) + " "
                                         "in configuration file: " + config_path)

            scheduler_type = None
            if config_parser.has_option(resource_id, OCFG_SCHEDULER_TYPE):
                scheduler_type = config_parser.get(
                    resource_id, OCFG_SCHEDULER_TYPE)
                from soma_workflow import scheduler
                try:
                    sched_impl \
                        = scheduler.get_scheduler_implementation(
                            scheduler_type)
                except Exception:
                    raise ConfigurationError("Unknown scheduler type:"
                                             " " + repr(scheduler_type))
            else:
                scheduler_type = DRMAA_SCHEDULER

            config = cls(resource_id=resource_id,
                         mode=None,
                         scheduler_type=scheduler_type,
                         database_file=None,
                         transfered_file_dir=None)
            config._config_parser = config_parser
            config._config_path = config_path

            return config

    def get_config_parser(self):
        return self._config_parser

    def get_scheduler_type(self):
        return self._scheduler_type

    def get_soma_workflow_dir(self):
        config_parser = self._config_parser
        if config_parser.has_option(self._resource_id, OCFG_SWF_DIR):
            swf_dir = config_parser.get(self._resource_id, OCFG_SWF_DIR)
        else:
            home_dir = self.get_home_dir()
            swf_dir = os.path.join(home_dir, ".soma-workflow")
        return swf_dir

    def get_res_install_path(self):
        if self._config_parser == None or self._res_install_path:
            return self._res_install_path

        if not self._config_parser.has_option(self._resource_id,
                                              OCFG_INSTALLPATH):
            self._res_install_path = None
        else:
            self._res_install_path = self._config_parser.get(
                self._resource_id, OCFG_INSTALLPATH)

        return self._res_install_path

    def get_ssh_port(self):
        if self._config_parser == None or self._sshport:
            return self._sshport

        if self._config_parser.has_option(self._resource_id,
                                          OCFG_SSHPort):
            self._sshport = self._config_parser.get(
                self._resource_id, OCFG_SSHPort)
        else:
            self._sshport = "22"

        return self._sshport

    def get_submitting_machines(self):
        if self._config_parser == None or self._submitting_machines:
            return self._submitting_machines

        if not self._config_parser.has_option(self._resource_id,
                                              CFG_SUBMITTING_MACHINES):
            raise ConfigurationError("Can not find the configuration item %s for the "
                                     "resource %s, in the configuration file %s." %
                                     (CFG_SUBMITTING_MACHINES,
                                      self._resource_id,
                                      self._config_path))
        submitting_machines = self._config_parser.get(self._resource_id,
                                                      CFG_SUBMITTING_MACHINES).split()
        self._submitting_machines = []
        for sub_machine in submitting_machines:
            self._submitting_machines.append(os.path.expandvars(sub_machine))
        return self._submitting_machines

    @staticmethod
    def search_config_path():
        '''
        returns the path of the soma workflow configuration file
        '''

        home_dir = Configuration.get_home_dir()

        config_path = os.getenv('SOMA_WORKFLOW_CONFIG')
        if not config_path or not os.path.isfile(config_path):
            config_path = os.path.join(home_dir, ".soma-workflow.cfg")
        if not config_path or not os.path.isfile(config_path):
            config_path = os.path.dirname(__file__)
            config_path = os.path.join(config_path, "etc/soma-workflow.cfg")
        if not config_path or not os.path.isfile(config_path):
            config_path = "/etc/soma-workflow.cfg"
        if not config_path or not os.path.isfile(config_path):
            config_path = None

        return config_path

    @staticmethod
    def get_configured_resources(config_file_path=None, filtered=True):
        '''
        returns the list of resource ids

        Parameters
        ----------
        config_file_path: str
            config file path, use default one if not specified
        filtered: bool
            if True, filter out non-matching resources (based on python
            version)
        '''
        resource_ids = []
        if config_file_path == None:
            config_file_path = Configuration.search_config_path()
        config_parser = configparser.ConfigParser()
        if config_file_path:
            config_parser.read(config_file_path)
        for r_id in config_parser.sections():
            resource_ids.append(r_id)
        local_machine = socket.gethostname()
        local_resource_id = local_machine
        new_resource_ids = []
        for resource_id in resource_ids:
            if filtered \
                    and not Configuration.is_python_version_matching(
                        config_parser, resource_id):
                # remove this one
                continue
            new_resource_ids.append(resource_id)

            try:
                machines = config_parser.get(resource_id,
                                             CFG_SUBMITTING_MACHINES)
            except configparser.NoOptionError:
                machines = None

            if (machines is None or local_machine in machines
                or 'localhost' in machines) \
                    and config_parser.has_option(resource_id, OCFG_LIGHT_MODE):
                local_resource_id = resource_id
        if local_resource_id not in new_resource_ids:
            new_resource_ids.append(local_resource_id)
        return new_resource_ids

    @staticmethod
    def is_local_resource(config_parser, resource_id):
        '''
        Tells if the given resource id is a local resource or not.
        Several criterions may apply:
        * if resource_id is "localhost" or is the local machine name
        * if the associated config specifies that the resource is in
          "light mode"
        * if the associated config has none of the options scheduler_type,
          submitting_machines or cluster_address.
        '''
        if resource_id is None \
                or resource_id in ('localhost',
                                   socket.gethostname(),
                                   socket.gethostname().split('.')[0]) \
                or (config_parser.has_option(resource_id, OCFG_LIGHT_MODE)
                    and bool(int(config_parser.get(
                        resource_id, OCFG_LIGHT_MODE)))
                    ) \
                or (not config_parser.has_option(
                    resource_id, OCFG_SCHEDULER_TYPE)
                    and not config_parser.has_option(
                        resource_id, CFG_SUBMITTING_MACHINES)
                    and not config_parser.has_option(
                        resource_id, CFG_CLUSTER_ADDRESS)
                    ):
            return True
        return False

    @staticmethod
    def get_local_resource_id(config=None, config_file_path=None,
                              filtered=True):
        '''
        Find a matching local resource configuration. A local resource
        is identified by the following criterions:
        * it is not filtered out is "filtered" is True
        * the resource id matches the local machine name, or its
          SUBMITTING_MACHINES section contains the local machine name, or
          "localhost"
        * the resource mode is ``LIGHT_MODE``
        '''
        if config_file_path is None:
            config_file_path = Configuration.search_config_path()
        if config is None:
            if config_file_path == None:
                return socket.gethostname()
            config_parser = configparser.ConfigParser()
            config_parser.read(config_file_path)
        else:
            config_parser = config._config_parser
            if config_parser is None:
                if config._config_path is None:
                    return socket.gethostname()
                config_parser = configparser.ConfigParser()
                config_parser.read(config._config_path)
                config._config_parser = config_parser

        local_machine = socket.gethostname()
        for resource_id in config_parser.sections():
            if filtered \
                    and not Configuration.is_python_version_matching(
                        config_parser, resource_id):
                # remove this one
                continue

            if Configuration.is_local_resource(config_parser, resource_id):
                return resource_id

            try:
                machines = config_parser.get(resource_id,
                                             CFG_SUBMITTING_MACHINES)
            except configparser.NoOptionError:
                machines = None

            if (machines is None or local_machine in machines
                or 'localhost' in machines) \
                    and (config_parser.has_option(resource_id, OCFG_LIGHT_MODE)
                         or (not config_parser.has_option(
                             resource_id, CFG_SUBMITTING_MACHINES)
                             and not config_parser.has_option(
                             resource_id, CFG_CLUSTER_ADDRESS))):
                return resource_id
        return None

    @staticmethod
    def get_logins(config_file_path=None):
        '''
        returns the dictionary resource_id -> login
        '''
        resource_ids = []
        if config_file_path == None:
            return {socket.gethostname(): None}
        config_parser = configparser.ConfigParser()
        config_parser.read(config_file_path)
        logins = {}
        for r_id in config_parser.sections():
            resource_ids.append(r_id)
            if config_parser.has_option(r_id, OCFG_LOGIN):
                logins[r_id] = os.path.expandvars(
                    config_parser.get(r_id, OCFG_LOGIN))
            else:
                logins[r_id] = None
        logins[socket.gethostname()] = None
        return logins

    def get_mode(self):
        '''
        Return the application mode: 'local', 'remote' or 'light'
        '''
        if self._mode:
            return self._mode

        if self._config_parser.has_option(self._resource_id, OCFG_LIGHT_MODE):
            self._mode = LIGHT_MODE
            return self._mode

        if not self._config_parser.has_option(self._resource_id,
                                              CFG_SUBMITTING_MACHINES):
            self._mode = LOCAL_MODE
            return self._mode

        if not self._submitting_machines:
            self._submitting_machines = self.get_submitting_machines()

        hostname = socket.gethostname()
        mode = REMOTE_MODE
        for machine in self._submitting_machines:
            if hostname == machine:
                mode = LOCAL_MODE
        self._mode = mode
        return mode

    def get_cluster_address(self):
        if self._config_parser == None or self._cluster_address:
            return self._cluster_address

        if not self._config_parser.has_option(self._resource_id,
                                              CFG_CLUSTER_ADDRESS):
            raise ConfigurationError("Can not find the configuration item %s for the "
                                     "resource %s, in the configuration file %s." %
                                     (CFG_CLUSTER_ADDRESS,
                                      self._resource_id,
                                      self._config_path))
        self._cluster_address = self._config_parser.get(self._resource_id,
                                                        CFG_CLUSTER_ADDRESS)
        return self._cluster_address

    def get_database_file(self):
        if self._database_file:
            return self._database_file

        if not self._config_parser.has_option(self._resource_id, CFG_DATABASE_FILE):
            swf_dir = self.get_soma_workflow_dir()
            self._database_file = os.path.join(
                swf_dir, "soma_workflow-%s.db" % DB_VERSION)
        else:
            database_file = self._config_parser.get(self._resource_id,
                                                    CFG_DATABASE_FILE)
            # append db version before extension ("soma_workflow-<version>.db")
            db_file_parts = database_file.split('.')
            self._database_file = '.'.join(
                db_file_parts[:-1]) + '-%s' % DB_VERSION
            if len(db_file_parts) >= 2:
                self._database_file += '.' + db_file_parts[-1]
        self._database_file = os.path.expandvars(self._database_file)
        return self._database_file

    def get_transfered_file_dir(self):
        if self._transfered_file_dir:
            return self._transfered_file_dir

        if not self._config_parser.has_option(self._resource_id,
                                              CFG_TRANSFERED_FILES_DIR):
            swf_dir = self.get_soma_workflow_dir()
            self._transfered_file_dir = os.path.join(
                swf_dir, 'transfered_files')
            # raise ConfigurationError("Can not find the configuration item %s "
                                     #"for the resource %s, in the configuration " "file %s." %
                                    #(CFG_TRANSFERED_FILES_DIR,
                                        # self._resource_id,
                                        # self._config_path))
        else:
            self._transfered_file_dir = self._config_parser.get(
                self._resource_id, CFG_TRANSFERED_FILES_DIR)
            self._transfered_file_dir = os.path.expandvars(
                self._transfered_file_dir)
        return self._transfered_file_dir

    def get_shared_temporary_directory(self):
        '''config for a directory where temporary files can be generated and used from every node of the resource'''
        if self._shared_temporary_dir:
            return self._shared_temporary_dir

        if self._config_parser is None or \
            not self._config_parser.has_option(self._resource_id,
                                               OCFG_SHARED_TEMPORARY_DIR):
            return None
        self._shared_temporary_dir = self._config_parser.get(self._resource_id,
                                                             OCFG_SHARED_TEMPORARY_DIR)
        if not self._shared_temporary_dir:
            # fallback to transfered files dir
            self._shared_temporary_dir = self.get_transfered_file_dir()
        self._shared_temporary_dir = os.path.expandvars(
            self._shared_temporary_dir)
        return self._shared_temporary_dir

    def get_remove_orphan_files(self):
        '''config that manages orphan files removal at connection time'''
        if self._remove_orphan_files is not None:
            return self._remove_orphan_files

        if self._config_parser is None or \
            not self._config_parser.has_option(self._resource_id,
                                               OCFG_REMOVE_ORPHAN_FILES):
            return True

        self._remove_orphan_files = self._config_parser.get(self._resource_id,
                                                            OCFG_REMOVE_ORPHAN_FILES)
        self._remove_orphan_files = bool(int(os.path.expandvars(
            self._remove_orphan_files)))

        return self._remove_orphan_files

    def get_parallel_job_config(self):
        if self._config_parser == None or self.parallel_job_config != None:
            return self.parallel_job_config

        self.parallel_job_config = {}
        for parallel_config_info in PARALLEL_DRMAA_ATTRIBUTES + \
            PARALLEL_JOB_ENV + \
                PARALLEL_CONFIGURATIONS:
            if self._config_parser.has_option(self._resource_id,
                                              parallel_config_info):
                self.parallel_job_config[parallel_config_info] = \
                    self._config_parser.get(self._resource_id,
                                            parallel_config_info)

        return self.parallel_job_config

    def get_drmaa_implementation(self):
        if self._config_parser == None or self._drmaa_implementation != None:
            return self._drmaa_implementation
        self._drmaa_implementation = None
        if self._config_parser != None and \
           self._config_parser.has_option(self._resource_id,
                                          OCFG_DRMAA_IMPLEMENTATION):
            self._drmaa_implementation = self._config_parser.get(
                self._resource_id,
                OCFG_DRMAA_IMPLEMENTATION)
        return self._drmaa_implementation

    def get_login(self):
        if self._config_parser == None or self._login != None:
            return self._login
        self._login = None
        if self._config_parser != None:
            try:
                self._login = os.path.expandvars(
                    self._config_parser.get(self._resource_id,
                                            OCFG_LOGIN))
            except configparser.NoOptionError:
                return None
        return self._login

    def get_native_specification(self):
        if self._config_parser == None or self._native_specification != None:
            return self._native_specification
        self._native_specification = None
        if self._config_parser != None and \
           self._config_parser.has_option(self._resource_id,
                                          OCFG_NATIVE_SPECIFICATION):
            self._native_specification = self._config_parser.get(
                self._resource_id,
                OCFG_NATIVE_SPECIFICATION)
        return self._native_specification

    def get_path_translation(self):
        if self._config_parser == None or self.path_translation != None:
            return self.path_translation

        self.path_translation = {}
        if self._config_parser.has_option(self._resource_id,
                                          OCFG_PATH_TRANSLATION_FILES):
            translation_files_str = self._config_parser.get(self._resource_id,
                                                            OCFG_PATH_TRANSLATION_FILES)
            # logger.info("Path translation files configured:")
            for ns_file_str in translation_files_str.split():
                ns_file = ns_file_str.split("{")
                namespace = ns_file[0]
                filename = ns_file[1].rstrip("}")
                filename = os.path.expandvars(filename)
                # logger.info(" -namespace: " + namespace + ", translation
                # file: " + filename)
                try:
                    f = open(filename, "r")
                except IOError as e:
                    print("Can not read the translation file %s" % (filename),
                          file=sys.stderr)
                    continue

                if not namespace in self.path_translation.keys():
                    self.path_translation[namespace] = {}
                line = f.readline()
                while line:
                    splitted_line = line.split(None, 1)
                    if len(splitted_line) > 1:
                        uuid = splitted_line[0]
                        content = splitted_line[1].rstrip()
                        # logger.info("    uuid: " + uuid + "   translation:" +
                        # content)
                        self.path_translation[namespace][
                            uuid] = os.path.expandvars(content)
                    line = f.readline()
                f.close()

        return self.path_translation

    def get_server_name(self):
        if self._config_parser == None or self._server_name != None:
            return self._server_name
        if not self._config_parser.has_option(self._resource_id,
                                              CFG_SERVER_NAME):
            raise ConfigurationError("Can not find the configuration item %s "
                                     "for the resource %s, in the configuration " "file %s." %
                                     (CFG_SERVER_NAME,
                                      self._resource_id,
                                      self._config_path))
        self._server_name = self._config_parser.get(self._resource_id,
                                                    CFG_SERVER_NAME)
        return self._server_name

    def change_queue_limits(self, queue_name, queue_limit):
        '''
        * queue_name *string*

        * queue_limit *int*
        '''
        self.get_queue_limits()
        if queue_limit == 0:  # unlimited
            if queue_name in self._queue_limits:
                del self._queue_limits[queue_name]
        else:
            self._queue_limits[queue_name] = queue_limit
        self.notifyObservers(Configuration.QUEUE_LIMITS_CHANGED)

    def change_running_jobs_limits(self, queue_name, running_jobs_limit):
        '''
        * queue_name *string*

        * running_jobs_limit *int*
        '''
        self.get_running_jobs_limits()
        if running_jobs_limit == 0:  # unlimited
            if queue_name in self._running_jobs_limits:
                del self._running_jobs_limits[queue_name]
        else:
            self._running_jobs_limits[queue_name] = running_jobs_limit
        self.notifyObservers(Configuration.RUNNING_JOBS_LIMITS_CHANGED)

    def disable_queue_limits(self):
        self._queue_limits_disabled = True

    def get_queue_limits(self):
        if self._queue_limits_disabled:
            return {}

        if self._config_parser == None or len(self._queue_limits) != 0:
            return self._queue_limits

        self._queue_limits = {}
        if self._config_parser.has_option(self._resource_id,
                                          OCFG_MAX_JOB_IN_QUEUE):
            queue_limits_str = self._config_parser.get(self._resource_id,
                                                       OCFG_MAX_JOB_IN_QUEUE)
            for info_str in queue_limits_str.split():
                info = info_str.split("{")
                if len(info[0]) == 0:
                    queue_name = None
                else:
                    queue_name = info[0]
                max_job = int(info[1].rstrip("}"))
                self._queue_limits[queue_name] = max_job

        return self._queue_limits

    def get_running_jobs_limits(self):
        if self._config_parser == None or len(self._running_jobs_limits) != 0:
            return self._running_jobs_limits

        self._running_jobs_limits = {}
        if self._config_parser.has_option(self._resource_id,
                                          OCFG_MAX_JOB_RUNNING):
            running_jobs_limits_str = self._config_parser.get(
                self._resource_id, OCFG_MAX_JOB_RUNNING)
            for info_str in running_jobs_limits_str.split():
                info = info_str.split("{")
                if len(info[0]) == 0:
                    queue_name = None
                else:
                    queue_name = info[0]
                max_job = int(info[1].rstrip("}"))
                self._running_jobs_limits[queue_name] = max_job

        return self._running_jobs_limits

    def get_queues(self):
        if self._config_parser == None or len(self._queues) != 0:
            return self._queues

        queues = set()
        if self._config_parser.has_option(self._resource_id, OCFG_QUEUES):
            queues.update(self._config_parser.get(self._resource_id,
                                                  OCFG_QUEUES).split())
        if self._config_parser.has_option(self._resource_id,
                                          OCFG_MAX_JOB_IN_QUEUE):
            queues.update(self.get_queue_limits().keys())
        if self._config_parser.has_option(self._resource_id,
                                          OCFG_MAX_JOB_RUNNING):
            queues.update(self.get_running_jobs_limits().keys())
        self._queues = list(queues)
        return self._queues

    def get_engine_log_info(self):
        if self._config_parser != None:
            if self._config_parser.has_option(self._resource_id,
                                              OCFG_ENGINE_LOG_DIR):
                engine_log_dir = self._config_parser.get(self._resource_id,
                                                        OCFG_ENGINE_LOG_DIR)
                engine_log_dir = os.path.expandvars(engine_log_dir)
            else:
                engine_log_dir = os.path.join(self.get_soma_workflow_dir(),
                                              'logs')
            if self._config_parser.has_option(self._resource_id,
                                              OCFG_ENGINE_LOG_FORMAT):
                engine_log_format = self._config_parser.get(
                    self._resource_id,
                    OCFG_ENGINE_LOG_FORMAT,
                    raw=1)
            else:
                engine_log_format = "%(asctime)s => %(module)s line %(lineno)s: %(message)s"
            if self._config_parser.has_option(self._resource_id,
                                              OCFG_ENGINE_LOG_LEVEL):
                engine_log_level = self._config_parser.get(
                    self._resource_id,
                    OCFG_ENGINE_LOG_LEVEL)
            else:
                engine_log_level = "WARNING"
            return (engine_log_dir, engine_log_format, engine_log_level)
        else:
            return (None, None, None)

    def get_server_log_info(self):
        if self._config_parser != None:
            if self._config_parser.has_option(self._resource_id,
                                                   OCFG_SERVER_LOG_FILE):
                server_log_file = self._config_parser.get(self._resource_id,
                                                          OCFG_SERVER_LOG_FILE)
                server_log_file = os.path.expandvars(server_log_file)
            else:
                server_log_file = os.path.join(self.get_soma_workflow_dir(),
                                               'logs', 'log_server')
            if self._config_parser.has_option(self._resource_id,
                                              OCFG_SERVER_LOG_FORMAT):
                server_log_format = self._config_parser.get(
                    self._resource_id,
                    OCFG_SERVER_LOG_FORMAT,
                    raw=1)
            else:
                server_log_format = "%(asctime)s => %(module)s line %(lineno)s: %(message)s"
            if self._config_parser.has_option(self._resource_id,
                                              OCFG_SERVER_LOG_LEVEL):
                server_log_level = self._config_parser.get(
                    self._resource_id,
                    OCFG_SERVER_LOG_LEVEL)
            else:
                server_log_level = "WARNING"
            return (server_log_file, server_log_format, server_log_level)
        else:
            return (None, None, None)

    def get_container_command(self):
        if self._config_parser is not None \
                and self._config_parser.has_option(self._resource_id,
                                                   OCFG_CONTAINER_COMMAND):
            container_command = eval(self._config_parser.get(
                self._resource_id, OCFG_CONTAINER_COMMAND))
            container_command = [os.path.expandvars(item)
                                 for item in container_command]
            return container_command
        else:
            return None

    @staticmethod
    def get_allowed_python_versions(config_parser, resource_id):
        if config_parser.has_option(resource_id, OCFG_ALLOWED_PYTHON_VERSIONS):
            allowed_py_ver = config_parser.get(resource_id,
                                               OCFG_ALLOWED_PYTHON_VERSIONS)
            allowed_py_ver = [int(x.strip())
                              for x in allowed_py_ver.split(',')]
            return allowed_py_ver
        else:
            return None

    @staticmethod
    def is_python_version_matching(config_parser, resource_id):
        '''
        checks if the resource is configured for a python version which
        matches the current interpreter. If not specified in the configuration
        (see option ALLOWED_PYTHON_VERSIONS), then the resource is considered
        matching in any case (which is probably wrong in practice).
        '''
        py_ver = Configuration.get_allowed_python_versions(
            config_parser, resource_id=resource_id)
        return py_ver is None or sys.version_info[0] in py_ver

    @staticmethod
    def get_python_command(config_parser, resource_id):
        if config_parser.has_option(resource_id, OCFG_PYTHON_COMMAND):
            return config_parser.get(resource_id, OCFG_PYTHON_COMMAND)
        else:
            return None

    def make_dirs(self, anypath, is_file_path=False):
        '''
        Example
        -------
        make_dirs("/tmp/test", is_file_path=True)
        make_dirs("/tmp/test", is_file_path=False)
        '''
        if is_file_path:
            dir_path = os.path.dirname(anypath)
        else:
            dir_path = anypath
        if not os.path.isdir(dir_path):
            swf_dir = os.path.join(self.get_soma_workflow_dir(), '/')
            if dir_path == swf_dir \
                    or os.path.abspath(dir_path).startswith(swf_dir):
                if os.path.islink(swf_dir) and not os.path.exists(swf_dir):
                    # make ~/.soma-wodkflow as target of the dead link (if any)
                    swf_dir = os.readlink(swf_dir)
                    os.makedirs(swf_dir)
            os.makedirs(dir_path)

    def mk_config_dirs(self):
            # Create directories ############################################
        log_file_path, _, _ = self.get_engine_log_info()
        log_server_path, _, _ = self.get_server_log_info()
        transfered_file_dir = self.get_transfered_file_dir()
        if self._database_file is not None:
            self.make_dirs(self._database_file, is_file_path=True)
        if transfered_file_dir is not None:
            self.make_dirs(self._transfered_file_dir, is_file_path=False)
        if log_file_path is not None:
            self.make_dirs(log_file_path, is_file_path=False)
        if log_server_path is not None:
            self.make_dirs(log_server_path, is_file_path=True)

    def save_to_file(self, config_path=None):
        pass

        # home_dir = Configuration.get_home_dir()

        # disabled for now because it erases the commented part of the configuration
        # if not config_path:
        # if self._config_path != None:
            # config_path = self._config_path
        # else:
            # config_path = Configuration.search_config_path()
            # if config_path == None:
            # config_path = os.path.join(home_dir, ".soma-workflow.cfg")
            # print(config_path)

        # config_parser = configparser.ConfigParser()
        # config_parser.read(config_path)

        # if not config_parser.has_section(self._resource_id):
        # config_file = open(config_path, "w")
        # config_parser.write(config_file)
        # config_file.close()
        # raise ConfigurationError("The configuration can not be saved."
                                   #"The resource " + repr(self._resource_id) + " "
                                   #"can not be found in the configuration "
                                   #"file " + repr(config_path) + ".")

        # self.get_queue_limits()
        # if self._queue_limits != None and len(self._queue_limits):
        # queue_limits_str = ""
        # for queue, limit in six.iteritems(self._queue_limits):
            # if queue == None:
            # queue_limits_str = queue_limits_str + "{" + repr(limit) + "} "
            # else:
            # queue_limits_str = queue_limits_str + queue + "{" + repr(limit) + "} "
        # print("queue_limits_str " + queue_limits_str)
        # config_parser.set(self._resource_id,
                            # OCFG_MAX_JOB_IN_QUEUE,
                            # queue_limits_str)

        # config_file = open(config_path, "w")
        # config_parser.write(config_file)
        # config_file.close()

    def set_scheduler_config(self, scheduler_config):
        self._scheduler_config = scheduler_config

    def get_scheduler_config(self):
        return self._scheduler_config


def cpu_count():
    """
    Detects the number of CPUs on a system.
    """
    try:
        import multiprocessing
        return multiprocessing.cpu_count()
    except NotImplementedError:
        pass
    # Linux, Unix and MacOS:
    if hasattr(os, "sysconf"):
        if "SC_NPROCESSORS_ONLN" in os.sysconf_names:  # Linux & Unix:
            ncpus = os.sysconf("SC_NPROCESSORS_ONLN")
            if isinstance(ncpus, six.integer_types) and ncpus > 0:
                return ncpus
        else:  # OSX:
            from soma_workflow import subprocess
            return int(subprocess.Popen(
                ["sysctl", "-n", "hw.ncpu"],
                stdout=subprocess.PIPE).stdout.read())
    # Windows:
    if "NUMBER_OF_PROCESSORS" in os.environ:
        ncpus = int(os.environ["NUMBER_OF_PROCESSORS"])
        if ncpus > 0:
            return ncpus
    return 1  # Default


def default_cpu_number():
    '''
    Returns the number of CPU to be used on a local machine: cpu_count() - 1
    when 3 processors or more are available, or cpu_count() on a mono or
    bi-processor machine.
    '''
    # cpu = cpu_count()
    # if cpu > 2:
        # return cpu - 1
    # return cpu
    return 0


class LocalSchedulerCfg(observer.Observable):

    '''
    Local scheduler configuration.
    '''

    # number of processus which can run in parallel, which may be used
    # unconditionally.
    _proc_nb = None

    # max number of processus. Processes may be submitted additionally to
    # _proc_nb if the CPU load is not full, and this max number of running
    # processes is not reached.
    _max_proc_nb = None

    # interval (second)
    _interval = None

    # path of the configuration file
    _config_path = None

    PROC_NB_CHANGED = 0
    INTERVAL_CHANGED = 1
    MAX_PROC_NB_CHANGED = 2

    def __init__(self, proc_nb=default_cpu_number(), interval=0.05,
                 max_proc_nb=0):
        '''
        * proc_nb *int*
          Number of processus which can run in parallel

        * interval *int*
          Update interval in second
        '''

        super(LocalSchedulerCfg, self).__init__()
        self._proc_nb = proc_nb
        self._max_proc_nb = max_proc_nb
        self._interval = interval

    @classmethod
    def load_from_file(cls,
                       config_file_path=None):
        '''
        Load LocalSchedulerCfg and return it

        Parameters
        ----------
        config_file_path: string or file object
            filename or file object to read config from

        Returns
        -------
        LocalSchedulerCfg object
        '''

        hostname = socket.gethostname()
        if config_file_path:
            config_path = config_file_path
        else:
            config_path = Configuration.search_config_path()

        config_parser = configparser.ConfigParser()
        if hasattr(config_path, 'readline'):
            config_parser.readfp(config_path)
        elif config_path is not None:
            config_parser.read(config_path)

        if not config_parser.has_section(hostname):
            raise ConfigurationError("Wrong config file format. Can not find "
                                     "section " + hostname +
                                     " in configuration "
                                     "file: " + config_path)

        proc_nb = 0
        max_proc_nb = 0
        interval = None

        if config_parser.has_option(hostname,
                                    OCFG_SCDL_CPU_NB):
            proc_nb_str = config_parser.get(socket.gethostname(),
                                            OCFG_SCDL_CPU_NB)
            proc_nb = int(proc_nb_str)
        if config_parser.has_option(hostname,
                                    OCFG_SCDL_INTERVAL):
            interval_str = config_parser.get(hostname,
                                             OCFG_SCDL_INTERVAL)
            interval = float(interval_str)
        if config_parser.has_option(hostname,
                                    OCFG_SCDL_MAX_CPU_NB):
            max_proc_nb_str = config_parser.get(socket.gethostname(),
                                                OCFG_SCDL_MAX_CPU_NB)
            max_proc_nb = int(max_proc_nb_str)

        config = cls(proc_nb=proc_nb, interval=interval,
                     max_proc_nb=max_proc_nb)
        config._config_path = config_path
        return config

    @staticmethod
    def search_config_path():
        '''
        returns the path of the soma workflow configuration file
        '''
        hostname = socket.gethostname()
        home_dir = Configuration.get_home_dir()
        section_exist = False
        config_path = os.path.join(home_dir, ".soma-workflow-scheduler.cfg")
        if os.path.isfile(config_path):
            config_parser = configparser.ConfigParser()
            config_parser.read(config_path)
            section_exist = config_parser.has_section(hostname)
        if not section_exist:
            config_path = os.path.dirname(__file__)
            config_path = os.path.join(
                config_path, "etc/soma-workflow-scheduler.cfg")
            if os.path.isfile(config_path):
                config_parser = configparser.ConfigParser()
                config_parser.read(config_path)
                section_exist = config_parser.has_section(hostname)
        if not section_exist:
            config_path = "/etc/soma-workflow-scheduler.cfg"
            if os.path.isfile(config_path):
                config_parser = configparser.ConfigParser()
                config_parser.read(config_path)
                section_exist = config_parser.has_section(hostname)
        if not section_exist:
            config_path = None

        return config_path

    def get_proc_nb(self):
        return self._proc_nb

    def get_max_proc_nb(self):
        return self._max_proc_nb

    def get_cpu_count(self):
        return cpu_count()

    def get_interval(self):
        return self._interval

    def set_proc_nb(self, proc_nb):
        self._proc_nb = proc_nb
        self.notifyObservers(LocalSchedulerCfg.PROC_NB_CHANGED)

    def set_max_proc_nb(self, proc_nb):
        self._max_proc_nb = proc_nb
        self.notifyObservers(LocalSchedulerCfg.MAX_PROC_NB_CHANGED)

    def set_interval(self, interval):
        self._interval = interval
        self.notifyObservers(LocalSchedulerCfg.INTERVAL_CHANGED)

    def save_to_file(self, config_path=None):
        hostname = socket.gethostname()
        if not config_path:
            if self._config_path != None:

                config_path = self._config_path
            else:
                config_path = LocalSchedulerCfg.search_config_path()
                if config_path == None:
                    home_dir = Configuration.get_home_dir()
                    config_path = os.path.join(
                        home_dir, ".soma-workflow-scheduler.cfg")

        config_parser = configparser.ConfigParser()
        config_parser.read(config_path)

        if not config_parser.has_section(hostname):
            config_parser.add_section(hostname)

        config_parser.set(hostname,
                          OCFG_SCDL_CPU_NB,
                          str(self._proc_nb))
        config_parser.set(hostname,
                          OCFG_SCDL_INTERVAL,
                          str(self._interval))
        config_parser.set(hostname,
                          OCFG_SCDL_MAX_CPU_NB,
                          str(self._max_proc_nb))
        config_file = open(config_path, "w")
        config_parser.write(config_file)
        config_file.close()


def AddLineDefinitions2BashrcFile(lines2add, path2bashrc=""):
    """Add line defintions to the ~/.bashrc file

    Removing the lines2add which are already exsting in the ~/.bashrc
    Adding the lines2add at the end of ~/.bashrc

    Args:
        lines2add (string):  a list of line definitions. Line defintion like export PATH=~/mylocal/bin:${PATH}
        path2bashrc (string, optional): path to the ./bashrc file. It could be another path. Default paht is "~/.bashrc"

    Raises:
       IOError, ValueError

    Example:
        lines2add = ["SOMAWF_PATH=%s/soma-workflow"%(os.getenv("HOME")),
        'export PATH=$SOMAWF_PATH/bin:$PATH',
        'export PYTHONPATH=$SOMAWF_PATH/python:$PYTHONPATH',
        'export SOMA_WORKFLOW_EXAMPLES=$SOMAWF_PATH/test/jobExamples/',
        'export SOMA_WORKFLOW_EXAMPLES_OUT=$SOMAWF_PATH/test/jobExamples_out/']
        >>> print(AddVariables2BashrcFile(lines2add, "~/.bashrc"))
    """
    import os
    import sys

    if path2bashrc == "":

        path2bashrc = os.path.join(os.getenv("HOME"), ".bashrc")

    lines2rm = []
    content = []

    try:
        with open(path2bashrc) as f:
            content = f.readlines()
    except IOError as e:
        print("I/O error({0}): {1}".format(e.errno, e.strerror))
        print("%s does not exist, the system will create the new file"
              % (path2bashrc))
    except ValueError:
        print("Could not convert data to an integer.")
    except:  # noqa: E722
        print("Unexpected error:", sys.exc_info()[0])
        raise

    for i in range(len(content)):
        content[i] = content[i].rstrip()

    # try to find the duplicated paths and remove them
    for line2add in lines2add:
        isfound = any(line2add.strip() == cline.strip() for cline in content)
        while isfound:
            content.remove(line2add.strip())
            isfound = any(line2add.strip() == cline.strip()
                          for cline in content)

    for line2add in lines2add:
        content.append(line2add)

    try:
        with open(path2bashrc, 'w') as f:
            for cline in content:
                f.write(cline + "\n")
    except IOError as e:
        print("I/O error({0}): {1}".format(e.errno, e.strerror))
        print("The system cannot write the file %s. Please make sure it can "
              "be written. " % (path2bashrc))
        raise e
    except ValueError:
        print("Could not convert data to an integer.")
        raise ValueError
    except:  # noqa: E722
        print("Unexpected error:", sys.exc_info()[0])
        raise


def WriteOutConfiguration(config_parser, config_path):
    try:
        with open(config_path, 'w') as cfgfile:
            config_parser.write(cfgfile)
    except IOError as e:
        print("I/O error({0}): {1}".format(e.errno, e.strerror))
        print("The system cannot write the file %s. Please make sure that it "
              "can be written. " % (config_path))
        raise e
    except ValueError:
        print("Could not convert data to an integer.%s" % (config_path))
        raise ValueError
    except:  # noqa: E722
        print("Unexpected error:", sys.exc_info()[0])
        raise

def change_soma_workflow_directory(directory, config_name=None):
    '''
    Temporarily and locally change the configuration file of Soma-Workflow to a
    different directory. This is useful during tests to isolate the SWF
    environment from the current user settings and workflows.
    '''
    if not config_name:
        config_name = socket.gethostname()
    swf_conf = '[%s]\nSOMA_WORKFLOW_DIR = %s\n' \
        % (config_name, directory)
    if not hasattr(Configuration, '_old_search_config_path'):
        Configuration._old_search_config_path \
            = Configuration.search_config_path
    Configuration.search_config_path \
        = staticmethod(lambda: six.StringIO(swf_conf))

def restore_soma_workflow_directory():
    '''
    Restore the search_config_path static method of Configuration after
    change_soma_workflow_directory() has been used.
    '''
    if hasattr(Configuration, '_old_search_config_path'):
        if sys.version_info[0] < 3:
            Configuration.search_config_path \
                = staticmethod(Configuration._old_search_config_path.im_func)
        else:
            Configuration.search_config_path \
                = staticmethod(Configuration._old_search_config_path)
