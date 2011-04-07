'''
@author: Soizic Laguitton
@organization: U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''

#-------------------------------------------------------------------------------
# Imports
#-------------------------------------------------------------------------------

import os
import socket
import ConfigParser

import soma.workflow.constants as constants
from soma.workflow.errors import ConfigurationError


#-------------------------------------------------------------------------------
# Classes and functions
#-------------------------------------------------------------------------------

class Configuration(object):
  
  # path of the configuration file
  config_path = None

  # config parser object
  config = None

  resource_id = None

  mode = None

  submitting_machines = None

  cluster_address = None

  database_file = None

  transfered_file_dir = None
  
  parallel_job_config = None

  path_translation = None

  queue_limits = None

  queues = None

  def __init__(self, 
               resource_id, 
               config_file_path=None):

    self.resource_id = resource_id
    if config_file_path:
      self.config_path = config_file_path
    else:
      self.config_path = Configuration.search_config_path()

    self.config = ConfigParser.ConfigParser()
    self.config.read(self.config_path)
    if not self.config.has_section(resource_id):
      raise ConfigurationError("Can not find section " + self.resource_id + " "
                               "in configuration file: " + self.config_path)


  def get_submitting_machines(self):
    if self.submitting_machines: 
      return self.submitting_machines

    if not self.config.has_option(self.resource_id,
                                  constants.CFG_SUBMITTING_MACHINES):
      raise ConfigurationError("Can not find the configuration item %s for the "
                               "resource %s, in the configuration file %s." %
                               (constants.CFG_SUBMITTING_MACHINES,
                                self.resource_id,
                                self.config_path))
    self.submitting_machines= self.config.get(self.resource_id, 
                                         constants.CFG_SUBMITTING_MACHINES).split()
    return self.submitting_machines


  @staticmethod
  def search_config_path():
    '''
    returns the path of the soma workflow configuration file
    '''
    config_path = None
    if not config_path or not os.path.isfile(config_path):
      config_path = os.getenv('SOMA_WORKFLOW_CONFIG')
    if not config_path or not os.path.isfile(config_path):
      config_path = os.path.expanduser("~/.soma-workflow.cfg")
    if not config_path or not os.path.isfile(config_path):
      config_path = os.path.dirname(__file__)
      config_path = os.path.dirname(__file__)
      config_path = os.path.dirname(__file__)
      config_path = os.path.dirname(__file__)
      config_path = os.path.join(config_path, "etc/soma-workflow.cfg")
    if not config_path or not os.path.isfile(config_path):
      config_path = "/etc/soma-workflow.cfg"
    if not config_path or not os.path.isfile(config_path):
      raise ConfigurationError("Can not find the soma-workflow "
                               "configuration file. \n")
    return config_path


  @staticmethod
  def get_configured_resources(config_file_path):
    resource_ids = []
    config = ConfigParser.ConfigParser()
    config.read(config_file_path)
    for r_id in config.sections():
      resource_ids.append(r_id)
    return resource_ids


  def get_mode(self):
    '''
    Return the application mode: 'local', 'remote' or 'light'
    '''
    if self.mode:
      return self.mode

    if self.config.has_option(self.resource_id, 
                              constants.OCFG_LIGHT_MODE):
      self.mode = 'light'
      return self.mode
    
    if not self.submitting_machines:
      self.submitting_machines = self.get_submitting_machines()
   
    hostname = socket.gethostname()
    mode = 'remote'
    for machine in self.submitting_machines:
      if hostname == machine: 
        mode = 'local'
    self.mode = mode 
    return mode
    

  def get_cluster_address(self):
    if self.cluster_address:
      return self.cluster_address
    
    if not self.config.has_option(self.resource_id,
                                  constants.CFG_CLUSTER_ADDRESS):
      raise ConfigurationError("Can not find the configuration item %s for the "
                               "resource %s, in the configuration file %s." %
                               (constants.CFG_CLUSTER_ADDRESS,
                                self.resource_id,
                                self.config_path))
    self.cluster_address= self.config.get(self.resource_id, 
                                     constants.CFG_CLUSTER_ADDRESS)
    return self.cluster_address


  def get_database_file(self):
    if self.database_file:
      return self.database_file

    if not self.config.has_option(self.resource_id,
                                  constants.CFG_DATABASE_FILE):
      raise ConfigurationError("Can not find the configuration item %s " 
                                "for the resource %s, in the configuration " "file %s." %
                                (constants.CFG_DATABASE_FILE,
                                  self.resource_id,
                                  self.config_path))
    self.database_file= self.config.get(self.resource_id, 
                                        constants.CFG_DATABASE_FILE)
    return self.database_file


  def get_transfered_file_dir(self):
    if self.transfered_file_dir:
      return self.transfered_file_dir

    if not self.config.has_option(self.resource_id,
                                  constants.CFG_TRANSFERED_FILES_DIR):
      raise ConfigurationError("Can not find the configuration item %s " 
                                "for the resource %s, in the configuration " "file %s." %
                                (constants.CFG_TRANSFERED_FILES_DIR,
                                  self.resource_id,
                                  self.config_path))
    self.transfered_file_dir= self.config.get(self.resource_id, 
                                              constants.CFG_TRANSFERED_FILES_DIR)
    return self.transfered_file_dir


  def get_parallel_job_config(self):
    if self.parallel_job_config != None:
      return self.parallel_job_config
    
    self.parallel_job_config = {}
    for parallel_config_info in constants.PARALLEL_DRMAA_ATTRIBUTES + \
                                constants.PARALLEL_JOB_ENV + \
                                constants.PARALLEL_CONFIGURATIONS:
      if self.config.has_option(self.resource_id, parallel_config_info):
        self.parallel_job_config[parallel_config_info] = self.config.get(self.resource_id, parallel_config_info)

    return self.parallel_job_config


  def get_drmaa_implementation(self):
    drmaa_implementation = None
    if self.config.has_option(self.resource_id, 
                              constants.OCFG_DRMAA_IMPLEMENTATION):
      drmaa_implementation = self.config.get(self.resource_id,                    
                                             constants.OCFG_DRMAA_IMPLEMENTATION)
    return drmaa_implementation

  
  def get_path_translation(self):
    if self.path_translation != None:
      return self.path_translation

    self.path_translation = {}
    if self.config.has_option(self.resource_id, 
                              constants.OCFG_PATH_TRANSLATION_FILES):
      translation_files_str = self.config.get(self.resource_id, 
                                              constants.OCFG_PATH_TRANSLATION_FILES)
      #logger.info("Path translation files configured:")
      for ns_file_str in translation_files_str.split():
        ns_file = ns_file_str.split("{")
        namespace = ns_file[0]
        filename = ns_file[1].rstrip("}")
        #logger.info(" -namespace: " + namespace + ", translation file: " + filename)
        try: 
          f = open(filename, "r")
        except IOError, e:
          raise ConfigurationError("Can not read the translation file %s" %
                                    (filename))
        
        if not namespace in self.path_translation.keys():
          self.path_translation[namespace] = {}
        line = f.readline()
        while line:
          splitted_line = line.split(None,1)
          if len(splitted_line) > 1:
            uuid = splitted_line[0]
            content = splitted_line[1].rstrip()
            #logger.info("    uuid: " + uuid + "   translation:" + content)
            self.path_translation[namespace][uuid] = content
          line = f.readline()
        f.close()

    return self.path_translation
      

  def get_name_server_host(self):
    name_server_host = self.config.get(self.resource_id, 
                                       constants.CFG_NAME_SERVER_HOST)
    return name_server_host


  def get_server_name(self):
    if not self.config.has_option(self.resource_id,
                                  constants.CFG_SERVER_NAME):
      raise ConfigurationError("Can not find the configuration item %s " 
                                "for the resource %s, in the configuration " "file %s." %
                                (constants.CFG_SERVER_NAME,
                                  self.resource_id,
                                  self.config_path))
    server_name = self.config.get(self.resource_id, 
                                  constants.CFG_SERVER_NAME)
    return server_name

  
  def get_queue_limits(self):
    if self.queue_limits != None:
      return self.queue_limits

    self.queue_limits = {}
    if self.config.has_option(self.resource_id, 
                              constants.OCFG_MAX_JOB_IN_QUEUE):
      queue_limits_str = self.config.get(self.resource_id,
                                         constants.OCFG_MAX_JOB_IN_QUEUE)
      for info_str in queue_limits_str.split():
        info = info_str.split("{")
        if len(info[0]) == 0:
          queue_name = None
        else:
          queue_name = info[0]
        max_job = int(info[1].rstrip("}"))
        self.queue_limits[queue_name] = max_job

    return self.queue_limits

  
  def get_queues(self):
    if self.queues !=  None:
      return self.queues

    self.queues = []
    if self.config.has_option(self.resource_id,
                              constants.OCFG_QUEUES):
      self.queues.extend(self.config.get(self.resource_id,
                                         constants.OCFG_QUEUES).split())
    return self.queues


  def get_engine_log_info(self):
    if self.config.has_option(self.resource_id,
                              constants.OCFG_ENGINE_LOG_DIR):
      engine_log_dir = self.config.get(self.resource_id, 
                                       constants.OCFG_ENGINE_LOG_DIR)
      if self.config.has_option(self.resource_id,
                                    constants.OCFG_ENGINE_LOG_FORMAT):
        engine_log_format = self.config.get(self.resource_id,
                                            constants.OCFG_ENGINE_LOG_FORMAT,
                                            1)
      else:
        engine_log_format = "%(asctime)s => %(module)s line %(lineno)s: %(message)s"
      if self.config.has_option(self.resource_id,
                                constants.OCFG_ENGINE_LOG_LEVEL):
        engine_log_level = self.config.get(self.resource_id,
                                          constants.OCFG_ENGINE_LOG_LEVEL)
      else:
        engine_log_level = "WARNING"
      return (engine_log_dir, engine_log_format, engine_log_level)
    else:
      return (None, None, None)



  def get_server_log_info(self):
    if self.config.has_option(self.resource_id,
                              constants.OCFG_SERVER_LOG_FILE):
      server_log_file = self.config.get(self.resource_id, 
                                        constants.OCFG_SERVER_LOG_FILE)
      if self.config.has_option(self.resource_id,
                                constants.OCFG_SERVER_LOG_FORMAT):
        server_log_format = self.config.get(self.resource_id,
                                            constants.OCFG_SERVER_LOG_FORMAT, 
                                            1)
      else:
        server_log_format = "%(asctime)s => %(module)s line %(lineno)s: %(message)s"
      if self.config.has_option(self.resource_id,
                                constants.OCFG_SERVER_LOG_LEVEL):
        server_log_level = self.config.get(self.resource_id,
                                          constants.OCFG_SERVER_LOG_LEVEL)
      else:
        server_log_level = "WARNING"
      return (server_log_file, server_log_format, server_log_level)
    else:
      return (None, None, None)


  
