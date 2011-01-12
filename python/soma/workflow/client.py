'''
@author: Soizic Laguitton
@organization: U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''
__docformat__ = "epytext en"

''' 
soma-workflow client classes

The WorkflowController instances allow to submit, control and monitor jobs and
workflows on local or remote resources where the WorkflowDatabaseServer runs.
The contrustor takes a resource id, login and password as parameters and 
connects to the resource. One WorkflowController instance is connected to 
only one resource.

The other classes (Workflow, Job, FileTransfer, FileSending, FileRetrieving, 
SharedResourcePath and WorkflowNodeGroup) are made to build the jobs, worklfows, 
and file transfers objects to be used in the WorkflowControler interface.

Definitions:
A Parallel job is a job requiring more than one node to run.
DRMS: distributed resource management system
'''

#-------------------------------------------------------------------------------
# Imports
#-------------------------------------------------------------------------------

import ConfigParser
import os
import hashlib
import stat
import operator
import random
import socket
import time

import soma.workflow.connection as connection
from soma.workflow.constants import *


#-------------------------------------------------------------------------------
# Classes and functions
#-------------------------------------------------------------------------------

class Job(object):
  '''
  Job representation in a soma-workflow. 
  Workflow node type.
  
  The job parameters are identical to the WorkflowController.submit_job method 
  arguments except command, referenced_input_files and references_output_files.
  
  @type  command: sequence of string, 
                              L{SharedResourcePath},
                              L{FileTransfer},
                              tuple (relative path, L{FileTransfer}),
                              sequence of L{SharedResourcePath},
                              sequence of L{FileTransfer},
                              and/or sequence of tuple 
                              (relative path, L{FileTransfer})
  @param command: The command to execute. 
                  On the computing resource side, the L{SharedResourcePath} and
                  L{FileTransfer} objects will be replaced by the appropriate 
                  path before the job execution.
                  The tuple (absolute path, L{FileTransfer}) can be use to 
                  refer to a file in a transfered directory. The tuple will be 
                  replaced by the path:
                  "computing_resource_dir_path/absolute_path"
                  The sequence(s) of L{SharedResourcePath}, L{FileTransfer},
                  and tuple (absolute path, L{FileTransfer}) will be replaced 
                  by the string:
                  "['/somewhere/file1', '/somewhere/file2', '/somewhere/path3']"
                  
  @type referenced_input_files: sequence of L{FileTransfer}
  @param referenced_input_files: list of all tranfered input file required
                                 for the job to run.
  @type referenced_output_files: sequence of L{FileTransfer}
  @param referenced_input_files: list of all transfered output file required
                                 for the job to run.
                                 
  (See the WorkflowController.submit_job method for a description of each parameter)
  '''
  name = None

  command = None

  referenced_input_files = None

  referenced_output_files = None

  stdin = None

  join_stderrout = None

  stdout_file = None

  stderr_file = None
  
  working_directory = None

  parallel_job_info = None

  def __init__( self, 
                command,
                referenced_input_files=None,
                referenced_output_files=None,
                stdin=None,
                join_stderrout=False,
                disposal_timeout=168,
                name_description=None,
                stdout_file=None,
                stderr_file=None,
                working_directory=None,
                parallel_job_info=None):
    self.name = name_description
    self.command = command
    if referenced_input_files: 
      self.referenced_input_files = referenced_input_files
    else: self.referenced_input_files = set([])
    if referenced_output_files:
      self.referenced_output_files = referenced_output_files
    else: self.referenced_output_files = set([]) 
    self.stdin = stdin
    self.join_stderrout = join_stderrout
    self.disposal_timeout = disposal_timeout
    self.name_description = name_description
    self.stdout_file = stdout_file
    self.stderr_file = stderr_file
    self.working_directory = working_directory
    self.parallel_job_info = parallel_job_info
    
    self.job_id = -1
    self.workflow_id = -1

    
    
class SharedResourcePath(object):
  '''
  Representation of path universaly over the resource.
  The information required is:
    - a namespace
    - a database uuid
    - the relative path of the file in the database
  The SharedResourcePath objects can be used instead of file path to describe job 
  (Job.command, Job.stdin, Job.stdout_file and Job.stderr_file)
  When a Workflow or a Job is submitted to a resource with the JobClient API, 
  the SharedResourcePath objects are replaced by the absolut path of the file on the resource,
  provided the namespace and database are configured on the cluster (OCFG_U_PATH_TRANSLATION_FILES).   
  '''
  
  relative_path = None
  
  namespace = None

  uuid = None

  def __init__(self,
               relative_path,
               namespace,
               uuid,
               disposal_timeout = 168,
               name = None):#,
               #relative_paths = None):
    if name:
      self.name = name
    else:
      self.name = namespace + "::" + uuid + "::" + relative_path
   
    self.relative_path = relative_path
    self.namespace = namespace
    self.uuid = uuid
    self.disposal_timout = disposal_timeout

class FileTransfer(object):
  '''
  Workflow node type.
  Abstract class (use FileSending or FileRetrieving) 
  File/directory transfer representation in a workflow.
  Use remote_paths if the transfer involves several associated files and/or directories, eg:
        - file serie 
        - in the case of file format associating several file and/or directories
          (ex: a SPM image is stored in 2 files: .img and .hdr)
  In this case, set remote_path to one the files (eq: .img).
  In other cases (1 file or 1 directory) the remote_paths must be set to None.
  '''
  name = None

  remote_path = None

  remote_paths = None

  def __init__( self,
                remote_path, 
                disposal_timeout = 168,
                name = None,
                remote_paths = None):
    if name:
      ft_name = name
    else:
      ft_name = remote_path + "transfer"
    self.name = ft_name

    self.remote_path = remote_path
    self.disposal_timeout = disposal_timeout

    self.remote_paths = remote_paths
    self.local_path = None

class FileSending(FileTransfer):
  '''
  Workflow node type.
  File/directory transfer for an input file.
  '''
  def __init__( self,
                remote_path, 
                disposal_timeout = 168,
                name = None,
                remote_paths = None):
    super(FileSending, self).__init__(remote_path, disposal_timeout, name, remote_paths)
   

class FileRetrieving(FileTransfer):
  '''
  Workflow node type.
  File/directory transfer for an output file.
  '''
  def __init__( self,
                remote_path, 
                disposal_timeout = 168,
                name = None,
                remote_paths = None):
    super(FileRetrieving, self).__init__(remote_path, disposal_timeout, name, remote_paths)
    
class WorkflowNodeGroup(object):
  '''
  Workflow node group: provides a hierarchical structure to a workflow.
  However groups has only a displaying role, it doesn't have
  any impact on the workflow execution.
  '''
  name = None

  elements = None

  def __init__(self, elements, name = None):
    '''
    @type  elements: sequence of Job and WorkflowNodeGroup
    @param elements: the elements belonging to the group.
    @type  name: string
    @param name: name of the group. 
    If name is None the group will be named 'group'
    '''
    self.elements = elements
    if name:
      self.name = name
    else: 
      self.name = "group"


class Workflow(object):
  '''
  Workflow to be submitted using an instance of the WorkflowController class.
  '''
  name = None

  nodes = None

  dependencies = None

  groups = None

  mainGroup = None
  

  def __init__(self, 
               nodes, 
               dependencies, 
               mainGroup = None, 
               groups = [], 
               disposal_timeout = 168,):
    '''
    @type  node: sequence of L{Job} 
    @param node: job in the workflow
    @type  dependencies: sequence of tuple (node, node) a node being 
    a L{Job}
    @param dependencies: dependencies between jobs.
    @type  groups: sequence of sequence of L{WorkflowNodeGroup} and/or L{Job}
    @param groups: (optional) provide a hierarchical structure to a workflow for displaying purpose only
    @type  mainGroup: L{WorkflowNodeGroup}
    @param mainGroup: (optional) lower level group.  
    '''
    
    self.wf_id = -1
    self.name = None
    self.nodes = nodes
    self.dependencies = dependencies
    self.groups= groups
    self.disposal_timeout = 168
    if mainGroup:
      self.mainGroup = mainGroup
    else:
      elements = []
      for job in self.nodes:
        elements.append(job) 
      self.mainGroup = WorkflowNodeGroup(elements, "main_group")
      

class WorkflowController(object):
  '''
  Submition, controlling and monitoring of Jobs, FileTransfers and Workflows.
  '''
  def __init__(self, 
               config_file,
               resource_id, 
               login = None, 
               password = None,
               log = ""):
    '''
    @type  config_file: string
    @param config_file: configuration file path
    @type  resource_id: C{ResourceIdentifier} or None
    @param resource_id: The name of the resource to use, eg: "neurospin_test_cluster" or 
    "DSV_cluster"... the ressource_id config must be inside the config_file.
    @param login and password: only required if run from a submitting machine of the cluster.
    '''
    
    
    #########################
    # reading configuration 
    config = ConfigParser.ConfigParser()
    config.read(config_file)
    self.resource_id = resource_id
    self.config = config
   
    if not config.has_section(resource_id):
      raise Exception("Can't find section " + resource_id + " in configuration file: " + config_file)

    submitting_machines = config.get(resource_id, CFG_SUBMITTING_MACHINES).split()
    cluster_address = config.get(resource_id, CFG_CLUSTER_ADDRESS)
    hostname = socket.gethostname()
    mode = 'remote'
    for machine in submitting_machines:
      if hostname == machine: mode = 'local'
    print "hostname: " + hostname + " => mode = " + mode

    #########################
    # Connection
    self._mode = mode #  'local_no_disconnection' #(local debug)#       

    #########
    # LOCAL #
    #########
    if self._mode == 'local':
      self._connection = connection.LocalConnection(resource_id,  
                                                                  log)
      self._engine_proxy = self._connection.get_workflow_engine()
    
    ##########
    # REMOTE #
    ##########
    if self._mode == 'remote':
      sub_machine = submitting_machines[random.randint(0, len(submitting_machines)-1)]
      print 'cluster address: ' + cluster_address
      print 'submission machine: ' + sub_machine
      self._connection = connection.RemoteConnection(login, password, cluster_address, sub_machine, resource_id, log)
      self._engine_proxy = self._connection.get_workflow_engine()
    
    ###############
    # LOCAL DEBUG #
    ###############
    if self._mode == 'local_no_disconnection': # DEBUG
      import soma.workflow.engine
      import logging
      import Pyro.naming
      import Pyro.core
      from Pyro.errors import PyroError, NamingError
      
      # log file 
      print "config_file " + repr(config_file)
      if not config.get(resource_id, OCFG_ENGINE_LOG_DIR) == 'None':
        logfilepath = config.get(resource_id, OCFG_ENGINE_LOG_DIR) + "log_debug_local"

        print "logfilepath: " + repr(logfilepath)
        logging.basicConfig(
          filename=logfilepath,
          format=config.get(resource_id, OCFG_ENGINE_LOG_FORMAT, 1),
          level=eval("logging." + config.get(resource_id, OCFG_ENGINE_LOG_LEVEL)))
      
      logger = logging.getLogger('engine')
      logger.info(" ")
      logger.info("****************************************************")
      logger.info("****************************************************")
    
      # looking for the database server
      Pyro.core.initClient()
      locator = Pyro.naming.NameServerLocator()
      name_server_host = config.get(resource_id, CFG_NAME_SERVER_HOST)
      if name_server_host == 'None':
        ns = locator.getNS()
      else: 
        ns = locator.getNS(host= name_server_host )
    
      server_name = config.get(resource_id, CFG_SERVER_NAME)
      try:
        uri = ns.resolve(server_name)
        logger.info('Server URI:'+ repr(uri))
      except NamingError,x:
        logger.critical('Couldn\'t find' + server_name + ' nameserver says:',x)
        raise SystemExit
      database_server= Pyro.core.getProxyForURI(uri)
  
      #parallel_job_submission_info
      parallel_job_submission_info= {}
      for parallel_config_info in PARALLEL_DRMAA_ATTRIBUTES + \
                                  PARALLEL_JOB_ENV + \
                                  PARALLEL_CONFIGURATIONS:
        if config.has_option(resource_id, parallel_config_info):
          parallel_job_submission_info[parallel_config_info] = config.get(resource_id, parallel_config_info)

      # Drmaa implementation
      drmaa_implem = None
      if config.has_option(resource_id, OCFG_DRMAA_IMPLEMENTATION):
        drmaa_implem = config.get(resource_id, OCFG_DRMAA_IMPLEMENTATION)
  
      # translation files specific information 
      path_translation = None
      if config.has_option(resource_id, OCFG_PATH_TRANSLATION_FILES):
        path_translation = {}
        translation_files_str = config.get(resource_id, 
                                           OCFG_PATH_TRANSLATION_FILES)
        logger.info("Path translation files configured:")
        for ns_file_str in translation_files_str.split():
          ns_file = ns_file_str.split("{")
          namespace = ns_file[0]
          filename = ns_file[1].rstrip("}")
          logger.info(" -namespace: " + namespace + ", translation file: " + filename)
          try: 
            f = open(filename, "r")
          except IOError, e:
            logger.info("Couldn't read the translation file: " + filename)
          else:
            if not namespace in path_translation.keys():
              path_translation[namespace] = {}
            line = f.readline()
            while line:
              splitted_line = line.split(None,1)
              if len(splitted_line) > 1:
                uuid = splitted_line[0]
                contents = splitted_line[1].rstrip()
                logger.info("    uuid: " + uuid + "   translation:" + contents)
                path_translation[namespace][uuid] = contents
              line = f.readline()
            f.close()
          
      drmaa = soma.workflow.engine.Drmaa(drmaa_implem, 
                                         parallel_job_submission_info)

      engine_loop = soma.workflow.engine.WorkflowEngineLoop(database_server,
                                                            drmaa,
                                                            path_translation)
    
      self._engine_proxy = soma.workflow.engine.WorkflowEngine(database_server, 
                                                               engine_loop)

      engine_loop_thread = soma.workflow.engine.EngineLoopThread(engine_loop)
      engine_loop_thread.setDaemon(True)
      engine_loop_thread.start()

      self._connection = None

    


  def disconnect(self):
    '''
    Simulates a disconnection for TEST PURPOSE ONLY.
    !!! The current instance won't be usable anymore after this call !!!!
    '''
    self._connection.stop()

   


  ########## FILE TRANSFER ###############################################
    
  '''
  The file transfer methods must be used when submitting a job from a remote
  machine
  
  Example:
  
  #job remote input files: rfin_1, rfin_2, ..., rfin_n 
  #job remote output files: rfout_1, rfout_2, ..., rfout_m  
  
  #Call register_transfer for each transfer file:
  lfout_1 = wf_controller.register_transfer(rfout_1)
  lfout_2 = wf_controller.register_transfer(rfout_2)
  ...
  lfout_n = wf_controller.register_transfer(rfout_m)

  lfin_1= wf_controller.register_transfer(rfin_1)
  lfin_2= wf_controller.register_transfer(rfin_2)
  ...
  lfin_n= wf_controller.register_transfer(rfin_n)
  
  #Call send for each input file:
  wf_controller.send(lfin_1)
  wf_controller.send(lfin_1)
  ...
  wf_controller.send(lfin_1)
    
  #Job submittion: don't forget to reference local input and output files 
  job_id = wf_controller.submit_job(['python', '/somewhere/something.py'], 
                       [lfin_1, lfin_2, ..., lfin_n],
                       [lfout_1, lfout_2, ..., lfout_n])
  wf_controller.wait_job(job_id)
  
  #After Job execution, transfer back the output file
  wf_controller.retrieve(lfout_1)
  wf_controller.retrieve(lfout_2)
  ...
  wf_controller.retrieve(lfout_m)
  
  When sending or registering a transfer, use remote_paths if the transfer involves several associated files and/or directories:
          - when transfering a file serie 
          - in the case of file format associating several file and/or directories
            (ex: a SPM image is stored in 2 files: .img and .hdr)
  In this case, set remote_path to one the files (eq: .img).
  In other cases (1 file or 1 directory) the remote_paths must be set to None.
  
  Example:
    
  #transfer of a SPM image file
  fout_1 = wf_controller.register_transfer(remote_path = 'mypath/myimage.img', 
                                 remote_paths = ['mypath/myimage.img', 'mypath/myimage.hdr'])
  ...
  wf_controller.retrive(fout_1)
 
  '''
        
  def register_transfer(self, remote_path, disposal_timeout=168, remote_paths=None): 
    '''
    Generates a unique local path and save the (local_path, remote_path) association.
    
    @type  remote_path: string
    @param remote_path: remote path of file
    @type  disposalTimeout: int
    @param disposalTimeout: The local file and transfer information is 
    automatically deleted after disposal_timeout hours, except if a job 
    references it as output or input. Default delay is 168 hours (7 days).
    @type remote_paths: sequence of string or None
    @type remote_paths: sequence of file to transfer if transfering a 
    file serie or if the file format involve serveral file of directories.
    @rtype: string or sequence of string
    @return: local file path associated with the remote file
    '''
    return self._engine_proxy.register_transfer(remote_path, disposal_timeout, remote_paths)

  def send(self, local_path, buffer_size = 512**2):
    '''
    Transfers one or several remote file(s) to a local directory. The local_path 
    must have been generated using the register_transfer method. 
    '''
    
    local_path, remote_path, expiration_date, workflow_id, remote_paths = self._engine_proxy.transfer_information(local_path)
    transfer_action_info = self._engine_proxy.transfer_action_info(local_path)
    if not transfer_action_info:
      init_info = self.initialize_sending_transfer(local_path)
    if not remote_paths:
      if os.path.isfile(remote_path):
        if transfer_action_info:
          self._send_file(remote_path, 
                          local_path, 
                          buffer_size, 
                          transmitted = transfer_action[1])
        else:
          self._send_file(remote_path, 
                          local_path, 
                          buffer_size)
                          
      elif os.path.isdir(remote_path):
        if transfer_action_info:
          for relative_path, (file_size, transmitted) in transfer_action_info[1].iteritems():
            self._send_file(remote_path, 
                            local_path, 
                            buffer_size, 
                            transmitted, 
                            relative_path)
        else:
          for relative_path in init_info[1]:
            self._send_file(remote_path, 
                            local_path, 
                            buffer_size,
                            transmitted = 0,
                            relative_path = relative_path)
    else:
      if transfer_action_info:
        for relative_path, (file_size, transmitted) in transfer_action_info[1].iteritems():
          self._send_file(os.path.dirname(remote_path), 
                          local_path, 
                          buffer_size, 
                          transmitted,
                          relative_path)
      else:
        for relative_path in init_info[1]:
          self._send_file(os.path.dirname(remote_path), 
                          local_path, 
                          buffer_size, 
                          transmitted = 0,
                          relative_path = relative_path)
        
    

  def retrieve(self, local_path, buffer_size = 512**2):
    '''
    If local_path is a file path: copies the local file to the associated remote file path.
    If local_path is a directory path: copies the contents of the directory to the associated remote directory.
    The local path must belong to the user's transfers (ie belong to 
    the sequence returned by the L{transfers} method). 
    
    @type  local_path: string 
    @param local_path: local path 
    '''
    local_path, remote_path, expiration_date, workflow_id, remote_paths = self._engine_proxy.transfer_information(local_path)
    transfer_action_info = self.initialize_retrieving_transfer(local_path)
    
    if transfer_action_info[2] == FILE_RETRIEVING:
      # file case
      (file_size, md5_hash, transfer_type) = transfer_action_info
      self._retrieve_file(remote_path, 
                          local_path, 
                          file_size, 
                          md5_hash, 
                          buffer_size)
    elif transfer_action_info[2] == DIR_RETRIEVING:
      # dir case
      (cumulated_file_size, file_transfer_info, transfer_type) = transfer_action_info
      
      for relative_path, file_info in file_transfer_info.iteritems(): 
        (file_size, md5_hash) = file_info
        self._retrieve_file(remote_path, 
                            local_path, 
                            file_size, 
                            md5_hash, 
                            buffer_size,
                            relative_path)
    

  def _send_file(self, remote_path, local_path, buffer_size = 512**2, transmitted = 0, relative_path = None):
    if relative_path:
      r_path = os.path.join(remote_path, relative_path)
    else:
      r_path = remote_path
    f = open(r_path, 'rb')
    if transmitted:
      f.seek(transmitted)
    transfer_ended = False
    while not transfer_ended:
      transfer_ended = self.send_piece(local_path, f.read(buffer_size), relative_path)
    f.close()


  def _retrieve_file(self, remote_path, local_path, file_size, md5_hash, buffer_size = 512**2, relative_path = None):
    if relative_path:
      r_path = os.path.join(os.path.dirname(remote_path), relative_path)
    else:
      r_path = remote_path
    print "copy file to " + repr(r_path)
    f = open(r_path, 'ab')
    fs = f.tell()
    #if fs > file_size:
      #open(r_path, 'wb')
    transfer_ended = False
    while not transfer_ended :
      data = self.retrieve_piece(local_path, buffer_size, fs, relative_path)
      f.write(data)
      fs = f.tell()
      if fs > file_size:
         f.close()
         open(r_path, 'wb')
         raise Exception('retrieve_piece: Transmitted data exceed expected file size.')
      elif fs == file_size:
        if md5_hash is not None:
          if hashlib.md5( open( r_path, 'rb' ).read() ).hexdigest() != md5_hash:
            # Reset file
            f.close()
            open( r_path, 'wb' )
            raise Exception('retrieve_piece: Transmission error detected.')
          else:
            transfer_ended = True
        else:
          transfer_ended = True   
    f.close()

          
  @staticmethod
  def _contents(path_seq, md5_hash=False):
    result = []
    for path in path_seq:
      s = os.stat(path)
      if stat.S_ISDIR(s.st_mode):
        full_path_list = []
        for element in os.listdir(path):
          full_path_list.append(os.path.join(path, element))
        contents = WorkflowController._contents(full_path_list, md5_hash)
        result.append((os.path.basename(path), contents, None))
      else:
        if md5_hash:
          result.append( ( os.path.basename(path), s.st_size, hashlib.md5( open( path, 'rb' ).read() ).hexdigest() ) )
        else:
          result.append( ( os.path.basename(path), s.st_size, None ) )
    return result
        
        
  def initialize_sending_transfer(self, local_path):
    '''
    Initializes the transfer action (from client to server) and returns the transfer action information.
    
    @rtype: tuple 
    @return: in the case of a file transfer: tuple (file_size, md5_hash)
             in the case of a dir transfer: tuple (cumulated_size, dictionary relative path -> (file_size, md5_hash))
    '''
    
    local_path, remote_path, expiration_date, workflow_id, remote_paths = self._engine_proxy.transfer_information(local_path)
    if not remote_paths:
      if os.path.isfile(remote_path):
        stat = os.stat(remote_path)
        file_size = stat.st_size
        md5_hash = hashlib.md5( open( remote_path, 'rb' ).read() ).hexdigest() 
        transfer_action_info = self._engine_proxy.initializeFileSending(local_path, file_size, md5_hash)
      elif os.path.isdir(remote_path):
        full_path_list = []
        for element in os.listdir(remote_path):
          full_path_list.append(os.path.join(remote_path, element))
        contents = WorkflowController._contents(full_path_list)
        transfer_action_info = self._engine_proxy.initializeDirSending(local_path, contents)
    else: #remote_paths
      contents = self._contents(remote_paths)
      transfer_action_info = self._engine_proxy.initializeDirSending(local_path,contents)
    return transfer_action_info
  
        
  def send_piece(self, local_path, data, relative_path=None):
    '''
    Sends a piece of data to a registered transfer (identified by local_path).

    @type  local_path: string
    @param local_path: transfer id
    @type  data: data
    @param data: data to write to the file
    @type  relative_path: relative file path
    @param relative_path: If local_path is a file, relative_path should be None. If local_path is a directory, relative_path is mandatory. 
    @rtype : boolean
    @return: the file transfer ended (=> but not necessarily the transfer associated to local_path)
    '''
    status = self._engine_proxy.transfer_status(local_path)
    if not status == TRANSFERING:
      self.initialize_sending_transfer(local_path)
    transfer_ended = self._engine_proxy.send_piece(local_path, data, relative_path)
    return transfer_ended


  def initialize_retrieving_transfer(self, local_path):
    '''
    Initializes the transfer action (from server to client) and returns the transfer action information.
    
    @rtype: tuple 
    @return: in the case of a file transfer: tuple (file_size, md5_hash)
             in the case of a dir transfer: tuple (cumulated_size, dictionary relative path -> (file_size, md5_hash))
    '''
    
    (transfer_action_info, contents) = self._engine_proxy.initializeRetrivingTransfer(local_path)
    local_path, remote_path, expiration_date, workflow_id, remote_paths = self._engine_proxy.transfer_information(local_path)
    if contents:
      self._create_dir_structure(os.path.dirname(remote_path), contents)
    return transfer_action_info
    

  def _create_dir_structure(self, path, contents, subdirectory = ""):
    if not os.path.isdir(path):
      print "create " + repr(path)
      os.makedirs(path)

    for item, description, md5_hash in contents:
      relative_path = os.path.join(subdirectory,item)
      full_path = os.path.join(path, relative_path)
      if isinstance(description, list):
        print "to create " + repr(full_path)
        if not os.path.isdir(full_path):
          os.mkdir(full_path)
        self._create_dir_structure(path, description, relative_path)
   
   
  def retrieve_piece(self, local_path, buffer_size, transmitted, relative_path=None):
    '''
    Retrieves a piece of data from a file or directory (identified by local_path).
    
    @type  local_path: string
    @param local_path: transfer id
    @type  transmitted: int
    @param transmitted: size of the data already retrieved
    @type  buffer_size: int
    @param buffer_size: size of the piece to retrieve
    @type  relative_path: file path
    @param relative_path: If local_path is a file, relative_path should be None. If local_path is a directory, relative_path is mandatory. 
    @rtype: data
    @return: piece of data read from the file at the position already_transmitted
    '''
    data = self._engine_proxy.retrieve_piece(local_path, buffer_size, transmitted, relative_path)
    return data
    
   
  def delete_transfer(self, local_path):
    '''
    Deletes the local file or directory and the associated transfer information.
    If some jobs reference the local file(s) as input or output, the transfer won't be 
    deleted immediately but as soon as all the jobs will be deleted.
    
    @type local_path: string
    @param local_path: local path associated with a transfer (ie 
    belongs to the list returned by L{transfers}    
    '''
    self._engine_proxy.delete_transfer(local_path)
    

  ########## JOB SUBMISSION ##################################################

  '''
  L{submit_job} method submits a job for execution to the cluster. 
  A job identifier is returned and can be used to inspect and 
  control the job.
  
  Example:
    import soma.workflow.client
      
    wf_controller = soma.workflow.client.WorkflowController()
    job_id = wf_controller.submit_job( ['python', '/somewhere/something.py'] )
    wf_controller.stop_job(job_id)
    wf_controller.restart_job(job_id)
    wf_controller.wait_job([job_id])
    exitinfo = wf_controller.job_termination_status(job_id)
    wf_controller.delete_job(job_id)
  '''

  def submit_job( self,
              command,
              referenced_input_files=None,
              referenced_output_files=None,
              stdin=None,
              join_stderrout=True,
              disposal_timeout=168,
              name_description=None,
              stdout_file=None,
              stderr_file=None,
              working_directory=None,
              parallel_job_info=None,
              queue=None):

    '''
    Submits a job for execution to the cluster. A job identifier is returned and 
    can be used to inspect and control the job.
    If the job used transfered files (L{send} and L{register_transfer} 
    methods) The list of involved local input and output file must be specified to 
    guarantee that the files will exist during the whole job life. 
    
    Every path must be local (ie reachable by the cluster machines) 

    @type  command: sequence of string 
    @param command: The command to execute. Must constain at least one element.
    
    @type  referenced_input_files: sequence of string
    @param referenced_input_files: list of all transfered input files required 
    for the job to run. The files must be local and belong to the list of transfered 
    file L{transfers}
    
    @type  referenced_output_files: sequence of string
    @param referenced_output_files: list of all local files which will be used as 
    output of the job. The files must be local and belong to the list of transfered 
    file L{transfers}
    
    @type  stdin: string
    @param stdin: job's standard input as a path to a file. C{None} if the 
    job doesn't require an input stream.
    
    @type  join_stderrout: bool
    @param join_stderrout: C{True}  if the standard error should be redirect in the 
    same file as the standard output.
   
    @type  disposal_timeout: int
    @param disposal_timeout: Number of hours before the job is considered to have been 
    forgotten by the submitter. Passed that delay, the job is destroyed and its
    resources released (including standard output and error files) as if the 
    user had called L{kill_job} and L{delete_job}.
    Default delay is 168 hours (7 days).
    
    @type  name_description: string
    @param name_description: optional job name or description for user usage only
 
    @type  stdout_file: string
    @param stdout_file: this argument can be set to choose the file where the job's 
    standard output will be redirected. (optional: if it not set the user will still be
    able to read the standard output)
    @type  stderr_file: string 
    @param stderr_file: this argument can be set to choose the file where the job's 
    standard error will be redirected (optional: if it not set the user will still be
    able to read the standard error output). 
    It won't be used if the stdout_file argument is not set. 
    
    @type  working_directory: string
    @param working_directory: his argument can be set to choose the directory where 
    the job will be executed. (optional) 

    @type  parallel_job_info: tuple (string, int)
    @param parallel_job_info: (configuration_name, max_node_num) or None
    This argument must be filled if the job is made to run on several nodes (parallel job). 
    configuration_name: type of parallel job as defined in soma.workflow.constants (eg MPI, OpenMP...)
    max_node_num: maximum node number the job requests (on a unique machine or separated machine
    depending on the parallel configuration)
    !! Warning !!: parallel configurations are not necessarily implemented for every cluster. 
                   This is the only argument that is likely to request a specific implementation 
                   for each cluster/DRMS.

    @type  queue: string
    @param queue: the name of the queue to be used. Default queue if None.
  
    @rtype:   C{JobIdentifier}
    @return:  the identifier of the submitted job 

    '''

    job_id = self._engine_proxy.submit_job(Job(command,
                                    referenced_input_files,
                                    referenced_output_files,
                                    stdin,
                                    join_stderrout,
                                    disposal_timeout,
                                    name_description,
                                    stdout_file,
                                    stderr_file,
                                    working_directory,
                                    parallel_job_info),
                                    queue)
    return job_id
   

  def delete_job( self, job_id ):
    '''
    Frees all the resources allocated to the submitted job on the database 
    server. After this call, the C{job_id} becomes invalid and
    cannot be used anymore. 
    To avoid that jobs create non handled files, L{delete_job} kills the job if 
    it's running.

    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{jobs} or the submission 
    methods L{submit_job}, L{customSubmit} or L{submitWithTransfer})
    '''
    
    self._engine_proxy.delete_job(job_id)
    

  ########## WORKFLOW SUBMISSION ####################################
  
  def submit_workflow(self, 
                      workflow, 
                      expiration_date=None, 
                      name=None, 
                      queue=None):
    '''
    Submits a workflow to the system and returns the id of each 
    submitted workflow element (Job or file transfer).
    
    @type  workflow: L{Workflow}
    @param workflow: workflow description (nodes and node dependencies)
    @type  expiration_date: datetime.datetime
    @type  queue: string
    @param queue: the name of the queue to be used. Default queue if None.
    @rtype: L{WorkflowIdentifier}
    @return: workflow id
    '''

    wf_id =  self._engine_proxy.submit_workflow(workflow, 
                                                expiration_date, 
                                                name,
                                                queue)
    return wf_id
  
  
  def delete_workflow(self, workflow_id):
    '''
    Removes a workflow and all its associated nodes (file transfers and jobs)
    '''
    self._engine_proxy.delete_workflow(workflow_id)
    
  def change_workflow_expiration_date(self, workflow_id, new_expiration_date):
    '''
    Ask a new expiration date for the workflow.
    Return True if the workflow expiration date was set to the new date.
        
    @type  workflow_id: C{WorkflowIdentifier}
    @type  expiration_date: datetime.datetime
    @rtype: boolean
    '''
    return self._engine_proxy.change_workflow_expiration_date(workflow_id, new_expiration_date)
  
  def restart_workflow(self, workflow_id):
    '''
    The jobs which failed in the previous submission will be submitted again.
    The workflow execution must be done.
    Return true if the workflow was resubmitted.
    '''
    return self._engine_proxy.restart_workflow(workflow_id)
    

  ########## MONITORING #############################################


  def jobs(self, job_ids=None):
    '''
    Submitted jobs which are not part of a workflow.
    If a sequence of job id is given, the function returns general information 
    about these jobs.
    Returns a dictionary of job identifier associated to general information.

    @rtype:  dictionary: job identifiers -> tuple (string, string, date))
    @return: job_id -> (name/description, command, submission date)
    '''
    
    return self._engine_proxy.jobs(job_ids)
    
  def transfers(self, transfer_ids=None):
    '''
    Transfers which are not part of a workflow.
    If a sequence of transfer id is given, the function returns general 
    information about these transfers.
    Returns a dictionary of transfer identifiers associated to general 
    information.
    
    @type  s
    @rtype: dictionary: string -> tuple(string, 
                                        string, 
                                        date,  
                                        None or sequence of string)
    @return: transfer_id -> ( -remote_path: remote file or directory path
                              -expiration_date: after this date the local file 
                              will be deleted, unless an existing job has 
                              declared this file as output or input.
                              -remote_paths: sequence of file or directory path 
                              or None
                            )
    '''
    return self._engine_proxy.transfers(transfer_ids)

  def workflows(self, workflow_ids=None):
    '''
    Workflows submitted. 
    If a sequence of workflow id is given, the function returns general 
    information about these workflows.
    Returns a dictionary of workflow identifiers associated to general 
    information.
    
    @rtype: dictionary: workflow identifier -> tupe(date, string)
    @return: workflow_id -> (workflow_name, expiration_date)
    '''
    return self._engine_proxy.workflows(workflow_ids)
  
  
  def workflow(self, wf_id):
    '''
    Returns the submitted workflow.
    
    @rtype: L{Workflow}
    @return: submitted workflow
    '''
    return self._engine_proxy.workflow(wf_id)
   
   
  def job_status( self, job_id ):
    '''
    Returns the status of a submitted job.
    
    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{submit_job} or L{jobs})
    @rtype:  C{JobStatus} or None
    @return: the status of the job, if its valid and own by the current user, None 
    otherwise. See the list of status: constants.JOB_STATUS.
    '''
    return self._engine_proxy.job_status(job_id)
  
  
  def workflow_status(self, wf_id):
    '''
    Returns the status of the submitted workflow.
    
    @type  wf_id: Workflow identifier
    @param wf_id: The workflow identifier.
    @rtype: string
    @return: the workflow status, if its valid and own by the current use, None
    otherwise. See the list of status: constants.WORKFLOW_STATUS
    '''
    return self._engine_proxy.workflow_status(wf_id)
  
  
  def workflow_nodes_status(self, wf_id, group = None):
    '''
    Gets back the status of all the workflow elements at once, minimizing the
    communication with the server and requests to the database.
    
    @type  wf_id: C{WorflowIdentifier}
    @param wf_id: The workflow identifier
    @rtype: tuple (sequence of tuple (job_id, status, exit_info, (submission_date, execution_date, ending_date)), sequence of tuple (transfer_id, (status, progression_info)), workflow_status)
    '''
    wf_status = self._engine_proxy.workflow_nodes_status(wf_id)
    if not wf_status:
      # TBI raise ...
      return
     # special processing for transfer status:
    new_transfer_status = []
    for local_path, remote_path, status, transfer_action_info in wf_status[1]:
      progression = self._transfer_progress(local_path, remote_path, transfer_action_info)
      new_transfer_status.append((local_path, (status, progression)))
      
    new_wf_status = (wf_status[0],new_transfer_status, wf_status[2])
    return new_wf_status
    
  
  def transfer_status(self, local_path):
    '''
    Returns the status of a transfer and the information related to the transfer in progress in such case. 
    
    @type  local_path: string
    @param local_path: 
    @rtype: tuple  (C{transfer_status} or None, tuple or None)
    @return: [0] the transfer status among constants.FILE_TRANSFER_STATUS
             [1] None if the transfer status in not constants.TRANSFERING
                 if it's a file transfer: tuple (file size, size already transfered)
                 if it's a directory transfer: tuple (cumulated size, sequence of tuple (relative_path, file_size, size already transfered)
    '''
    
    status = self._engine_proxy.transfer_status(local_path)
    transfer_action_info =  self._engine_proxy.transfer_action_info(local_path)
    local_path, remote_path, expiration_date, workflow_id, remote_paths = self._engine_proxy.transfer_information(local_path)
    progression = self._transfer_progress(local_path, remote_path, transfer_action_info)
    return (status, progression)
      
            
            
  def _transfer_progress(self, local_path, remote_path, transfer_action_info):
    progression_info = None
    if transfer_action_info == None :
      progression_info = None
    elif transfer_action_info[2] == FILE_SENDING or transfer_action_info[2] == DIR_SENDING:
      progression_info = self._engine_proxy.transfer_progression_status(local_path, transfer_action_info)
    elif transfer_action_info[2] == FILE_RETRIEVING or transfer_action_info[2] == DIR_RETRIEVING:
      if transfer_action_info[2] == FILE_RETRIEVING:
        (file_size, md5_hash, transfer_type) = transfer_action_info
        if os.path.isfile(remote_path):
          transmitted = os.stat(remote_path).st_size
        else:
          transmitted = 0
        progression_info = (file_size, transmitted)
      elif transfer_action_info[2] == DIR_RETRIEVING:
        (cumulated_file_size, file_transfer_info, transfer_type) = transfer_action_info
        files_transfer_status = []
        for relative_path, (file_size, md5_hash) in file_transfer_info.iteritems():
          full_path = os.path.join(os.path.dirname(remote_path), relative_path)
          if os.path.isfile(full_path):
            transmitted = os.stat(full_path).st_size
          else:
            transmitted = 0
          files_transfer_status.append((relative_path, file_size, transmitted))
        cumulated_transmissions = reduce( operator.add, (i[2] for i in files_transfer_status) )
        progression_info = (cumulated_file_size, cumulated_transmissions, files_transfer_status)
       
    return progression_info
    

  def job_termination_status(self, job_id ):
    '''
    Gives the information related to the end of the job.
   
    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{submit_job} or L{jobs})
    @rtype:  tuple (exit_status, exit_value, term_signal, resource_usage) or None
    @return: It may be C{None} if the job is not valid. 
        - exit_status: The status of the terminated job. See the list of status
                       constants.JOB_EXIT_STATUS
        - exit_value: operating system exit code if the job terminated normally.
        - term_signal: representation of the signal that caused the termination of 
          the  job if the job terminated due to the receipt of a signal.
        - resource_usage: resource usage information as given by the cluser 
          distributed resource management system (DRMS).
    '''
    return self._engine_proxy.job_termination_status(job_id)
    
    
  def retrieve_job_stdouterr(self, job_id, stdout_file_path, stderr_file_path = None, buffer_size = 512**2):
    '''
    Copy the job standard error to a file.
    '''
    
    local_stdout_file, stdout_transfer_action_info, local_stderr_file, stderr_transfer_action_info = self._engine_proxy.stdouterr_transfer_action_info(job_id)
    
    open(stdout_file_path, 'wb') 
    if local_stdout_file and stdout_transfer_action_info:
      self._retrieve_file(stdout_file_path, 
                          local_stdout_file, 
                          stdout_transfer_action_info[0], 
                          stdout_transfer_action_info[1], 
                          buffer_size)
  
    if stderr_file_path:
      open(stderr_file_path, 'wb') 
      if local_stderr_file and stderr_transfer_action_info:
          self._retrieve_file(stderr_file_path, 
                              local_stderr_file, 
                              stderr_transfer_action_info[0], 
                              stderr_transfer_action_info[1], 
                              buffer_size)
    
    
  ########## JOB CONTROL VIA DRMS ########################################
  
  
  def wait_job( self, job_ids, timeout = -1):
    '''
    Waits for all the specified jobs to finish execution or fail. 
    The job_id must be valid.
    
    @type  job_ids: set of C{JobIdentifier}
    @param job_ids: Set of jobs to wait for
    @type  timeout: int
    @param timeout: the call exits before timout seconds. a negative value 
    means to wait indefinetely for the result. 0 means to return immediately
    '''
    self._engine_proxy.wait_job(job_ids, timeout)

  def kill_job( self, job_id ):
    '''
    Kill the job execution, the job won't be deleted of the database server. 
    Use the L{restart_job} method to restart the job.
    The job_id must be valid.
    
    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{submit_job} or L{jobs})
    '''
    self._engine_proxy.kill_job(job_id)
   
  
  def restart_job( self, job_id ):
    '''
    Restarts a job which status is constants.FAILED.
    The job_id must be valid.
    
    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{submit_job} or L{jobs})
    '''
    self._engine_proxy.restart_job(job_id)


    
    
    
