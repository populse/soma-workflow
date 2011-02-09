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

The other classes (Workflow, Job, Group, FileTransfer and SharedResourcePath) are made to build the jobs, workflows, and file transfers 
objects to be used in the WorkflowControler interface.

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
  Job representation.
  
  .. note::
    The command is the only argument required to create a Job.
    It is also useful to fill the job name for the workflow display in the GUI.

  **command**: *sequence of string or/and FileTransfer or/and SharedResourcePath or/and tuple (relative_path, FileTransfer) or/and sequence of FileTransfer or/and sequence of SharedResourcePath or/and sequence of tuple (relative_path, FileTransfers.)*
    
    The command to execute. It can not be empty. In case of a shared file system 
    the command is a sequence of string. 

    In the other cases, the FileTransfer and SharedResourcePath objects will be 
    replaced by the appropriate path before the job execution. 

    The tuples (relative_path, FileTransfer) can be used to refer to a file in a 
    transfered directory.

    The sequences of FileTransfer, SharedResourcePath or tuple (relative_path, 
    FileTransfer) will be replaced by the string "['path1', 'path2', 'path3']" 
    before the job execution. The FileTransfer, SharedResourcePath or tuple
    (relative_path, FileTransfer) are replaced by the appropriate path inside 
    the sequence.

  **name**: *string*
    Name of the Job which will be displayed in the GUI
  
  **referenced_input_files**: *sequence of FileTransfer*
    List of the FileTransfer which are input of the Job. In other words, 
    FileTransfer which are requiered by the Job to run. It includes the
    stdin if you use one.

  **referenced_output_files**: *sequence of FileTransfer*
    List of the FileTransfer which are output of the Job. In other words, the 
    FileTransfer which will be created or modified by the Job.

  **stdin**: *string or FileTransfer or SharedRessourcePath*
    Path to the file which will be read as input stream by the Job.
  
  **join_stderrout**: *boolean*
    Specifies whether the error stream should be mixed with the output stream.

  **stdout_file**: *string or FileTransfer or SharedRessourcePath*
    Path of the file where the standard output stream of the job will be 
    redirected.

  **stderr_file**: *string or FileTransfer or SharedRessourcePath*
    Path of the file where the standard error stream of the job will be 
    redirected.

  .. note::
    Set stdout_file and stderr_file only if you need to redirect the standard 
    output to a specific file. Indeed, even if they are not set the standard
    outputs will always be available through the WorklfowController API.

  **working_directory**: *string or FileTransfer or SharedRessourcePath*
    Path of the directory where the job will be executed. The working directory
    is useful if your Job uses relative file path for example.

  **parallel_job_info**: *tuple(string, int)*
    The parallel job information must be set if the Job is parallel (ie. made to 
    run on several CPU).
    The parallel job information is a tuple: (name of the configuration, 
    maximum number of CPU used by the Job).
    The configuration name is the type of parallel Job. Example: MPI or OpenMP.

  .. warning::
    The computing resources must be configured explicitly to use this feature.

  ..
    **disposal_time_out**: int
    Only requiered outside of a workflow

  '''

  # sequence of sequence of string or/and FileTransfer or/and SharedResourcePath or/and tuple (relative_path, FileTransfer) or/and sequence of FileTransfer or/and sequence of SharedResourcePath or/and sequence of tuple (relative_path, FileTransfers.)
  command = None

  # string
  name = None

  # sequence of FileTransfer
  referenced_input_files = None
  
  # sequence of FileTransfer
  referenced_output_files = None
 
  # string (path)
  stdin = None
  
  # boolean
  join_stderrout = None

  # string (path)
  stdout_file = None
 
  # string (path)
  stderr_file = None
 
  # string (path)
  working_directory = None
  
  # tuple(string, int)
  parallel_job_info = None

  # int (in hours)
  disposal_timeout = None
 
  def __init__( self, 
                command,
                referenced_input_files=None,
                referenced_output_files=None,
                stdin=None,
                join_stderrout=False,
                disposal_timeout=168,
                name=None,
                stdout_file=None,
                stderr_file=None,
                working_directory=None,
                parallel_job_info=None):
    if not name:
      self.name = command[0]
    else:
      self.name = name
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
    self.stdout_file = stdout_file
    self.stderr_file = stderr_file
    self.working_directory = working_directory
    self.parallel_job_info = parallel_job_info


class Workflow(object):
  '''
  Workflow representation.

  **jobs**: *sequence of Job*
    Workflow jobs.

  **dependencies**: *sequence of tuple (Job, Job)*
    Dependencies between the jobs of the workflow.
    If a job_a needs to be executed before a job_b can run: the tuple 
    (job_a, job_b) must be added to the workflow dependencies. job_a and job_b
    must belong to workflow.jobs.

  **name**: *string*
    Name of the workflow which will be displayed in the GUI. 
    Default: workflow_id once submitted

  **root_group**: *sequence of Job and/or Group*
    Recursive description of the workflow hierarchical structure. For displaying
    purpose only.

  .. note::
    root_group is only used to display nicely the workflow in the GUI. It
    does not have any impact on the workflow execution.
    
    If root_group is not set, all the jobs of the workflow will be 
    displayed at the root level in the GUI tree view.
  '''
  # string
  name = None

  # sequence of Job
  jobs = None

  # sequence of tuple (Job, Job)
  dependencies = None

  # sequence of Job and/or Group 
  root_group = None

  # sequence of Groups built from the root_group
  groups = None

  def __init__(self, 
               jobs, 
               dependencies, 
               root_group = None,
               disposal_timeout = 168,):
    '''
    TOTO
    '''
    self.name = None
    self.jobs = jobs
    self.dependencies = dependencies
    self.disposal_timeout = 168
    
    # Groups
    if root_group:
      if isinstance(root_group, Group):
        self.root_group = root_group.elements
      else:
        self.root_group = root_group
      self.groups = []
      to_explore = []
      for element in self.root_group:
        if isinstance(element, Group):
          to_explore.append(element)
      while to_explore:
        group = to_explore.pop()
        self.groups.append(group)
        for element in group.elements:
          if isinstance(element, Group):
            to_explore.append(element)
    else:
      self.root_group = self.jobs
      self.groups = []


  
class Group(object):
  '''
  Hierarchical structure of a workflow.

  .. note:
    It only has a displaying role and does not have any impact on the workflow 
    execution.

  **name**: *string*
    Name of the Group which will be displayed in the GUI.

  **elements**: *sequence of Job and/or Group*
    The elements (Job or Group) belonging to the group.
  '''
  #string
  name = None

  #sequence of Job and/or Group
  elements = None
  
  def __init__(self, elements, name):
    '''
    @type  elements: sequence of Job and/or Group
    @param elements: the elements belonging to the group
    @type  name: string
    @param name: name of the group
    '''
    self.elements = elements
    self.name = name


class FileTransfer(object):
  '''
  File/directory transfer representation
  
  .. note::
    FileTransfers objects are only required if the user and computing resources
    have a separate file system.
  
  **client_path**: *string*
    Path of the file or directory on the user's file system.

  **initial_status**: *constants.FILES_DO_NOT_EXIST or constants.FILES_ON_CLIENT*
    * constants.FILES_ON_CLIENT for workflow input files
      The file(s) will need to be transfered on the computing resource side
    * constants.FILES_DO_NOT_EXIST for workflow output files
      The file(s) will be created by a job on the computing resource side.

  **client_paths**: *sequence of string*
    Sequence of path. Files to transfer if the FileTransfers concerns a file
    series or if the file format involves several associated files or
    directories (see the note below).

  **name**: *string*
    Name of the FileTransfer which will be displayed in the GUI. 
    Default: client_path + "transfer"
 
  .. note::
    Use client_paths if the transfer involves several associated files and/or 
    directories. Examples:

      * file series
      * file format associating several file and/or directories 
        (ex: a SPM images are stored in 2 associated files: .img and .hdr)
        In this case, set client_path to one the files (ex: .img) and 
        client_paths contains all the files (ex: .img and .hdr files)

     In other cases (1 file or 1 directory) the client_paths must be set to None.
  '''

  # string
  client_path = None

  # sequence of string
  client_paths = None

  # constants.FILES_DO_NOT_EXIST constants.FILES_ON_CLIENT
  initial_status = None

  # int (hours)
  disposal_timeout = None

  # string
  name = None

  def __init__( self,
                is_input,
                client_path, 
                disposal_timeout = 168,
                name = None,
                client_paths = None,
                ):
    if name:
      ft_name = name
    else:
      ft_name = client_path + "transfer"
    self.name = ft_name

    self.client_path = client_path
    self.disposal_timeout = disposal_timeout

    self.client_paths = client_paths
  
    if is_input:
      self.initial_status = FILES_ON_CLIENT
    else:
      self.initial_status = FILES_DO_NOT_EXIST




class SharedResourcePath(object):
  '''
  Representation of path which is valid on either user's or computing resource 
  file system.

  .. note::
    SharedResourcePath objects are only required if the user and computing
    resources have a separate file system.

  **namespace**: *string*
    Namespace for the path. That way several applications can use the same 
    identifiers without risk.

  **uuid**: *string*
    Identifier of the absolute path. 

  **relative_path**: *string*
    Relative path of the file if the absolute path is a directory path. 

  .. warning::
    The namespace and uuid must exist in the translations files configured on 
    the computing resource side.  
  '''
  
  relative_path = None
  
  namespace = None

  uuid = None

  def __init__(self,
               relative_path,
               namespace,
               uuid,
               disposal_timeout = 168):
    self.relative_path = relative_path
    self.namespace = namespace
    self.uuid = uuid
    self.disposal_timout = disposal_timeout





class WorkflowController(object):
  '''
  Submission, control and monitoring of Job, FileTransfer and Workflow 
  objects.
  '''
  def __init__(self, 
               resource_id, 
               login=None, 
               password=None,
               config_path=None):
    '''
    Searchs for a soma-workflow configuration file (if not specified in the 
    *config_file_path* argument) and sets up the connection to the computing 
    resource. 

    **resource_id**: *string*
      Identifier of the computing resource to connect to.

    **login**: *string*

    **password**: *string*

    **config_path**: *string*
      Optional path to the configuration file.

    .. note::
      The login and password are only required for a remote computing 
      resource.
    '''
    
    
    #########################
    # reading configuration 
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
      raise Exception("Can not find the soma-workflow configuration file \n")
    
    print "Configuration file toto: " + repr(config_path)
    config = ConfigParser.ConfigParser()
    config.read(config_path)
    self.resource_id = resource_id
    self.config = config
   
    if not config.has_section(resource_id):
      raise Exception("Can not find section " + resource_id + " in configuration file: " + config_path)

    submitting_machines = config.get(resource_id, CFG_SUBMITTING_MACHINES).split()
    cluster_address = config.get(resource_id, CFG_CLUSTER_ADDRESS)
    hostname = socket.gethostname()
    mode = 'remote'
    for machine in submitting_machines:
      if hostname == machine: mode = 'local'
    print "hostname: " + hostname + " => mode = " + mode

    log = ""
    #########################
    # Connection
    self._mode = mode #'local_no_disconnection' # (local debug)#        

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
        logger.critical('Could not find' + server_name + ' nameserver says:',x)
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
            logger.info("Could not read the translation file: " + filename)
          else:
            if not namespace in path_translation.keys():
              path_translation[namespace] = {}
            line = f.readline()
            while line:
              splitted_line = line.split(None,1)
              if len(splitted_line) > 1:
                uuid = splitted_line[0]
                content = splitted_line[1].rstrip()
                logger.info("    uuid: " + uuid + "   translation:" + content)
                path_translation[namespace][uuid] = content
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
    !!! The current instance will not be usable anymore after this call !!!!
    '''
    self._connection.stop()

   

  ########## SUBMISSION / REGISTRATION ####################################
  
  def submit_workflow(self, 
                      workflow, 
                      expiration_date=None, 
                      name=None, 
                      queue=None):
    '''

    Submits a workflow to the system and returns a workflow identifier.
    
    **workflow**: *client.Workflow*
      Workflow descrition.
    
    **expiration_date**: *datetime.datetime*
      After this date the workflow will be deleted from the system.

    **name**: *string*
      Optional workflow name.

    **queue**: *string*
      Optional name of the queue where to submit jobs. If it is not specified
      the jobs will be submitted to the default queue.

    **returns**: int
      Workflow identifier.
    '''

    wf_id =  self._engine_proxy.submit_workflow(workflow, 
                                                expiration_date, 
                                                name,
                                                queue)
    return wf_id
  

  '''
  L{submit_job} method submits a job for execution to the cluster. 
  A job identifier is returned and can be used to inspect and 
  control the job.
  
  Example:
    import soma.workflow.client
      
    wf_controller = soma.workflow.client.WorkflowController("Titan")
    job_id = wf_controller.submit_job( ['python', '/somewhere/something.py'] )
    wf_controller.kill_job(job_id)
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
                  name=None,
                  stdout_file=None,
                  stderr_file=None,
                  working_directory=None,
                  parallel_job_info=None,
                  queue=None):

    '''
    Submits a job **which is not part of a workflow** to the system.
    Returns a job identifier.

    If the job used transfered files the list of involved file transfer **must 
    be** specified setting the arguments: *referenced_input_files* and 
    *referenced_output_files*.
    
    Each path must be reachable from the computing resource 

    .. note::
      The command is the only argument required to create a Job.

    **command**: *sequence of string*
      The command to execute. It must contain at least one element.

    **referenced_input_files**: *sequence of FileTransfer idenfier*
      List of the FileTransfer identifiers which are input of the Job. 
      In other words, FileTransfer which are requiered by the Job to run
      It includes the stdin if you use one.

    **referenced_output_files**: *sequence of FileTransfer idenfier*
      List of the FileTransfer identifiers which are output of the Job. 
      In other words, FileTransfer  which will be created or modified by the Job.
    
    **stdin**: *path or FileTransfer identifier* 
      Path to the file which will be read as input stream by the Job.

    **join_stderrout**: *boolean*
      Specifies whether the error stream should be mixed with the output stream.

    **stdout_file**: *path or FileTransfer identifier* 
      Path of the file where the standard output stream of the job will be 
      redirected.

    **stderr_file**: *path or FileTransfer identifier* 
      Path of the file where the standard error stream of the job will be 
      redirected.

    .. note::
      Set stdout_file and stderr_file only if you need to redirect the standard 
      output to a specific file. Indeed, even if they are not set the standard
      outputs will always be available through the WorklfowController API.
    
    **disposal_timeout**: *int*
      Number of hours before the Job will be deleted. Passed that delay, the job
      will deleted and its resources released (including standard output and 
      error files). 
      The default timeout is 168 hours (7 days).

    **name**: *string*
      Name of the job.
    
    **working_directory**: *path*
       Path of the directory where the job will be executed. The working directory is useful if your Job uses relative file path for example.

    **parallel_job_info**: *tuple(string, int)*
      The parallel job information must be set if the Job is parallel (ie. made to 
      run on several CPU).
      The parallel job information is a tuple: (name of the configuration, 
      maximum number of CPU used by the Job).
      The configuration name is the type of parallel Job. Example: MPI or 
      OpenMP.

      .. warning::
        The computing resources must be configured explicitly to use this 
        feature.

    **queue**: *string*
      Name of the queue where to submit the jobs. If it is not 
      specified the job will be submitted to the default queue.

    **returns**: int
      Job identifier.
    '''

    job_id = self._engine_proxy.submit_job(Job(command,
                                    referenced_input_files,
                                    referenced_output_files,
                                    stdin,
                                    join_stderrout,
                                    disposal_timeout,
                                    name,
                                    stdout_file,
                                    stderr_file,
                                    working_directory,
                                    parallel_job_info),
                                    queue)
    return job_id
   

  def register_transfer(self, 
                        is_input,
                        client_path, 
                        disposal_timeout=168, 
                        name = None,
                        client_paths=None): 
    '''
    Registers a file transfer **which is not part of a workflow** and returns a 
    file transfer identifier.

    **is_input**: *boolean*
      True if the files are input files existing on the client side.
      False if the files are output files which will be created by a job on 
      the computing resource side.

    **client_path**: *string*
      Path of the file or directory on the user's file system.

    **disposal_timeout**: *int*
      Number of hours before the file transfer will be deleted. Passed that 
      delay, files copied on the computing resource side and
      all the transfer information will be deleted, except if a job still 
      references it as output or input.
      The default timeout is 168 hours (7 days).

    **client_paths**: *sequence of string*
       Sequence of path. Files to transfer if the FileTransfers concerns a file
       series or if the file format involves several associated files or
       directories (see the note below).

    **name**: *string*
      Name of the file transfer. Default: client_path + "transfer"

    **returns**: *string*
      File transfer identifier.

    .. note::
      Use client_paths if the transfer involves several associated files and/or 
      directories. Examples:

        * file series
        * file format associating several file and/or directories 
          (ex: a SPM images are stored in 2 associated files: .img and .hdr)
          In this case, set client_path to one the files (ex: .img) and 
          client_paths contains all the files (ex: .img and .hdr files)

      In other cases (1 file or 1 directory) the client_paths must be set to None.
    '''
    return self._engine_proxy.register_transfer(FileTransfer(is_input,
                                                      client_path,
                                                      disposal_timeout, 
                                                      name,
                                                      client_paths))


  ########## FILE TRANSFER ###############################################
    
  '''
  The file transfer methods must be used when the data is located on the
  client machine and not reachable from the computing resource
  
  Example of a Job submission with file transfer outside of a workflow:
  
  #job client input files path on client: cfin_1, cfin_2, ..., rfin_n 
  #job client output files path on: cfout_1, rfout_2, ..., rfout_m  
  
  #Call register_transfer for each transfer file and get back the transfer id:
  in_1_trid= wf_controller.register_transfer(True, rfin_1)
  in_2_trid= wf_controller.register_transfer(True, rfin_2)
  ...
  in_n_trid= wf_controller.register_transfer(True, rfin_n)

  out_1_trid = wf_controller.register_transfer(False, cfout_1)
  out_2_trid = wf_controller.register_transfer(False, cfout_2)
  ...
  out_n_trid = wf_controller.register_transfer(False, cfout_m)
  
  # Transfer input files:
  wf_controller.transfer_files(in_1_trid)
  wf_controller.transfer_files(in_2_trid)
  ...
  wf_controller.transfer_files(in_n_trid)
    
  #Job submittion: 
  # use the transfer id in the command or stdin argument when needed
  # do not forget to reference input and output file transfers
  job_id = wf_controller.submit_job(['python', trid_in_1], 
                                    [in_1_trid, in_2_trid, ..., in_n_trid],
                                    [out_1_trid, out_2_trid, ..., out_n_trid])
  wf_controller.wait_job(job_id)
  
  #After Job execution, transfer back the output file
  wf_controller.transfer_files(out_1_trid)
  wf_controller.transfer_files(out_2_trid)
  ...
  wf_controller.transfer_files(out_n_trid)
  
  Use the FileTransfer client_paths attribute if the transfer involves several 
  associated files and/or directories:
          - when transfering a file serie 
          - in the case of file format associating several file and/or directories
            (ex: a SPM image is stored in 2 files: .img and .hdr)
  In this case, set client_path to one the files (eq: .img).
  In other cases (1 file or 1 directory) the client_paths must be set to None.
  
  Example:
    
  #transfer of a SPM image file
  fout_1 = wf_controller.register_transfer(client_path = 'mypath/myimage.img', 
                                 client_paths = ['mypath/myimage.img', 'mypath/myimage.hdr'])
  ...
  wf_controller.transfer_files(fout_1)
 
  '''
        
 

  def transfer_files(self, transfer_id, buffer_size = 512**2):
    '''
    Does the actual file(s) transfer.
    If the files are only located on the client side (transfer status: constants.FILES_ON_CLIENT) the transfer is done from the client to 
    the computing resource.
    If the files are localted on the computing reource side (transfer status:
    constants.FILES_ON_CR or constants.FILES_ON_CLIENT_AND_CR) the transfer is 
    done from the computing resouce to the client.
    The files are transfered piece by piece. The size of each piece can be
    tuned using the buffer_size argument.

    @rtype: Boolean
    @return: the transfer was done
    '''

    status, status_info = self.transfer_status(transfer_id)
    transfer_id, client_path, expiration_date, workflow_id, client_paths = self._engine_proxy.transfer_information(transfer_id)

    transfer_action_info = self._engine_proxy.transfer_action_info(transfer_id)

    if status == FILES_ON_CLIENT or \
       status == TRANSFERING_FROM_CLIENT_TO_CR:
      # transfer from client to computing resource
      transfer_from_scratch = False
      if not transfer_action_info or \
         transfer_action_info[2] == TR_FILE_CR_TO_C or \
         transfer_action_info[2] == TR_DIR_CR_TO_C or \
         transfer_action_info[2] == TR_MFF_CR_TO_C:
        # transfer reset
        transfer_from_scratch = True
        transfer_action_info = self.initialize_transfer(transfer_id)
      
      if transfer_action_info[2] == TR_FILE_C_TO_CR:
        if not transfer_from_scratch:
          transmitted = status_info[2]
          #(file_size, transmitted) = self._engine_proxy.transfer_progression_status(engine_path, transfer_action_info)
        else:
          transmitted = 0
        self._transfer_file_to_cr(client_path,
                                  transfer_id,
                                  buffer_size,
                                  transmitted)
        return True

      if transfer_action_info[2] == TR_DIR_C_TO_CR:
        for relative_path in transfer_action_info[1]:
          transfer_ended = False 
          if transfer_from_scratch:
              transmitted = 0
          else:
            for r_path, f_size, trans in status_info[2]:
              if r_path == relative_path:
                transmitted = trans
                transfer_ended = f_size == trans
                break
          if not transfer_ended:
            self._transfer_file_to_cr(client_path, 
                            transfer_id, 
                            buffer_size,
                            transmitted=transmitted,
                            relative_path=relative_path)
        return True

      if transfer_action_info[2] == TR_MFF_C_TO_CR:
        for relative_path in transfer_action_info[1]:
          transfer_ended = False 
          if transfer_from_scratch:
              transmitted = 0
          else:
            for r_path, f_size, trans in status_info[2]:
              if r_path == relative_path:
                transmitted = trans
                transfer_ended = f_size == trans
                break
          if not transfer_ended:
            self._transfer_file_to_cr(os.path.dirname(client_path), 
                                      transfer_id, 
                                      buffer_size, 
                                      transmitted=transmitted,
                                      relative_path=relative_path)
        return True

      return False

    if status == FILES_ON_CR or \
       status == TRANSFERING_FROM_CR_TO_CLIENT or \
       status == FILES_ON_CLIENT_AND_CR:
      # transfer from computing resource to client
      transfer_from_scratch = False
      if not transfer_action_info or \
         transfer_action_info[2] == TR_FILE_C_TO_CR or \
         transfer_action_info[2] == TR_DIR_C_TO_CR or \
         transfer_action_info[2] == TR_MFF_C_TO_CR :
        transfer_action_info = self.initialize_transfer(transfer_id)
        transfer_from_scratch = True
        # TBI remove existing files 

      if transfer_action_info[2] == TR_FILE_CR_TO_C:
        # file case
        (file_size, md5_hash, transfer_type) = transfer_action_info
        self._transfer_file_from_cr(client_path, 
                                    transfer_id, 
                                    file_size, 
                                    md5_hash, 
                                    buffer_size)
        return True
      if transfer_action_info[2] == TR_DIR_CR_TO_C:
        (cumulated_file_size, file_transfer_info, transfer_type) = transfer_action_info
        for relative_path, file_info in file_transfer_info.iteritems(): 
          (file_size, md5_hash) = file_info
          self._transfer_file_from_cr(client_path, 
                                      transfer_id, 
                                      file_size, 
                                      md5_hash, 
                                      buffer_size,
                                      relative_path)

        return True
      if transfer_action_info[2] == TR_MFF_CR_TO_C:
        (cumulated_file_size, file_transfer_info, transfer_type) = transfer_action_info
        for relative_path, file_info in file_transfer_info.iteritems(): 
          (file_size, md5_hash) = file_info
          self._transfer_file_from_cr(os.path.dirname(client_path), 
                                      transfer_id, 
                                      file_size, 
                                      md5_hash, 
                                      buffer_size,
                                      relative_path)
        return True 

      return False
    

  def _transfer_file_to_cr(self, 
                           client_path, 
                           engine_path, 
                           buffer_size = 512**2, 
                           transmitted = 0, 
                           relative_path = None):
    '''
    Transfer a file from the client to the computing resource.

    @param client_path: file path on the client side
    @param engine_path: file path on the computing resource side
    @param buffer_size: the file is transfered piece by piece of size buffer_size
    @param transmitted: size already transfered
    '''
      
    if relative_path:
      r_path = os.path.join(client_path, relative_path)
    else:
      r_path = client_path
    f = open(r_path, 'rb')
    if transmitted:
      f.seek(transmitted)
    transfer_ended = False
    while not transfer_ended:
      transfer_ended = self.write_to_computing_resource_file(engine_path,   
                                                       f.read(buffer_size),
                                                       relative_path)
    f.close()


  def _transfer_file_from_cr(self, 
                             client_path, 
                             engine_path, 
                             file_size, 
                             md5_hash, 
                             buffer_size = 512**2, 
                             relative_path = None):
    '''
    Transfer a file from the computing resource to client side.

    @param client_path: file path on the client side
    @param engine_path: file path on the computing resource side
    @param buffer_size: the file is transfered piece by piece of size buffer_size
    @param transmitted: size already transfered
    '''
    if relative_path:
      r_path = os.path.join(client_path, relative_path)
    else:
      r_path = client_path
    print "copy file to " + repr(r_path)
    f = open(r_path, 'ab')
    fs = f.tell()
    #if fs > file_size:
      #open(r_path, 'wb')
    data = self.read_from_computing_resource_file(engine_path, 
                                                  buffer_size, 
                                                  fs, 
                                                  relative_path)
    f.write(data)
    fs = f.tell()
    while data:
      data = self.read_from_computing_resource_file(engine_path, 
                                                    buffer_size, 
                                                    fs, 
                                                    relative_path)
      f.write(data)
      fs = f.tell()

    if fs > file_size:
        f.close()
        open(r_path, 'wb')
        raise Exception('read_from_computing_resource_file: Transmitted data exceed expected file size.')
    else:
      if md5_hash is not None and \
         hashlib.md5( open( r_path, 'rb' ).read() ).hexdigest() != md5_hash:
          # Reset file
          f.close()
          open( r_path, 'wb' )
          raise Exception('read_from_computing_resource_file: Transmission error detected.')
    f.close()

  
  def initialize_transfer(self, transfer_id):
    '''
    Initializes the transfer and returns the transfer action information.

    @rtype: tuple 
    @return: in the case of a file transfer: tuple (file_size, md5_hash)
             in the case of a dir transfer: tuple (cumulated_size, dictionary relative path -> (file_size, md5_hash))
    '''
    status = self._engine_proxy.transfer_status(transfer_id)
    transfer_id, client_path, expiration_date, workflow_id, client_paths = self._engine_proxy.transfer_information(transfer_id)

    if status == FILES_ON_CLIENT:
      if not client_paths:
        if os.path.isfile(client_path):
          stat = os.stat(client_path)
          file_size = stat.st_size
          md5_hash = hashlib.md5( open( client_path, 'rb' ).read() ).hexdigest() 
          transfer_action_info = self._engine_proxy.init_file_transfer_to_cr(transfer_id, 
                                                      file_size, 
                                                      md5_hash)
        elif os.path.isdir(client_path):
          full_path_list = []
          for element in os.listdir(client_path):
            full_path_list.append(os.path.join(client_path, element))
          content = WorkflowController.dir_content(full_path_list)
          transfer_action_info = self._engine_proxy.init_dir_transfer_to_cr(transfer_id, 
                                                     content,
                                                     TR_DIR_C_TO_CR)

      else: #client_paths
        content = WorkflowController.dir_content(client_paths)
        transfer_action_info = self._engine_proxy.init_dir_transfer_to_cr(transfer_id,
                                                   content,
                                                   TR_MFF_C_TO_CR)
      return transfer_action_info
    elif status == FILES_ON_CR or FILES_ON_CLIENT_AND_CR:
      (transfer_action_info, dir_content) = self._engine_proxy.init_transfer_from_cr(transfer_id)
      if transfer_action_info[2] == TR_MFF_CR_TO_C:
        WorkflowController.create_dir_structure(os.path.dirname(client_path), 
                                                dir_content)
      if transfer_action_info[2] == TR_DIR_CR_TO_C:
        WorkflowController.create_dir_structure(client_path, 
                                                dir_content)
      return transfer_action_info
    
    return None
    

  def write_to_computing_resource_file(self, 
                                       transfer_id, 
                                       data, 
                                       relative_path=None):
    '''
    Write a piece of data to a file locate on the computing resouce.

    @type  transfer_id: string
    @param transfer_id: transfer id
    @type  data: data
    @param data: data to write to the file
    @type  relative_path: relative file path
    @param relative_path: Mandatory to identify the file concerned if is the    
                          transfer_id correspond to a directory transfer. 
                          None if it concerns only one file.
    @rtype : boolean
    @return: the file transfer ended. Note that if the transfer_id correspond to
    a directory transfer, it does not mean that the whole directory transfer ended. 
    '''

    status = self._engine_proxy.transfer_status(transfer_id)
    if not status == TRANSFERING_FROM_CLIENT_TO_CR:
      self.initialize_transfer(transfer_id)
    transfer_ended = self._engine_proxy.write_to_computing_resource_file(
                                                                  transfer_id, 
                                                                  data, 
                                                                 relative_path)
    return transfer_ended

  def read_from_computing_resource_file(self, 
                                        transfer_id, 
                                        buffer_size, 
                                        transmitted,
                                        relative_path=None):
    '''
    Read a piece of data from a file located on the computing resource.
    
    @type  transfer_id: string
    @param transfer_id: transfer id
    @type  transmitted: int
    @param transmitted: size of the data already read
    @type  buffer_size: int
    @param buffer_size: size of the piece to read
    @type  relative_path: file path
    @param relative_path: Mandatory to identify the file concerned if is the    
                          transfer_id correspond to a directory transfer. 
                          None if it concerns only one file.

    @rtype: data
    @return: piece of data read from the file at the position transmitted
    '''
    
    data = self._engine_proxy.read_from_computing_resource_file(transfer_id, 
                                                                buffer_size, 
                                                                transmitted,
                                                                relative_path)
    if not data:
      # check if the whole transfer ended
      transfer_action_info = self._engine_proxy.transfer_action_info(transfer_id)
      if not transfer_action_info == None: # None if stdout and stderr
        assert(transfer_action_info[2] == TR_FILE_CR_TO_C or        
              transfer_action_info[2] == TR_DIR_CR_TO_C or 
              transfer_action_info[2] == TR_MFF_CR_TO_C)
        (status, progression) = self.transfer_status(transfer_id)
        if transfer_action_info[2] == TR_FILE_CR_TO_C:
          (file_size, transfered) = progression
          if file_size == transfered:
            self._engine_proxy.set_transfer_status(transfer_id,   
                                                  FILES_ON_CLIENT_AND_CR)
        if transfer_action_info[2] == TR_DIR_CR_TO_C or \
          transfer_action_info[2] == TR_MFF_CR_TO_C:
          (cumulated_file_size, 
          cumulated_transmissions, 
          files_transfer_status) = progression
          if cumulated_transmissions == cumulated_file_size:
            self._engine_proxy.set_transfer_status(transfer_id,   
                                                  FILES_ON_CLIENT_AND_CR)

    return data
    
     
  def delete_transfer(self, transfer_id):
    '''
    Deletes the file or directory copied on the computing resource side and the 
    associated transfer information.
    If some jobs reference these file(s) as input or output, the transfer will 
    not be deleted immediately but as soon as all the jobs will be deleted.
    
    @type transfer_id: string
    @param transfer_id: transfer id
    '''
    self._engine_proxy.delete_transfer(transfer_id)
    

  ########## JOB SUBMISSION ##################################################



  def delete_job( self, job_id ):
    '''
    Frees all the resources allocated to the submitted job on the database 
    server. After this call, the C{job_id} becomes invalid and
    cannot be used anymore. 
    To avoid that jobs create non handled files, L{delete_job} kills the job if 
    it is running.

    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{jobs} or the submission 
    methods L{submit_job}, L{customSubmit} or L{submitWithTransfer})
    '''
    
    self._engine_proxy.delete_job(job_id)
    

  
  
  def delete_workflow(self, workflow_id):
    '''
    Removes a workflow and all its associated elements (file transfers and jobs)
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
    @return: job_id -> (name, command, submission date)
    '''
    
    return self._engine_proxy.jobs(job_ids)
    
  def transfers(self, transfer_ids=None):
    '''
    Transfers which are not part of a workflow.
    If a sequence of transfer id is given, the function returns general 
    information about these transfers.
    Returns a dictionary of transfer identifiers associated to general 
    information.
    
    @rtype: dictionary: string -> tuple(string, 
                                        string, 
                                        date,  
                                        None or sequence of string)
    @return: transfer_id -> ( -client_path: client file or directory path
                              -expiration_date: after this date the file copied
                              on the computing resource and all the transfer 
                              information will be deleted, unless an existing 
                              job has declared this file as output or input.
                              -client_paths: sequence of file or directory path 
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
  
  
  def workflow_elements_status(self, wf_id, group = None):
    '''
    Gets back the status of all the workflow elements at once, minimizing the
    communication with the server and requests to the database.
    
    @type  wf_id: C{WorflowIdentifier}
    @param wf_id: The workflow identifier
    @rtype: tuple (sequence of tuple (job_id, status, exit_info, (submission_date, execution_date, ending_date)), sequence of tuple (transfer_id, (status, progression_info)), workflow_status)
    '''
    wf_status = self._engine_proxy.workflow_elements_status(wf_id)
    if not wf_status:
      # TBI raise ...
      return
     # special processing for transfer status:
    new_transfer_status = []
    for engine_path, client_path, status, transfer_action_info in wf_status[1]:
      progression = self._transfer_progress(engine_path, 
                                            client_path, 
                                            transfer_action_info)
      new_transfer_status.append((engine_path, (status, progression)))
      
    new_wf_status = (wf_status[0],new_transfer_status, wf_status[2])
    return new_wf_status
    
  
  def transfer_status(self, transfer_id):
    '''
    Returns the status of a transfer and the information related to the transfer in progress in such case. 
    
    @type  transfer_id: string
    @rtype: tuple  (C{transfer_status} or None, tuple or None)
    @return: [0] the transfer status among constants.FILE_TRANSFER_STATUS
             [1] None if the transfer status in not constants.TRANSFERING_FROM_CLIENT_TO_CR or 
                 constants.TRANSFERING_FROM_CR_TO_CLIENT
                 if it is a file transfer: tuple (file size, size already transfered)
                 if it is a directory transfer: tuple (cumulated size, sequence of tuple (relative_path, file_size, size already transfered)
    '''
    
    status = self._engine_proxy.transfer_status(transfer_id)
    transfer_action_info =  self._engine_proxy.transfer_action_info(transfer_id)
    transfer_id, client_path, expiration_date, workflow_id, client_paths = self._engine_proxy.transfer_information(transfer_id)
    progression = self._transfer_progress(transfer_id, client_path, transfer_action_info)
    return (status, progression)
      
            
            
  def _transfer_progress(self, engine_path, client_path, transfer_action_info):
    progression_info = None
    if transfer_action_info == None :
      return None
    if transfer_action_info[2] == TR_FILE_C_TO_CR or \
       transfer_action_info[2] == TR_DIR_C_TO_CR or \
       transfer_action_info[2] == TR_MFF_C_TO_CR:
      return self._engine_proxy.transfer_progression_status(engine_path, 
                                                            transfer_action_info)

    if transfer_action_info[2] == TR_FILE_CR_TO_C:
      (file_size, md5_hash, transfer_type) = transfer_action_info
      if os.path.isfile(client_path):
        transmitted = os.stat(client_path).st_size
      else:
        transmitted = 0
      return (file_size, transmitted)

    elif transfer_action_info[2] == TR_MFF_CR_TO_C or \
         transfer_action_info[2] == TR_DIR_CR_TO_C:
      (cumulated_file_size, 
       dir_element_action_info, 
       transfer_type) = transfer_action_info
      files_transfer_status = []
      for relative_path, (file_size, md5_hash) in dir_element_action_info.iteritems():
        if transfer_type == TR_MFF_CR_TO_C:
          full_path = os.path.join(os.path.dirname(client_path), 
                                   relative_path)
        else: #TR_DIR_CR_TO_C
          full_path = os.path.join(client_path, 
                                   relative_path)
        if os.path.isfile(full_path):
          transmitted = os.stat(full_path).st_size
        else:
          transmitted = 0
        files_transfer_status.append((relative_path, file_size, transmitted))
      cumulated_transmissions = reduce( operator.add, (i[2] for i in files_transfer_status) )
      return (cumulated_file_size, 
              cumulated_transmissions, 
              files_transfer_status)
       
    return None
    

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
    
    engine_stdout_file, stdout_transfer_action_info, engine_stderr_file, stderr_transfer_action_info = self._engine_proxy.stdouterr_transfer_action_info(job_id)
    
    open(stdout_file_path, 'wb') 
    if engine_stdout_file and stdout_transfer_action_info:
      self._transfer_file_from_cr(stdout_file_path, 
                                  engine_stdout_file, 
                                  stdout_transfer_action_info[0], 
                                  stdout_transfer_action_info[1], 
                                  buffer_size)
  
    if stderr_file_path:
      open(stderr_file_path, 'wb') 
      if engine_stderr_file and stderr_transfer_action_info:
          self._transfer_file_from_cr(stderr_file_path, 
                                      engine_stderr_file, 
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
    Kill the job execution, the job will not be deleted of the database server. 
    Use the L{restart_job} method to restart the job.
    The job_id must be valid.
    
    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{submit_job} or L{jobs})
    '''
    self._engine_proxy.kill_job(job_id)
   
  
  def restart_job( self, job_id ):
    '''   
    Restarts a job which status is constants.FAILED or constants.WARNING.
    The job_id must be valid.
    Return True if the job was restarted.
    
    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{submit_job} or L{jobs})
    '''
    self._engine_proxy.restart_job(job_id)


  @staticmethod
  def dir_content(path_seq, md5_hash=False):
    result = []
    for path in path_seq:
      s = os.stat(path)
      if stat.S_ISDIR(s.st_mode):
        full_path_list = []
        for element in os.listdir(path):
          full_path_list.append(os.path.join(path, element))
        content = WorkflowController.dir_content(full_path_list, md5_hash)
        result.append((os.path.basename(path), content, None))
      else:
        if md5_hash:
          result.append( ( os.path.basename(path), s.st_size, hashlib.md5( open( path, 'rb' ).read() ).hexdigest() ) )
        else:
          result.append( ( os.path.basename(path), s.st_size, None ) )
    return result

  @staticmethod     
  def create_dir_structure(path, content, subdirectory = ""):
    if not os.path.isdir(path):
      os.makedirs(path)
    for item, description, md5_hash in content:
      relative_path = os.path.join(subdirectory,item)
      full_path = os.path.join(path, relative_path)
      if isinstance(description, list):
        if not os.path.isdir(full_path):
          os.mkdir(full_path)
        WorkflowController.create_dir_structure(path, description, relative_path)
