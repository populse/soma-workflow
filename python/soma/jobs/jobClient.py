'''
@author: Soizic Laguitton
@organization: U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''

__docformat__ = "epytext en"

import ConfigParser
import soma.jobs.connection 
import random
import socket
from soma.jobs.constants import *
import time


''' 
Jobs
====
  
  Instances of Jobs allow to submit, control and retrieve jobs, file transfers and workflows.

Workflows
=========
  
  Use JobTemplate, FileTransfer, FileRetrieving, FileSending and Workflow classes to build worfklows. 
  Use Jobs.submitWorkflow to submit a workflow.

Definitions
===========

  -'Local' refers to the hosts of the cluster where the jobs are submitted to. Eg: a local file can be reached by any host of the cluser and a local process runs on a cluster submitting host.
  -'Remote' refers to all hosts, processes or files which are not local.
  -'Parallel job': job requiring more than one node to run.
'''


    
class WorkflowNode(object):
  '''
  Workflow node.
  '''
  def __init__(self, name):
    self.name = name

class JobTemplate(WorkflowNode):
  '''
  Job representation in a workflow. 
  Workflow node type.
  
  The job parameters are identical to the Jobs.submit method arguments except 
  that referenced_input_files and references_output_files must be sequences of 
  L{FileTransfer}.
  (See the Jobs.submit method for a description of each parameter)
  '''
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
    super(JobTemplate, self).__init__(name_description)
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

    
    
class FileTranslation(object):
  '''
  #Workflow node type
  File representation using relative path. 
  When the workflow will be submitted to a resource, local translation files will be used to transform a namespace, 
  an uuid and a relative path to an absolut path on the cluster.
  #Use relative_paths if the translation involves several associated files and/or directories, eg:
        #- file serie 
        #- in the case of file format associating several file and/or directories
          #(ex: a SPM image is stored in 2 files: .img and .hdr)
  #In this case, set remote_path to one the files (eq: .img).
  #In other cases (1 file or 1 directory) the relative_paths must be set to None.
  '''
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
    #super(FileTranslation, self).__init__(ftl_name)
    
    self.relative_path = relative_path
    self.namespace = namespace
    self.uuid = uuid
    self.disposal_timout = disposal_timeout
    #self.relative_paths = relative_paths
    
    self.translation = None # absolute local file path 

class FileTransfer(WorkflowNode):
  '''
  Workflow node type.
  File transfer representation in a workflow.
  Use remote_paths if the transfer involves several associated files and/or directories, eg:
        - file serie 
        - in the case of file format associating several file and/or directories
          (ex: a SPM image is stored in 2 files: .img and .hdr)
  In this case, set remote_path to one the files (eq: .img).
  In other cases (1 file or 1 directory) the remote_paths must be set to None.
  '''
  def __init__( self,
                remote_path, 
                disposal_timeout = 168,
                name = None,
                remote_paths = None):
    if name:
      ft_name = name
    else:
      ft_name = remote_path + "transfer"
    super(FileTransfer, self).__init__(ft_name)
    
    self.remote_path = remote_path
    self.disposal_timeout = disposal_timeout
   

    self.remote_paths = remote_paths
    self.local_path = None

class FileSending(FileTransfer):
  '''
  Workflow node type.
  File transfer for an input file.
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
  File transfer for an output file.
  '''
  def __init__( self,
                remote_path, 
                disposal_timeout = 168,
                name = None,
                remote_paths = None):
    super(FileRetrieving, self).__init__(remote_path, disposal_timeout, name, remote_paths)
    
class Group(object):
  '''
  Workflow group: provide a hierarchical structure to a workflow.
  However groups has only a displaying role, it doesn't have
  any impact on the workflow execution.
  '''
  def __init__(self, elements, name = None):
    '''
    @type  elements: sequence of JobTemplate and groups
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
  Workflow to be submitted using an instance of the Jobs class.
  '''
  def __init__(self, nodes, dependencies, mainGroup = None, groups = []):
    '''
    @type  node: sequence of L{WorkflowNode} 
    @param node: workflow elements
    @type  dependencies: sequence of tuple (node, node) a node being 
    a L{JobTemplate} or a L{FileTransfer}
    @param dependencies: dependencies between workflow elements specifying an execution order. 
    @type  groups: sequence of sequence of L{Groups} and/or L{JobTemplate}
    @param groups: (optional) provide a hierarchical structure to a workflow for displaying purpose only
    @type  mainGroup: sequence of L{Groups} and/or L{JobTemplate}
    @param mainGroup: (optional) lower level group.  
    
    For submitted workflows: 
      - full_dependencies is a set including the Workflow.dependencies + the FileTransfer-JobTemplate and JobTemplate-FileTransfer dependencies
      - full_nodes is a set including the nodes + the FileTransfer nodes. 
    '''
    
    self.wf_id = -1
    self.name = None
    self.nodes = nodes
    self.full_nodes = None
    self.dependencies = dependencies
    self.full_dependencies = None 
    self.groups= groups
    if mainGroup:
      self.mainGroup = mainGroup
    else:
      elements = []
      for node in self.nodes:
        if isinstance(node, JobTemplate):
          elements.append(node) 
      self.mainGroup = Group(elements, "main_group")
      
class Jobs(object):
  
  '''
  Submition, control and monitoring of jobs, transfer and workflows.
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

    if config.has_section(OCFG_SECTION_CLIENT) and not config.get(OCFG_SECTION_CLIENT, OCFG_CLIENT_LOG_FILE) == 'None':
       import logging
       logging.basicConfig(filename = config.get(OCFG_SECTION_CLIENT, OCFG_CLIENT_LOG_FILE),
                           format = config.get(OCFG_SECTION_CLIENT, OCFG_CLIENT_LOG_FORMAT, 1), 
                           level = eval("logging."+config.get(OCFG_SECTION_CLIENT, OCFG_CLIENT_LOG_LEVEL)))
   
    submitting_machines = config.get(resource_id, CFG_SUBMITTING_MACHINES).split()
    cluster_address = config.get(resource_id, CFG_CLUSTER_ADDRESS)
    hostname = socket.gethostname()
    mode = 'remote'
    for machine in submitting_machines:
      if hostname == machine: mode = 'local'
    print "hostname: " + hostname + " => mode = " + mode
    src_local_process = config.get(resource_id, CFG_SRC_LOCAL_PROCESS)

    #########################
    # Connection
    self.__mode = mode #'local_no_disconnection' #(local debug)#      
    
    #########
    # LOCAL #
    #########
    if self.__mode == 'local':
      self.__connection = soma.jobs.connection.JobLocalConnection(src_local_process, resource_id, log)
      self.__js_proxy = self.__connection.getJobScheduler()
      self.__file_transfer = soma.jobs.connection.LocalTransfer(self.__js_proxy)
    
    ##########
    # REMOTE #
    ##########
    if self.__mode == 'remote':
      sub_machine = submitting_machines[random.randint(0, len(submitting_machines)-1)]
      print 'cluster address: ' + cluster_address
      print 'submission machine: ' + sub_machine
      self.__connection = soma.jobs.connection.JobRemoteConnection(login, password, cluster_address, sub_machine, src_local_process, resource_id, log)
      self.__js_proxy = self.__connection.getJobScheduler()
      self.__file_transfer = soma.jobs.connection.RemoteTransfer(self.__js_proxy)
    
    ###############
    # LOCAL DEBUG #
    ###############
    if self.__mode == 'local_no_disconnection': # DEBUG
      from soma.jobs.jobScheduler import JobScheduler
      import logging
      import Pyro.naming
      import Pyro.core
      from Pyro.errors import PyroError, NamingError
      
      # log file 
      if not config.get(resource_id, OCFG_LOCAL_PROCESSES_LOG_DIR) == 'None':
        logfilepath =  config.get(resource_id, OCFG_LOCAL_PROCESSES_LOG_DIR)+ "log_jobScheduler_sl225510"+repr(log)#+time.strftime("_%d_%b_%I:%M:%S", time.gmtime())
        logging.basicConfig(
          filename = logfilepath,
          format = config.get(resource_id, OCFG_LOCAL_PROCESSES_LOG_FORMAT, 1),
          level = eval("logging."+config.get(resource_id, OCFG_LOCAL_PROCESSES_LOG_LEVEL)))
      
      global logger
      logger = logging.getLogger('ljp')
      logger.info(" ")
      logger.info("****************************************************")
      logger.info("****************************************************")
    
      # looking for the JobServer
      Pyro.core.initClient()
      locator = Pyro.naming.NameServerLocator()
      name_server_host = config.get(resource_id, CFG_NAME_SERVER_HOST)
      if name_server_host == 'None':
        ns = locator.getNS()
      else: 
        ns = locator.getNS(host= name_server_host )
    
      job_server_name = config.get(resource_id, CFG_JOB_SERVER_NAME)
      try:
        URI=ns.resolve(job_server_name)
        logger.info('JobServer URI:'+ repr(URI))
      except NamingError,x:
        logger.critical('Couldn\'t find' + job_server_name + ' nameserver says:',x)
        raise SystemExit
      jobServer= Pyro.core.getProxyForURI( URI )
  
      #parallel_job_submission_info
      parallel_job_submission_info= {}
      for parallel_config_info in PARALLEL_DRMAA_ATTRIBUTES + PARALLEL_JOB_ENV + PARALLEL_CONFIGURATIONS:
        if config.has_option(resource_id, parallel_config_info):
          parallel_job_submission_info[parallel_config_info] = config.get(resource_id, parallel_config_info)
  
      # File path translation
      file_path_translation = None
      if config.has_option(resource_id, OCFG_TRANSLATION_FILES):
        file_path_translation={}
        translation_files_str = config.get(resource_id, OCFG_TRANSLATION_FILES)
        print "Translation files configured:"
        for ns_file_str in translation_files_str.split():
          ns_file = ns_file_str.split("{")
          namespace = ns_file[0]
          filename = ns_file[1].rstrip("}")
          print " -namespace: " + namespace + ", translation file: " + filename
          try: 
            f = open(filename, "r")
          except IOError, e:
            logger.info("Couldn't read the translation file: " + filename)
          else:
            if not namespace in file_path_translation.keys():
              file_path_translation[namespace] = {}
            line = f.readline()
            while line:
              splitted_line = line.split(None,1)
              if len(splitted_line) > 1:
                uuid = splitted_line[0]
                contents = splitted_line[1].rstrip()
                print "      uuid: " + uuid + "   translation:" + contents
                file_path_translation[namespace][uuid] = contents
              line = f.readline()
            f.close()
      
  
      self.__js_proxy  = JobScheduler(job_server=jobServer, drmaa_job_scheduler=None, parallel_job_submission_info=parallel_job_submission_info,
      file_path_translation = file_path_translation)
      self.__file_transfer = soma.jobs.connection.LocalTransfer(self.__js_proxy)
      self.__connection = None

    


  def disconnect(self):
    '''
    Simulates a disconnection for TEST PURPOSE ONLY.
    !!! The current instance won't be usable anymore after this call !!!!
    '''
    self.__connection.stop()

   


  ########## FILE TRANSFER ###############################################
    
  '''
  The file transfer methods must be used when submitting a job from a remote
  machine
  
  Example:
  
  #job remote input files: rfin_1, rfin_2, ..., rfin_n 
  #job remote output files: rfout_1, rfout_2, ..., rfout_m  
  
  #Call registerTransfer for each output file:
  lfout_1 = jobs.registerTransfer(rfout_1)
  lfout_2 = jobs.registerTransfer(rfout_2)
  ...
  lfout_n = jobs.registerTransfer(rfout_m)
  
  #Call send for each input file:
  lfin_1= jobs.send(rfin_1)
  lfin_2= jobs.send(rfin_2)
  ...
  lfin_n= jobs.send(rfin_n)
    
  #Job submittion: don't forget to reference local input and output files 
  job_id = jobs.submit(['python', '/somewhere/something.py'], 
                       [lfin_1, lfin_2, ..., lfin_n],
                       [lfout_1, lfout_2, ..., lfout_n])
  jobs.wait(job_id)
  
  #After Job execution, transfer back the output file
  jobs.retrieve(lfout_1)
  jobs.retrieve(lfout_2)
  ...
  jobs.retrieve(lfout_m)
  
  When sending or registering a transfer, use remote_paths if the transfer involves several associated files and/or directories:
          - when transfering a file serie 
          - in the case of file format associating several file and/or directories
            (ex: a SPM image is stored in 2 files: .img and .hdr)
  In this case, set remote_path to one the files (eq: .img).
  In other cases (1 file or 1 directory) the remote_paths must be set to None.
  
  Example:
    
  #transfer of a SPM image file
  fout_1 = jobs.registerTransfer(remote_path = 'mypath/myimage.img', 
                                 remote_paths = ['mypath/myimage.img', 'mypath/myimage.hdr'])
  ...
  jobs.retrive(fout_1)
 
  '''
    
  def send(self, remote_input, disposal_timeout = 168, remote_paths=None):
    '''
    Transfers a remote file to a local directory. 
   
    @type  remote_input: string 
    @param remote_input: remote path of input file
    @type  disposalTimeout: int
    @param disposalTimeout:  The local file and transfer information is 
    automatically disposed after disposal_timeout hours, except if a job 
    references it as input. Default delay is 168 hours (7 days).
    @type remote_paths: sequence of string or None
    @type remote_paths: sequence of file to transfer if transfering a 
    file serie or if the file format involve serveral files of directories.
    @rtype: string 
    @return: local path where the remote file was copied
    '''
    local_path = self.__js_proxy.registerTransfer(remote_input, disposal_timeout, remote_paths)
    self.__js_proxy.setTransferStatus(local_path, TRANSFERING)
    self.__file_transfer.send(local_path, remote_input, remote_paths) 
    self.__js_proxy.setTransferStatus(local_path, TRANSFERED)
    self.__js_proxy.signalTransferEnded(local_path)
    return local_path
  

  def registerTransfer(self, remote_path, disposal_timeout=168, remote_paths=None): 
    '''
    Generates a unique local path and save the (local_path, remote_path) association.
    
    @type  remote_path: string
    @param remote_path: remote path of file
    @type  disposalTimeout: int
    @param disposalTimeout: The local file and transfer information is 
    automatically disposed after disposal_timeout hours, except if a job 
    references it as output or input. Default delay is 168 hours (7 days).
    @type remote_paths: sequence of string or None
    @type remote_paths: sequence of file to transfer if transfering a 
    file serie or if the file format involve serveral file of directories.
    @rtype: string or sequence of string
    @return: local file path associated with the remote file
    '''
    return self.__js_proxy.registerTransfer(remote_path, disposal_timeout, remote_paths)

  def sendRegisteredTransfer(self, local_path):
    '''
    Transfer one or several remote file(s) to a local directory. The local_path 
    must have been generated using the registerTransfer method. 
        
      local_path = send(remote_path)
      
    is strictly equivalent to :
    
      local_path = registerTransfer(remote_path)
      sendRegisteredTransfer(local_path)
    
    Use registerTransfer + sendRegisteredTransfer when the local_path
    is needed before transfering to file.
    '''
    self.__js_proxy.setTransferStatus(local_path, TRANSFERING)
    time.sleep(1) #TEST !
    local_path, remote_path, expiration_date, workflow_id, remote_paths = self.__js_proxy.transferInformation(local_path)
    self.__file_transfer.send(local_path, remote_path, remote_paths)
    self.__js_proxy.setTransferStatus(local_path, TRANSFERED)
    self.__js_proxy.signalTransferEnded(local_path)

  def retrieve(self, local_path):
    '''
    If local_path is a file path: copies the local file to the associated remote file path.
    If local_path is a directory path: copies the content of the directory to the associated remote directory.
    The local path must belong to the user's transfers (ie belong to 
    the sequence returned by the L{transfers} method). 
    
    @type  local_path: string 
    @param local_path: local path 
    '''
    
    local_path, remote_path, expiration_date, workflow_id, remote_paths = self.__js_proxy.transferInformation(local_path)
    self.__js_proxy.setTransferStatus(local_path, TRANSFERING)
    self.__file_transfer.retrieve(local_path, remote_path, remote_paths)
    self.__js_proxy.setTransferStatus(local_path, TRANSFERED)
   
  def cancelTransfer(self, local_path):
    '''
    Deletes the local file or directory and the associated transfer information.
    If some jobs reference the local file(s) as input or output, the transfer won't be 
    deleted immediately but as soon as all the jobs will be disposed.
    
    @type local_path: string
    @param local_path: local path associated with a transfer (ie 
    belongs to the list returned by L{transfers}    
    '''
    self.__js_proxy.cancelTransfer(local_path)
    

  ########## JOB SUBMISSION ##################################################

  '''
  L{submit} method submits a job for execution to the cluster. 
  A job identifier is returned and can be used to inspect and 
  control the job.
  
  Example:
    import soma.jobs.jobClient
      
    jobs = soma.jobs.jobClient.Jobs()
    job_id = jobs.submit( ['python', '/somewhere/something.py'] )
    jobs.stop(job_id)
    jobs.restart(job_id)
    jobs.wait([job_id])
    exitinfo = jobs.exitInformation(job_id)
    jobs.dispose(job_id)
  '''

  def submit( self,
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

    '''
    Submits a job for execution to the cluster. A job identifier is returned and 
    can be used to inspect and control the job.
    If the job used transfered files (L{send} and L{registerTransfer} 
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
    user had called L{kill} and L{dispose}.
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
    configuration_name: type of parallel job as defined in soma.jobs.constants (eg MPI, OpenMP...)
    max_node_num: maximum node number the job requests (on a unique machine or separated machine
    depending on the parallel configuration)
    !! Warning !!: parallel configurations are not necessarily implemented for every cluster. 
                   This is the only argument that is likely to request a specific implementation 
                   for each cluster/DRMS.
  
    @rtype:   C{JobIdentifier}
    @return:  the identifier of the submitted job 

    '''

    job_id = self.__js_proxy.submit(JobTemplate(command,
                                    referenced_input_files,
                                    referenced_output_files,
                                    stdin,
                                    join_stderrout,
                                    disposal_timeout,
                                    name_description,
                                    stdout_file,
                                    stderr_file,
                                    working_directory,
                                    parallel_job_info))
    return job_id
   

  def dispose( self, job_id ):
    '''
    Frees all the resources allocated to the submitted job on the data server
    L{JobServer}. After this call, the C{job_id} becomes invalid and
    cannot be used anymore. 
    To avoid that jobs create non handled files, L{dispose} kills the job if 
    it's running.

    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{jobs} or the submission 
    methods L{submit}, L{customSubmit} or L{submitWithTransfer})
    '''
    
    self.__js_proxy.dispose(job_id)
    

  ########## WORKFLOW SUBMISSION ####################################
  
  def submitWorkflow(self, workflow, expiration_date=None, name = None):
    '''
    Submits a workflow to the system and returns the id of each 
    submitted workflow element (Job or file transfer).
    
    @type  workflow: L{Workflow}
    @param workflow: workflow description (nodes and node dependencies)
    @type  expiration_date: datetime.datetime
    @rtype: L{Workflow}
    @return: The workflow compemented by the id of each submitted 
    workflow element.
    '''
    submittedWF =  self.__js_proxy.submitWorkflow(workflow, expiration_date, name)
    return submittedWF
  
  
  def disposeWorkflow(self, workflow_id):
    '''
    Removes a workflow and all its associated nodes (file transfers and jobs)
    '''
    self.__js_proxy.disposeWorkflow(workflow_id)
    
  def changeWorkflowExpirationDate(self, workflow_id, new_expiration_date):
    '''
    Ask a new expiration date for the workflow.
    Return True if the workflow expiration date was set to the new date.
        
    @type  workflow_id: C{WorkflowIdentifier}
    @type  expiration_date: datetime.datetime
    @rtype: boolean
    '''
    return self.__js_proxy.changeWorkflowExpirationDate(workflow_id, new_expiration_date)

  ########## MONITORING #############################################


  def jobs(self):
    '''
    Returns the identifier of the submitted and not diposed jobs.

    @rtype:  sequence of C{JobIdentifier}
    @return: series of job identifiers
    '''
    
    return self.__js_proxy.jobs()
    
  def transfers(self):
    '''
    Returns the transfers currently owned by the user as a sequence of local file path 
    returned by the L{registerTransfer} method. 
    
    @rtype: sequence of string
    @return: sequence of local file path.
    '''
 
    return self.__js_proxy.transfers()

  def workflows(self):
    '''
    Returns the identifiers of the submitted workflows.
    
    @rtype: sequence of C{WorkflowIdentifier}
    '''
    return self.__js_proxy.workflows()
  
  def workflowInformation(self, wf_id):
    '''
    Returns a tuple: 
      - expiration date
      - workflow name
      
    @rtype: (date, name)
    @return: (expiration date, workflow name) 
    '''
    return self.__js_proxy.workflowInformation(wf_id)
    
  
  def submittedWorkflow(self, wf_id):
    '''
    Returns the submitted workflow.
    
    @rtype: L{Workflow}
    @return: submitted workflow
    '''
    return self.__js_proxy.submittedWorkflow(wf_id)
    
  def transferInformation(self, local_path):
    '''
    The local_path must belong to the list of paths returned by L{transfers}.
    Returns the information related to the file transfer corresponding to the 
    local_path.

    @rtype: tuple (local_path, remote_path, expiration_date)
    @return:
        -local_path: path of the local file or directory
        -remote_path: remote file or directory path
        -expiration_date: after this date the local file will be deleted, unless an
        existing job has declared this file as output or input.
        -remote_paths: sequence of file or directory path or None
    '''
    return self.__js_proxy.transferInformation(local_path)
   
  
  
  def status( self, job_id ):
    '''
    Returns the status of a submitted job.
    
    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{submit} or L{jobs})
    @rtype:  C{JobStatus} or None
    @return: the status of the job, if its valid and own by the current user, None 
    otherwise. See the list of status: constants.JOBS_STATUS.
    '''
    return self.__js_proxy.status(job_id)
  
  
  def workflowStatus(self, wf_id, groupe = None):
    '''
    Gets back the status of all the workflow elements at once, minimizing the
    communication with the server and requests to the database.
    
    @type  wf_id: C{WorflowIdentifier}
    @param wf_id: The workflow identifier
    @rtype: tuple (sequence of tuple (job_id, status, exit_info, (submission_date, execution_date, ending_date)), sequence of tuple (transfer_id, status))
    '''
    
    return self.__js_proxy.workflowStatus(wf_id)
  
  def transferStatus(self, local_path):
    '''
    Returns the status of a transfer. 
    
    @type  local_path: string
    @param local_path: 
    @rtype: C{TransferStatus} or None
    @return: the status of the job transfer if its valid and own by the current user, None
    otherwise. See the list of status: constants. constants.FILE_TRANSFER_STATUS
    '''
    return self.__js_proxy.transferStatus(local_path) 

  def exitInformation(self, job_id ):
    '''
    Gives the information related to the end of the job.
   
    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{submit} or L{jobs})
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
    return self.__js_proxy.exitInformation(job_id)
    
 
  def jobInformation(self, job_id):
    '''
    Gives general information about the job: name/description, command and 
    submission date.
    
    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{submit} or L{jobs})
    @rtype: tuple or None
    @return: (name_description, command, submission_date) it may be C{None} if the job 
    is not valid. 
    '''
    return self.__js_proxy.jobInformation(job_id)
    

  def resertStdReading(self):
    self.__js_proxy.resertStdReading()

  def stdoutReadLine(self, job_id):
    '''
    Reads a line from the file where the job standard output stream is written.
    
    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{submit} or L{jobs})
    @rtype: string
    return: read line
    '''
    return self.__js_proxy.stdoutReadLine(job_id)

  

  def stderrReadLine(self, job_id):
    '''
    Reads a line from the file where the job standard error stream is written.
    
    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{submit} or L{jobs})
    @rtype: string
    return: read line
    '''
    return self.__js_proxy.stderrReadLine(job_id)

    
  ########## JOB CONTROL VIA DRMS ########################################
  
  
  def wait( self, job_ids, timeout = -1):
    '''
    Waits for all the specified jobs to finish execution or fail. 
    The job_id must be valid.
    
    @type  job_ids: set of C{JobIdentifier}
    @param job_ids: Set of jobs to wait for
    @type  timeout: int
    @param timeout: the call exits before timout seconds. a negative value 
    means to wait indefinetely for the result. 0 means to return immediately
    '''
    self.__js_proxy.wait(job_ids, timeout)

  def stop( self, job_id ):
    '''
    Temporarily stops the job until the method L{restart} is called. The job 
    is held if it was waiting in a queue and suspended if was running. 
    The job_id must be valid.
    
    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{submit} or L{jobs})
    '''
    self.__js_proxy.stop(job_id)
   
  
  
  def restart( self, job_id ):
    '''
    Restarts a job previously stopped by the L{stop} method.
    The job_id must be valid.
    
    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{submit} or L{jobs})
    '''
    self.__js_proxy.restart(job_id)


  def kill( self, job_id ):
    '''
    Definitely terminates a job execution. After a L{kill}, a job is still in
    the list returned by L{jobs} and it can still be inspected by methods like
    L{status} or L{output}. To completely erase a job, it is necessary to call
    the L{dispose} method.
    The job_id must be valid.
    
    @type  job_id: C{JobIdentifier}
    @param job_id: The job identifier (returned by L{submit} or L{jobs})
    '''
    self.__js_proxy.kill(job_id)

    
    
    