from __future__ import with_statement

'''
@author: Yann Cointepas
@author: Soizic Laguitton
@organization: U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''

'''
soma-workflow engine classes

The soma-workflow engine is a process that run on the computing resource side.
The engine classes are used in soma.workflow.start_workflow_engine script which 
creates a workflow engine process.

The DrmaaWorkflowEngine object submits jobs to the resource DRMS using a DRMAA 
session, it also schedules the workflow's job submission and updates the 
database (soma.workflow.server.WorkflowDatabaseServer) at regular time 
interval with the status of the jobs and workflows it manages.
The WorkflowEngine uses a DrmaaWorkflowEngine instance, it does all the file 
transfer work that should be done on the computing resource side, and queries 
the database for all that doesn't concern submitted jobs and workflows.
'''

#-----------------------------------------------------------------------------
# Imports
#-----------------------------------------------------------------------------

from datetime import date, timedelta, datetime
import pwd
import os
import threading
import time
import logging
import copy
import stat, hashlib, operator

import Pyro.naming, Pyro.core
from Pyro.errors import NamingError

import soma.workflow.constants as constants
from soma.workflow.client import Job, FileTransfer, FileSending, FileRetrieving, Workflow, SharedResourcePath
import soma.workflow.database_server 
from soma.pipeline.somadrmaajobssip import DrmaaJobs

#-----------------------------------------------------------------------------
# Globals and constants
#-----------------------------------------------------------------------------

__docformat__ = "epytext en"
refreshment_interval = 1 #seconds

#-----------------------------------------------------------------------------
# Classes and functions
#-----------------------------------------------------------------------------

class WorkflowEngineError(Exception): 
  def __init__(self, msg, logger = None):
    self.args = (msg,)
    if logger:
      logger.critical('EXCEPTION ' + msg)

class DrmaaWorkflowEngine(object):
  '''
  Instances of this class opens a DRMAA session and allows to submit and control 
  the jobs. It updates constantly the jobs status on the L{WorkflowDatabaseServer}. 
  The L{DrmaaWorkflowEngine} must be created on one of the machine which is allowed
  to submit jobs by the DRMS.
  '''

  class RegisteredJob(object):
    '''
    Job representation in DrmaaWorkflowEngine.
    '''
    def __init__(self, jobTemplate):
      '''
      @type  jobTemplate: L{constant.Job}
      @param jobTemplate: job submittion information. 
      ote a few restrictions concerning the type of some Job members:
      referenced_input_files, referenced_output_files and command: must be lists of string
      stdout, stderr and stdin: must be strings
      job_id must be a valid job id registered in the database server
      @type  drmaa_id: string or None
      @param drmaa_id: submitted job DRMAA identifier, None if the job is not submitted
      @type  status: string
      @param status: job status as defined in constants.JOB_STATUS
      @type  last_status_update: date
      @param last_status_update: last status update date
      @type  exit_status: string 
      @param exit_status: exit status string as defined in L{WorkflowDatabaseServer}
      @type  exit_value: int or None
      @param exit_value: if the status is FINISHED_REGULARLY, it contains the operating 
      system exit code of the job.
      @type  terminating_signal: string or None
      @param terminating_signal: if the status is FINISHED_TERM_SIG, it contain a representation 
      '''
      self.jobTemplate = jobTemplate
      self.drmaa_id = None
      self.status = constants.NOT_SUBMITTED
      self.exit_status = None
      self.exit_value = None
      self.terminating_signal = None


  def __init__(self, 
               database_server, 
               parallel_job_submission_info=None, 
               path_translation=None,
               drmaa_implementation=None):
    '''
    Opens a connection to the pool of machines and to the data server L{WorkflowDatabaseServer}.

    @type  database_server: L{WorkflowDatabaseServer}
    @type  parallel_job_submission_info: dictionary 
    @param parallel_job_submission_info: DRMAA doesn't provide an unified way of submitting
    parallel jobs. The value of parallel_job_submission is cluster dependant. 
    The keys are:
      - Drmaa job template attributes 
      - parallel configuration name as defined in soma.jobs.constants
    @type  path_translation: dictionary, namespace => uuid => path
    @param path_translation: for each namespace a dictionary holding the traduction (association uuid => local path)
    '''
    self.logger = logging.getLogger('ljp.drmaajs')
    
    self._drmaa = DrmaaJobs()
    # patch for pbs-torque drmaa ##
    jobTemplateId = self._drmaa.allocateJobTemplate()
    self._drmaa.setCommand(jobTemplateId, "echo", [])
    self._drmaa.setAttribute(jobTemplateId, "drmaa_output_path", "[void]:/tmp/soma-workflow-empty-job.o")
    self._drmaa.setAttribute(jobTemplateId, "drmaa_error_path", "[void]:/tmp/soma-workflow-empty-job.e")
    self._drmaa.runJob(jobTemplateId)
    ################################
    
    self._database_server = database_server

    self.logger.debug("Parallel job submission info: %s", repr(parallel_job_submission_info))
    self._parallel_job_submission_info = parallel_job_submission_info
    self._path_translation = path_translation
    
    self._drmaa_implementation = drmaa_implementation

    try:
      userLogin = pwd.getpwuid(os.getuid())[0] 
    except Exception, e:
      self.logger.critical("Couldn't identify user %s: %s \n" %(type(e), e))
      raise SystemExit
    
    self._user_id = self._database_server.register_user(userLogin) 

    self._jobs = {} # job_id -> job
    self._workflows = {} # workflow_id -> workflow 

    self._lock = threading.RLock()
    
    self._jobsEnded = False
    
    
    
    def startJobStatusUpdateLoop( self, interval ):
      logger_su = logging.getLogger('ljp.drmaajs.su')
      while True:
        # get rid of all the jobs that doesn't exist anymore
        with self._lock:
          server_jobs = self._database_server.get_jobs(self._user_id)
          removed_from_server = set(self._jobs.keys()).difference(server_jobs)
          for job_id in removed_from_server:
            del self._jobs[job_id]
          allJobsEnded = True
          ended = []
          for job in self._jobs.values():
            if job.drmaa_id:
              # get back the status from DRMAA
              job.status = self._drmaa.jobStatus(job.drmaa_id)
              logger_su.debug("job " + repr(job.jobTemplate.job_id) + " : " + job.status)
              if job.status == constants.DONE or job.status == constants.FAILED:
                # update the exit status and information on the job server 
                self.logger.debug("End of job %s, drmaaJobId = %s", job.jobTemplate.job_id, job.drmaa_id)
                job.exit_status, job.exit_value, job.term_sig, resource_usage = self._drmaa.wait(job.drmaa_id, 0)
                self.logger.debug("job " + repr(job.jobTemplate.job_id) + " exit_status " + repr(job.exit_status) + " exit_value " + repr(job.exit_value) + " signal " + repr(job.term_sig))
                str_rusage = ''
                for rusage in resource_usage:
                  str_rusage = str_rusage + rusage + ' '
                self._database_server.set_job_exit_info(job.jobTemplate.job_id, job.exit_status, job.exit_value, job.term_sig, str_rusage)
                ended.append(job.jobTemplate.job_id)
              else:
                allJobsEnded = False
            # update the status on the job server 
            self._database_server.set_job_status(job.jobTemplate.job_id, job.status)

          # get back ended transfers
          endedTransfers = []
          for workflow_id in self._workflows.keys():
            self._database_server.set_workflow_status(workflow_id, constants.WORKFLOW_IN_PROGRESS)
            workflow_ended_transfers = self._database_server.pop_workflow_ended_transfer(workflow_id)
            if workflow_ended_transfers:
              endedTransfers.append((workflow_id, workflow_ended_transfers))
          # get the exit information for terminated jobs and update the database 
          if ended or endedTransfers:
            self._workflowProcessing(endedJobs = ended, endedTransfers = endedTransfers )
          for job_id in ended:
            del self._jobs[job_id]
          self._jobsEnded = len( self._jobs) == 0 
          
        #logger_su.debug("---------- all jobs done : " + repr(self._jobsEnded))
        time.sleep(interval)
    
    
    self._job_status_thread = threading.Thread(name = "job_status_loop", 
                                                target = startJobStatusUpdateLoop, 
                                                args = (self, refreshment_interval))
    self._job_status_thread.setDaemon(True)
    self._job_status_thread.start()


   

  def __del__( self ):
    pass
    '''
    Closes the connection with the pool and the data server L{WorkflowDatabaseServer} and
    stops updating the L{WorkflowDatabaseServer}. (should be called when all the jobs are
    done) 
    '''


  ########## JOB SUBMISSION #################################################

  def submit_job(self, jobTemplate):
    
    '''
    Implementation of the L{WorkflowEngine} method.
    '''
    self.logger.debug(">> submit_job")

    jobTemplateCopy = copy.deepcopy(jobTemplate)
      
    registered_job = self._registerJob(jobTemplateCopy)
    
    self._drmaaJobSubmission(registered_job)
    self.logger.debug("<< submit_job")
    return registered_job.jobTemplate.job_id
  
      
  def _registerJob(self,
                    jobTemplate,
                    workflow_id=-1):

    '''
    @type  jobTemplate: L{constant.Job}
    @param jobTemplate: job submittion information. 
    Note a few restrictions concerning the type of some Job members:
    referenced_input_files, referenced_output_files and command: must be lists of string
    stdout, stderr and stdin: must be strings
    @type  workflow_id: int
    @param workflow_id: workflow id if the job belongs to any, -1 otherwise
    '''
    expiration_date = datetime.now() + timedelta(hours=jobTemplate.disposal_timeout) 
    parallel_config_name = None
    max_node_number = 1

    if not jobTemplate.stdout_file:
      jobTemplate.stdout_file = self._database_server.generate_local_file_path(self._user_id)
      jobTemplate.stderr_file = self._database_server.generate_local_file_path(self._user_id)
      custom_submission = False #the std out and err file has to be removed with the job
    else:
      custom_submission = True #the std out and err file won't to be removed with the job
      jobTemplate.stdout_file = jobTemplate.stdout_file
      jobTemplate.stderr_file = jobTemplate.stderr_file
      
      
    if jobTemplate.parallel_job_info:
      parallel_config_name, max_node_number = jobTemplate.parallel_job_info
      
    if self._drmaa_implementation == "PBS":
      new_command = []
      for command_el in jobTemplate.command:
        command_el = command_el.replace('"', '\\\"')
        new_command.append("\"" + command_el + "\"")
      jobTemplate.command = new_command
      self.logger.debug("PBS case, new command:" + repr(jobTemplate.command))
     
       
    command_info = ""
    for command_element in jobTemplate.command:
      command_info = command_info + " " + repr(command_element)
      
    with self._lock:
      job_id = self._database_server.add_job( soma.workflow.database_server.DBJob(
                                        user_id = self._user_id, 
                                        custom_submission = custom_submission,
                                        expiration_date = expiration_date, 
                                        command = command_info,
                                        workflow_id = workflow_id,
                                        
                                        stdin_file = jobTemplate.stdin, 
                                        join_errout = jobTemplate.join_stderrout,
                                        stdout_file = jobTemplate.stdout_file,
                                        stderr_file = jobTemplate.stderr_file,
                                        working_directory = jobTemplate.working_directory,
                                        
                                        parallel_config_name = parallel_config_name,
                                        max_node_number = max_node_number,
                                        name_description = jobTemplate.name_description))
                                        
                                        
      # create standard output files (not mandatory but useful for "tail -f" kind of function)
      try:  
        tmp = open(jobTemplate.stdout_file, 'w')
        tmp.close()
      except IOError, e:
        raise WorkflowEngineError("Could not create the standard output file %s: %s \n"  %(type(e), e), self.logger)
      if jobTemplate.stderr_file:
        try:
          tmp = open(jobTemplate.stderr_file, 'w')
          tmp.close()
        except IOError, e:
          raise WorkflowEngineError("Could not create the standard error file %s: %s \n"  %(type(e), e), self.logger)
      
                                      
      if jobTemplate.referenced_input_files:
        self._database_server.register_inputs(job_id, jobTemplate.referenced_input_files)
      if jobTemplate.referenced_output_files:
        self._database_server.register_outputs(job_id, jobTemplate.referenced_output_files)

    jobTemplate.job_id = job_id
    jobTemplate.workflow_id = workflow_id
    registered_job = DrmaaWorkflowEngine.RegisteredJob(jobTemplate)
    self._jobs[job_id] = registered_job
    return registered_job
        
  def _drmaaJobSubmission(self, job): 
    '''
    Submit a registered job.
    The job must have been registered in the current DrmaaWorkflowEngine instance.

    @type  job: L{DrmaaWorkflowEngine.RegisteredJob}
    @param job: registered job
    
    '''
    if job not in self._jobs.values():
      raise WorkflowEngineError("A job must be registered before submission.", self.logger)
    
    with self._lock:
      
      drmaaJobId = self._drmaa.allocateJobTemplate()
      self._drmaa.setCommand(drmaaJobId, job.jobTemplate.command[0], job.jobTemplate.command[1:])
    
      self._drmaa.setAttribute(drmaaJobId, "drmaa_output_path", "[void]:" + job.jobTemplate.stdout_file)
      
      if job.jobTemplate.join_stderrout:
        self._drmaa.setAttribute(drmaaJobId,"drmaa_join_files", "y")
      else:
        if job.jobTemplate.stderr_file:
          self._drmaa.setAttribute(drmaaJobId, "drmaa_error_path", "[void]:" + job.jobTemplate.stderr_file)
     
      if job.jobTemplate.stdin:
        self._drmaa.setAttribute(drmaaJobId, "drmaa_input_path", "[void]:" + job.jobTemplate.stdin)
        
      if job.jobTemplate.working_directory:
        self._drmaa.setAttribute(drmaaJobId, "drmaa_wd", job.jobTemplate.working_directory)
      
      if job.jobTemplate.parallel_job_info :
        parallel_config_name, max_node_number = job.jobTemplate.parallel_job_info
        self._setDrmaaParallelJob(drmaaJobId, parallel_config_name, max_node_number)
        
      job_env = []
      for var_name in os.environ.keys():
        job_env.append(var_name+"="+os.environ[var_name])
      
      #job_env = ["LD_LIBRARY_PATH="+os.environ["LD_LIBRARY_PATH"]]
      #job_env.append("PATH="+os.environ["PATH"])
      #self.logger.debug("Environment:")
      #self.logger.debug("  PATH="+os.environ["PATH"])
      #self.logger.debug("  LD_LIBRARY_PATH="+os.environ["LD_LIBRARY_PATH"])
      self._drmaa.setVectorAttribute(drmaaJobId, 'drmaa_v_env', job_env)

      drmaaSubmittedJobId = self._drmaa.runJob(drmaaJobId)
      self._drmaa.deleteJobTemplate(drmaaJobId)
     
      if drmaaSubmittedJobId == "":
        self.logger.error("Could not submit job: Drmaa problem.");
        return -1
      
      self._database_server.set_submission_information(job.jobTemplate.job_id, drmaaSubmittedJobId, datetime.now())
      job.drmaa_id = drmaaSubmittedJobId
      job.status = constants.UNDETERMINED
      
    self.logger.debug("job %s submitted! drmaa id = %s", job.jobTemplate.job_id, job.drmaa_id)
    
    


  def _setDrmaaParallelJob(self, drmaa_job_template_id, configuration_name, max_num_node):
    '''
    Set the DRMAA job template information for a parallel job submission.
    The configuration file must provide the parallel job submission information specific 
    to the cluster in use. 

    @type  drmaa_job_template_id: string 
    @param drmaa_job_template_id: id of drmaa job template
    @type  parallel_job_info: tuple (string, int)
    @param parallel_job_info: (configuration_name, max_node_num)
    configuration_name: type of parallel job as defined in soma.jobs.constants (eg MPI, OpenMP...)
    max_node_num: maximum node number the job requests (on a unique machine or separated machine
    depending on the parallel configuration)
    ''' 

    self.logger.debug(">> _setDrmaaParallelJob")
    if not self._parallel_job_submission_info:
      raise WorkflowEngineError("Configuration file : Couldn't find parallel job submission information.", self.logger)
    
    if configuration_name not in self._parallel_job_submission_info:
      raise WorkflowEngineError("Configuration file : couldn't find the parallel configuration %s." %(configuration_name), self.logger)

    cluster_specific_config_name = self._parallel_job_submission_info[configuration_name]
    
    for drmaa_attribute in constants.PARALLEL_DRMAA_ATTRIBUTES:
      value = self._parallel_job_submission_info.get(drmaa_attribute)
      if value: 
        #value = value.format(config_name=cluster_specific_config_name, max_node=max_num_node)
        value = value.replace("{config_name}", cluster_specific_config_name)
        value = value.replace("{max_node}", repr(max_num_node))
        with self._lock:
          self._drmaa.setAttribute( drmaa_job_template_id, 
                                    drmaa_attribute, 
                                    value)
          self.logger.debug("Parallel job, drmaa attribute = %s, value = %s ", drmaa_attribute, value) 


    job_env = []
    for parallel_env_v in constants.PARALLEL_JOB_ENV:
      value = self._parallel_job_submission_info.get(parallel_env_v)
      if value: job_env.append(parallel_env_v+'='+value.rstrip())
    
    
    with self._lock:
        self._drmaa.setVectorAttribute(drmaa_job_template_id, 'drmaa_v_env', job_env)
        self.logger.debug("Parallel job environment : " + repr(job_env))
        
    self.logger.debug("<< _setDrmaaParallelJob")
    
  def _shared_resource_path_translation(self, urp):
    '''
    @type urp: L{SharedResourcePath}
    @rtype: string
    @returns: path in the local environment
    '''
    if not self._path_translation:
      raise WorkflowEngineError("Configuration file: Couldn't find path translation files.", self.logger)
    if not urp.namespace in self._path_translation.keys():
      raise WorkflowEngineError("Path translation: the namespace %s does'nt exist" %(urp.namespace), self.logger)
    if not urp.uuid in self._path_translation[urp.namespace].keys():
      raise WorkflowEngineError("Path translation: the uuid %s does'nt exist for the namespace %s." %(urp.uuid, urp.namespace), self.logger)
    
    local_path = os.path.join(self._path_translation[urp.namespace][urp.uuid], urp.relative_path)
    return local_path
    

  def delete_job( self, job_id ):
    '''
    Implementation of the L{WorkflowEngine} method.
    '''
    self.logger.debug(">> delete_job %s", job_id)
    with self._lock:
      self.kill_job(job_id)
      self._database_server.delete_job(job_id)
    self.logger.debug("<< delete_job")


  ########## WORKFLOW SUBMISSION ############################################
  
  
  def restart_workflow(self, wf_id):
    
    workflow = self._database_server.get_workflow(wf_id)
    with self._lock:
      to_restart = False
      undone_jobs = []
      for node in workflow.full_nodes:
        if isinstance(node, Job):
          node_status = self._getNodeStatus(node)
          if node_status == DrmaaWorkflowEngine.NODE_FAILED:
            #clear all the information related to the previous job submission
            self._database_server.set_submission_information(node.job_id, None, None)
            self._database_server.set_job_status(node.job_id, constants.NOT_SUBMITTED)
          if node_status != DrmaaWorkflowEngine.NODE_ENDED_WITH_SUCCESS:
            registered_job = DrmaaWorkflowEngine.RegisteredJob(node)
            self._jobs[node.job_id] = registered_job
            undone_jobs.append(node)
           
      if undone_jobs:
        self._workflows[workflow.wf_id] = workflow
        # look for nodes to run
        for job in undone_jobs:
          node_to_run = True # a node is run when all its dependencies succeed
          for dep in workflow.full_dependencies:
            if dep[1] == job: 
              node_status = self._getNodeStatus(dep[0])
              if node_status != DrmaaWorkflowEngine.NODE_ENDED_WITH_SUCCESS:
                node_to_run = False
                
          if node_to_run: 
            self.logger.debug("  ---- Job to run : " + node.name + " " + repr(node.command))
            self._drmaaJobSubmission(self._jobs[job.job_id])

  
  def submit_workflow(self, workflow_o, expiration_date, name):
    # type checking for the workflow ?
    workflow = copy.deepcopy(workflow_o)
    
    #First, do the path translation to check possible errors
    for node in workflow.nodes:
      if isinstance(node, Job):
        for command_el in node.command:
          if isinstance(command_el, SharedResourcePath):
            command_el.local_path = self._shared_resource_path_translation(command_el)
          elif isinstance(command_el, list):
            for list_el in command_el:
              if isinstance(list_el, SharedResourcePath):
                list_el.local_path =  self._shared_resource_path_translation(list_el)
        if node.stdout_file and isinstance(node.stdout_file, SharedResourcePath):
          node.stdout_file = self._shared_resource_path_translation(node.stdout_file)
        if node.stderr_file and isinstance(node.stderr_file, SharedResourcePath):
          node.stderr_file = self._shared_resource_path_translation(node.stderr_file)
        if node.working_directory and isinstance(node.working_directory, SharedResourcePath):
          node.working_directory = self._shared_resource_path_translation(node.working_directory)
        if node.stdin and isinstance(node.stdin, SharedResourcePath):
          node.stdin = self._shared_resource_path_translation(node.stdin)
     ######################################################
 
    workflow_id = self._database_server.add_workflow(self._user_id, expiration_date, name)
    workflow.wf_id = workflow_id 
    workflow.name = name
    
    def assert_is_a_workflow_node(local_path):
      matching_node = None
      for node in workflow.full_nodes:
        if isinstance(node, FileTransfer) and node.local_path == input_file:
          matching_node = node 
          break
      if not matching_node: 
        raise WorkflowEngineError("Workflow submission: The localfile path \"" + repr(local_path) + "\" doesn't match with a workflow FileTransfer node.", self.logger)
      else: 
        return matching_node
    
    if not workflow.full_nodes:
      workflow.full_nodes = set(workflow.nodes)
      # get back the full nodes looking for fileTransfer nodes in the Job node
      for node in workflow.nodes:
        if isinstance(node, Job):
          for ft in node.referenced_input_files:
            if isinstance(ft, FileTransfer):  workflow.full_nodes.add(ft)
          for ft in node.referenced_output_files:
            if isinstance(ft, FileTransfer): workflow.full_nodes.add(ft)
          
    # the missing dependencies between Job and FileTransfer will be added 
    workflow.full_dependencies = set(workflow.dependencies)
   
    w_js = []
    w_fts = []
    # Register FileTransfer to the database server
    for node in workflow.full_nodes:
      if isinstance(node, FileSending):
        if node.remote_paths:
          node.local_path = self._database_server.generate_local_file_path(self._user_id)
          os.mkdir(node.local_path)
        else:
          node.local_path = self._database_server.generate_local_file_path(self._user_id, node.remote_path)
        self._database_server.add_transfer(node.local_path, node.remote_path, expiration_date, self._user_id, constants.READY_TO_TRANSFER, workflow_id, node.remote_paths)
        w_fts.append(node)
      elif isinstance(node, FileRetrieving):
        if node.remote_paths:
          node.local_path = self._database_server.generate_local_file_path(self._user_id)
          os.mkdir(node.local_path)
        else:
          node.local_path = self._database_server.generate_local_file_path(self._user_id, node.remote_path)
        self._database_server.add_transfer(node.local_path, node.remote_path, expiration_date, self._user_id, constants.TRANSFER_NOT_READY, workflow_id, node.remote_paths)
        w_fts.append(node)
      elif isinstance(node, Job):
        w_js.append(node)
    
    # Job attributs conversion and job registration to the database server:
    for job in w_js:
      # command
      new_command = []
      self.logger.debug("job.command " + repr(job.command))
      for command_el in job.command:
        if isinstance(command_el, list):
          new_list = []
          for list_el in command_el:
            if isinstance(list_el, tuple) and  isinstance(list_el[0], FileTransfer):
              new_list.append(os.path.join(list_el[0].local_path, list_el[1])) 
            elif isinstance(list_el, FileTransfer):
              new_list.append(list_el.local_path)
            elif isinstance(list_el, SharedResourcePath):
              new_list.append(list_el.local_path)
            elif isinstance(list_el, str):
              new_list.append(list_el)
            else:
              self.logger.debug("!!!!!!!!!!!!! " + repr(list_el) + " doesn't have an appropriate type !!!")
              ##TBI raise an exception and unregister everything on the server
              ## or do this type checking before any registration on the server ?
              pass
          new_command.append(str(repr(new_list)))
        elif isinstance(command_el, tuple) and isinstance(command_el[0], FileTransfer):
          new_command.append(os.path.join(command_el[0].local_path, command_el[1]))
        elif isinstance(command_el, FileTransfer):
          new_command.append(command_el.local_path)
        elif isinstance(command_el, SharedResourcePath):
          new_command.append(command_el.local_path)
        elif isinstance(command_el, str):
          new_command.append(command_el)
        else:
          self.logger.debug("!!!!!!!!!!!!! " + repr(command_el) + " doesn't have an appropriate type !!!")
          ##TBI raise an exception and unregister everything on the server
          ## or do this type checking before any registration on the server ?
          pass
        
      self.logger.debug("new_command " + repr(new_command))
      job.command = new_command
      
      # referenced_input_files => replace the FileTransfer objects by the corresponding local_path
      new_referenced_input_files = []
      for input_file in job.referenced_input_files:
        if isinstance(input_file, FileTransfer):
          new_referenced_input_files.append(input_file.local_path)
          workflow.full_dependencies.add((input_file, job))
        else: 
          ift_node = assert_is_a_workflow_node(input_file)
          new_referenced_input_files.append(input_file)
          workflow.full_dependencies.add((ift_node, job))
      job.referenced_input_files= new_referenced_input_files
      
      # referenced_input_files => replace the FileTransfer objects by the corresponding local_path
      new_referenced_output_files = []
      for output_file in job.referenced_output_files:
        if isinstance(output_file, FileTransfer):
          new_referenced_output_files.append(output_file.local_path)
          workflow.full_dependencies.add((job, output_file))
        else:
          oft_node = assert_is_a_workflow_node(output_file) 
          # TBI error management !!! => unregister the workflow !!
           new_referenced_output_files.append(output_file)
          workflow.full_dependencies.add((job, oft_node))
      job.referenced_output_files = new_referenced_output_files
      
      # stdin => replace JobTransfer object by corresponding
      if job.stdin:
        if isinstance(job.stdin, FileTransfer):
          job.stdin = job.stdin.local_path 
        
      # Job registration
      registered_job = self._registerJob(job, workflow_id)
      job.job_id = registered_job.jobTemplate.job_id
     
    self._database_server.set_workflow(workflow_id, workflow, self._user_id)
    self._workflows[workflow_id] = workflow
    
    # run nodes without dependencies
    for node in workflow.nodes:
      torun=True
      for dep in workflow.full_dependencies:
        torun = torun and not dep[1] == node
      if torun:
        if isinstance(node, Job):
          self._drmaaJobSubmission(self._jobs[node.job_id])
          
    return workflow
     
  #def _isWFNodeCompleted(self, node):
    #competed = False
    #if isinstance(node, Job):
      #if node.job_id: 
        #completed = True
        #if node.job_id in self._jobs :
          #status = self._jobs[node.job_id].status
          #completed = status == constants.DONE or status == constants.FAILED
    #if isinstance(node, FileSending):
      #if node.local_path:
        #status = self._database_server.get_transfer_status(node.local_path)
        #completed = status == constants.TRANSFERED
    #if isinstance(node, FileRetrieving):
      #if node.local_path:
        #status = self._database_server.get_transfer_status(node.local_path)
        #completed = status == constants.READY_TO_TRANSFER
    #return completed
      
  NODE_NOT_PROCESSED="node_not_processed"
  NODE_IN_PROGRESS="node_in_progress"
  NODE_ENDED_WITH_SUCCESS="node_ended_with_success"
  NODE_FAILED="node_failed"

  def _getNodeStatus(self, node):
    if isinstance(node, Job):
      if not node.job_id:
        return DrmaaWorkflowEngine.NODE_NOT_PROCESSED
      if not node.job_id in self._jobs:
        status = self._database_server.get_job_status(node.job_id)[0]
        if status == constants.DONE:
          job = self._database_server.get_job(node.job_id)
      else:
        job = self._jobs[node.job_id]
        status = job.status
      if status == constants.NOT_SUBMITTED:
        return DrmaaWorkflowEngine.NODE_NOT_PROCESSED
      if status == constants.FAILED:
        return DrmaaWorkflowEngine.NODE_FAILED
      if status == constants.DONE:
        if not job.exit_value == 0 or job.terminating_signal != None or not job.exit_status == constants.FINISHED_REGULARLY:
          return DrmaaWorkflowEngine.NODE_FAILED
        return DrmaaWorkflowEngine.NODE_ENDED_WITH_SUCCESS
      return DrmaaWorkflowEngine.NODE_IN_PROGRESS
    if isinstance(node, FileSending):
      if not node.local_path: return DrmaaWorkflowEngine.NODE_NOT_PROCESSED
      status = self._database_server.get_transfer_status(node.local_path)
      if status == constants.TRANSFERED:
        return DrmaaWorkflowEngine.NODE_ENDED_WITH_SUCCESS
      if status == constants.TRANSFER_NOT_READY or status == constants.READY_TO_TRANSFER:
        return DrmaaWorkflowEngine.NODE_NOT_PROCESSED
      if status == constants.TRANSFERING:
        return DrmaaWorkflowEngine.NODE_IN_PROGRESS
    if isinstance(node, FileRetrieving):
      if not node.local_path: return DrmaaWorkflowEngine.NODE_NOT_PROCESSED
      status = self._database_server.get_transfer_status(node.local_path)
      if status == constants.TRANSFERED or status == constants.READY_TO_TRANSFER:
        return DrmaaWorkflowEngine.NODE_ENDED_WITH_SUCCESS
      if status == constants.TRANSFER_NOT_READY:
        return DrmaaWorkflowEngine.NODE_NOT_PROCESSED
      if status == constants.TRANSFERING:
        return DrmaaWorkflowEngine.NODE_IN_PROGRESS
   
  def _workflowProcessing(self, endedJobs=[], endedTransfers=[]):
    '''
    Explore the submitted workflows to submit jobs and/or change transfer status.

    @type  endedJobs: list of job id
    @param endedJobs: list of the ended jobs
    @type  endedTransfers: sequence of tuple (workflow_id, set of local_file_path)
    @param endedTransfers: list of ended transfers for each workflow
    '''
    self.logger.debug(">> workflowProcessing")

    with self._lock:
      wf_to_process = set([])
      for job_id in endedJobs:
        job = self._jobs[job_id]
        self.logger.debug("  ==> ended job: " + repr(job.jobTemplate.name))     
      
        if job.jobTemplate.referenced_output_files:
          for local_path in job.jobTemplate.referenced_output_files:
            self._database_server.set_transfer_status(local_path, constants.READY_TO_TRANSFER)
            
        if not job.jobTemplate.workflow_id == -1 and job.jobTemplate.workflow_id in self._workflows:
          workflow = self._workflows[job.jobTemplate.workflow_id]
          wf_to_process.add(workflow)

      for workflow_id, w_ended_transfers in endedTransfers:
        for local_path in w_ended_transfers:
          self.logger.debug("  ==> ended Transfer: " + local_path + " workflow_id " + repr(workflow_id))
          workflow = self._workflows[workflow_id]
          wf_to_process.add(workflow)
        
      to_run = []
      to_abort = set([])
      for workflow in wf_to_process:
        for node in workflow.full_nodes:
          if isinstance(node, Job):
            to_inspect = False
            if node.job_id in self._jobs:
              status = self._jobs[node.job_id].status
              to_inspect = status == constants.NOT_SUBMITTED
            #print "node " + node.name + " status " + status[0] + " to inspect " + repr(to_inspect)
          if isinstance(node, FileTransfer):
            status = self._database_server.get_transfer_status(node.local_path)
            to_inspect = status == constants.TRANSFER_NOT_READY
            #print "node " + node.name + " status " + status + " to inspect " + repr(to_inspect)
          if to_inspect:
            #self.logger.debug("  -- to inspect : " + node.name)
            node_to_run = True # a node is run when all its dependencies succeed
            for dep in workflow.full_dependencies:
              if dep[1] == node: 
                #self.logger.debug("   node " + node.name + " dep: " + dep[0].name + " ---> " + dep[1].name)
                node_status = self._getNodeStatus(dep[0])
                #self.logger.debug("   dep[0] status" + repr(node_status))
                if node_status != DrmaaWorkflowEngine.NODE_ENDED_WITH_SUCCESS:
                  node_to_run = False
                  if isinstance(dep[1], Job) and isinstance(dep[0], Job) and not dep[1] in to_abort and node_status == DrmaaWorkflowEngine.NODE_FAILED  :
                      to_abort.add(dep[1]) 
                      break
                  
            if node_to_run: 
              if isinstance(node, Job):
                self.logger.debug("  ---- Job to run : " + node.name + " " + repr(node.command))
              to_run.append(node)
        
      for node in to_run:
        if isinstance(node, Job):
          self._drmaaJobSubmission(self._jobs[node.job_id])
        if isinstance(node,FileTransfer):
          self._database_server.set_transfer_status(node.local_path, constants.READY_TO_TRANSFER)
      
      # if a job fails the whole workflow branch has to be stopped
      # look for the node in the branch to abort
      previous_size = 0
      while previous_size != len(to_abort):
        previous_size = len(to_abort)
        for dep in workflow.full_dependencies:
          if isinstance(dep[1], Job) and dep[0] in to_abort and not dep[1] in to_abort:
            to_abort.add(dep[1])
            break
          
      # stop the whole branch
      for node in to_abort:
        if isinstance(node, Job) and node.job_id in self._jobs.keys():
          self.logger.debug("  ---- Failure: job to abort " + node.name)
          assert(self._jobs[node.job_id].status == constants.NOT_SUBMITTED)
          self._database_server.set_job_status(node.job_id, constants.FAILED)
          self._database_server.set_job_exit_info(node.job_id, constants.EXIT_ABORTED, None, None, None)
          del self._jobs[node.job_id]
        
    # delete ended workflows:
    finished_workfows = []
    for workflow_id, workflow in self._workflows.items():
      finished = True
      for node in workflow.full_nodes:
        if isinstance(node, Job):
          node_status = self._getNodeStatus(node)
          finished = finished and (node_status == DrmaaWorkflowEngine.NODE_ENDED_WITH_SUCCESS or node_status == DrmaaWorkflowEngine.NODE_FAILED)
          if not finished: break
      if finished: 
        finished_workfows.append(workflow_id)
    for workflow_id in finished_workfows:
      self.logger.debug("  ~~~~ END OF WORKFLOW " + repr(workflow_id) + " ~~~~")
      del self._workflows[workflow_id]
      self._database_server.set_workflow_status(workflow_id, constants.WORKFLOW_DONE)
    self.logger.debug("<<< workflowProcessing")
    


  ########### DRMS MONITORING ################################################

  def areJobsDone(self):
    return self._jobsEnded
    
  ########## JOB CONTROL VIA DRMS ########################################
  

  def stop_job( self, job_id ):
    '''
    Implementation of the L{WorkflowEngine} method.
    '''
    self.logger.debug(">> stop_job")
    status_changed = False
    with self._lock:
      if job_id in self._jobs:
        drmaaJobId = self._jobs[job_id].drmaa_id
      else:
        drmaaJobId = self._database_server.get_drmaa_job_id(job_id)
      if drmaaJobId:
        status = self._drmaa.jobStatus(drmaaJobId) 
        self.logger.debug("   status : " + status)
        if status==constants.RUNNING:
          self._drmaa.suspend(drmaaJobId)
          status_changed = True
        
        if status==constants.QUEUED_ACTIVE:
          self._drmaa.hold(drmaaJobId)
          status_changed = True
        
        
    if status_changed:
      self._waitForStatusUpdate(job_id)
    self.logger.debug("<< stop_job")
    
    
  def restart_job( self, job_id ):
    '''
    Implementation of the L{WorkflowEngine} method.
    '''
    self.logger.debug(">> restart_job")
    status_changed = False
    with self._lock:
      if job_id in self._jobs:
        drmaaJobId = self._jobs[job_id].drmaa_id
      else:
        drmaaJobId = self._database_server.get_drmaa_job_id(job_id)
      if drmaaJobId:
        status = self._drmaa.jobStatus(drmaaJobId) 
        
        if status==constants.USER_SUSPENDED or status==constants.USER_SYSTEM_SUSPENDED:
          self._drmaa.resume(drmaaJobId)
          status_changed = True
          
        if status==constants.USER_ON_HOLD or status==constants.USER_SYSTEM_ON_HOLD :
          self._drmaa.release(drmaaJobId)
          status_changed = True
        
    if status_changed:
      self._waitForStatusUpdate(job_id)
    self.logger.debug("<< restart_job")
    
  


  def kill_job( self, job_id ):
    '''
    Implementation of the L{WorkflowEngine} method.
    '''
    self.logger.debug(">> kill_job")
        
    with self._lock:
      (status, last_status_update) = self._database_server.get_job_status(job_id)

      if status and not status == constants.DONE and not status == constants.FAILED:
        drmaaJobId = self._database_server.get_drmaa_job_id(job_id)
        if drmaaJobId:
          self.logger.debug("terminate job %s drmaa id %s with status %s", job_id, drmaaJobId, status)
          self._drmaa.terminate(drmaaJobId)
        
          self._database_server.set_job_exit_info(job_id, 
                                          constants.USER_KILLED,
                                          None,
                                          None,
                                          None)
          
          self._database_server.set_job_status(job_id, constants.FAILED)
        if job_id in self._jobs.keys():
          del self._jobs[job_id]
        
    self.logger.debug("<< kill_job")


  def _waitForStatusUpdate(self, job_id):
    
    self.logger.debug(">> _waitForStatusUpdate")
    drmaaActionTime = datetime.now()
    time.sleep(refreshment_interval)
    (status, last_status_update) = self._database_server.get_job_status(job_id)
    while status and not status == constants.DONE and not status == constants.FAILED and last_status_update < drmaaActionTime:
      time.sleep(refreshment_interval)
      (status, last_status_update) = self._database_server.get_job_status(job_id) 
      if last_status_update and datetime.now() - last_status_update > timedelta(seconds = refreshment_interval*5):
        raise WorkflowEngineError('Could not get back status of job %s. The process updating its status failed.' %(job_id), self.logger)
    self.logger.debug("<< _waitForStatusUpdate")


class WorkflowEngine(object):
  
  def __init__( self, 
                database_server, 
                drmaa_workflow_engine = None,  
                parallel_job_submission_info = None, 
                path_translation = None,
                drmaa_implementation=None):
    ''' 
    @type  database_server: L{WorkflowDatabaseServer}
    @type  drmaa_workflow_engine: L{DrmaaWorkflowEngine} or None
    @param drmaa_workflow_engine: object of type L{DrmaaWorkflowEngine} to 
    delegate all the tasks related to the DRMS and management of submitted 
    jobs and workflow. If None a new instance is created.
    '''
    
    self.logger = logging.getLogger('ljp.js')
    
    Pyro.core.initClient()

    if drmaa_workflow_engine:
      self._drmaa_wf_engine = drmaa_workflow_engine
    else:
      self._drmaa_wf_engine = DrmaaWorkflowEngine(database_server,      
                                                   parallel_job_submission_info, path_translation,
                                                   drmaa_implementation)
    
    self._database_server= database_server
    
    try:
      user_login = pwd.getpwuid(os.getuid())[0]
    except Exception, e:
      raise WorkflowEngineError("Couldn't identify user %s: %s \n" %(type(e), e), self.logger)
    
    self._user_id = self._database_server.register_user(user_login)
    

  def __del__( self ):
    pass

  ########## FILE TRANSFER ###############################################
  
  '''
  For the following methods:
    Local means that it is located on a directory shared by the machine of the pool
    Remote means that it is located on a remote machine or on any directory 
    owned by the user. 
    A transfer will associate remote file path to unique local file path.
  
  Use L{register_transfer} then L{writeLine} or scp or 
  shutil.copy to transfer input file from the remote to the local 
  environment.
  Use L{register_transfer} and once the job has run use L{readline} or scp or
  shutil.copy to transfer the output file from the local to the remote environment.
  '''

  def register_transfer(self, remote_path, disposal_timeout=168, remote_paths = None): 
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
    if remote_paths:
      local_path = self._database_server.generate_local_file_path(self._user_id)
      os.mkdir(local_path)
    else:
      local_path = self._database_server.generate_local_file_path(self._user_id, remote_path)
    expirationDate = datetime.now() + timedelta(hours=disposal_timeout) 
    self._database_server.add_transfer(local_path, remote_path, expirationDate, self._user_id, -1, remote_paths)
    return local_path
  
  @staticmethod
  def _contents(path_seq, md5_hash=False):
    result = []
    for path in path_seq:
      s = os.stat(path)
      if stat.S_ISDIR(s.st_mode):
        full_path_list = []
        for element in os.listdir(path):
          full_path_list.append(os.path.join(path, element))
        contents = WorkflowEngine._contents(full_path_list, md5_hash)
        result.append((os.path.basename(path), contents, None))
      else:
        if md5_hash:
          result.append( ( os.path.basename(path), s.st_size, hashlib.md5( open( path, 'rb' ).read() ).hexdigest() ) )
        else:
          result.append( ( os.path.basename(path), s.st_size, None ) )
    return result


  def initializeRetrivingTransfer(self, local_path):
    local_path, remote_path, expiration_date, workflow_id, remote_paths = self.transferInformation(local_path)
    status = self.transfer_status(local_path)
    if status != constants.READY_TO_TRANSFER:
      # raise ?
      return (None, None)
    contents = None
    transfer_action_info = None
    if not remote_paths:
      if os.path.isfile(local_path):
        stat = os.stat(local_path)
        file_size = stat.st_size
        md5_hash = hashlib.md5( open( local_path, 'rb' ).read() ).hexdigest() 
        transfer_action_info = (file_size, md5_hash, constants.FILE_RETRIEVING)
      elif os.path.isdir(local_path):
        contents = WorkflowEngine._contents([local_path])
        (cumulated_file_size, dir_element_action_info) = self._initializeDirectory(local_path, contents)
        transfer_action_info = (cumulated_file_size, dir_element_action_info, constants.DIR_RETRIEVING)
    else: #remote_paths
      full_path_list = []
      for element in os.listdir(local_path):
        full_path_list.append(os.path.join(local_path, element))
      contents = WorkflowEngine._contents(full_path_list)
      (cumulated_file_size, dir_element_action_info) = self._initializeDirectory(local_path, contents)
      transfer_action_info = (cumulated_file_size, dir_element_action_info, constants.DIR_RETRIEVING)

    self._database_server.set_transfer_action_info(local_path, transfer_action_info)     
    return (transfer_action_info, contents)

  def initializeFileSending(self, local_path, file_size, md5_hash = None):
    '''
    Initialize the transfer of a file.
    '''
    transfer_action_info = (file_size, md5_hash, constants.FILE_SENDING)
    f = open(local_path, 'w')
    f.close()
    self._database_server.set_transfer_status(local_path, constants.TRANSFERING)
    self._database_server.set_transfer_action_info(local_path, transfer_action_info)
    return transfer_action_info


  def _initializeDirectory(self, local_path, contents, subdirectory = ""):
    '''
    Initialize local directory from the contents of remote directory.

    @rtype : tuple (int, dictionary)
    @return : (cumulated file size, dictionary : relative file path => (file_size, md5_hash))
    '''
    dir_element_action_info = {}
    cumulated_file_size = 0
    for item, description, md5_hash in contents:
      relative_path = os.path.join(subdirectory,item)
      full_path = os.path.join(local_path, relative_path)
      if isinstance(description, list):
        #os.mkdir(full_path)
        sub_size, sub_dir_element_action_info = self._initializeDirectory( local_path, description, relative_path)
        cumulated_file_size += sub_size
        dir_element_action_info.update(sub_dir_element_action_info)
      else:
        file_size = description
        dir_element_action_info[relative_path] = (file_size, md5_hash)
        cumulated_file_size += file_size

    return (cumulated_file_size, dir_element_action_info)

  def _createDirStructure(self, local_path, contents, subdirectory = ""):
    for item, description, md5_hash in contents:
      relative_path = os.path.join(subdirectory,item)
      full_path = os.path.join(local_path, relative_path)
      if isinstance(description, list):
        if not os.path.isdir(full_path):
          os.mkdir(full_path)
        self._createDirStructure( local_path, description, relative_path)
    

  def initializeDirSending(self, local_path, contents):
    '''
    Initialize the transfer of a directory.
    '''
    cumulated_file_size, dir_element_action_info = self._initializeDirectory(local_path, contents)
    transfer_action_info = (cumulated_file_size, dir_element_action_info, constants.DIR_SENDING)
    self._createDirStructure(local_path, contents)
    self._database_server.set_transfer_status(local_path, constants.TRANSFERING)
    self._database_server.set_transfer_action_info(local_path, transfer_action_info)
    return transfer_action_info
    
  
  def _sendToFile(self, local_path, data, file_size, md5_hash = None):
    '''
    @rtype: boolean
    @return: transfer ended
    '''
    file = open(local_path, 'ab')
    file.write(data)
    fs = file.tell()
    file.close()
    if fs > file_size:
      # Reset file_size   
      open(local_path, 'wb')
      raise WorkflowEngineError('send_piece: Transmitted data exceed expected file size.')
    elif fs == file_size:
      if md5_hash is not None:
        if hashlib.md5( open( local_path, 'rb' ).read() ).hexdigest() != md5_hash:
          # Reset file
          open( local_path, 'wb' )
          raise WorkflowEngineError('send_piece: Transmission error detected.')
        else:
          return True
      else:
        return True
    else:
      return False

  def send_piece(self, local_path, data, relative_path=None):
    '''
    @rtype : boolean
    @return : transfer ended
    '''
    if not relative_path:
      # File case
      (file_size, md5_hash, transfer_type) = self._database_server.get_transfer_action_info(local_path)
      transfer_ended = self._sendToFile(local_path, data, file_size, md5_hash)
      if transfer_ended:
        self._database_server.set_transfer_status(local_path, constants.TRANSFERED)
        self.signalTransferEnded(local_path)
      
    else:
      # Directory case
      (cumulated_size, dir_element_action_info, transfer_type) = self._database_server.get_transfer_action_info(local_path)
      if not relative_path in dir_element_action_info:
        raise WorkflowEngineError('send_piece: the file %s doesn t belong to the transfer %s' %(relative_path, local_path))
      (file_size, md5_hash) = dir_element_action_info[relative_path]
      transfer_ended = self._sendToFile(os.path.join(local_path, relative_path), data, file_size, md5_hash)
      
      if transfer_ended:
        cumulated_file_size, cumulated_transmissions, files_transfer_status = self.transferProgressionStatus(local_path, (cumulated_size, dir_element_action_info, transfer_type))
        if cumulated_transmissions == cumulated_file_size:
          self._database_server.set_transfer_status(local_path, constants.TRANSFERED)
          self.signalTransferEnded(local_path)
      
    return transfer_ended
  

  def retrieve_piece(self, local_path, buffer_size, transmitted, relative_path = None):
    '''
    Implementation of the L{Jobs} method.
    '''
    if relative_path:
      local_full_path = os.path.join(local_path, relative_path)
    else:
      local_full_path = local_path
      
    f = open(local_full_path, 'rb')
    if transmitted:
      f.seek(transmitted)
    data = f.read(buffer_size)
    f.close()
    return data

    
  def set_transfer_status(self, local_path, status):
    '''
    Set a transfer status. 
    '''
     
    if not self._database_server.is_user_transfer(local_path, self._user_id) :
      #print "Couldn't set transfer status %s. It doesn't exist or is not owned by the current user \n" % local_path
      return
    
    self._database_server.set_transfer_status(local_path, status)

  def delete_transfer(self, local_path):
    '''
    Implementation of the L{Jobs} method.
    '''
    
    if not self._database_server.is_user_transfer(local_path, self._user_id) :
      #print "Couldn't cancel transfer %s. It doesn't exist or is not owned by the current user \n" % local_path
      return

    self._database_server.remove_transfer(local_path)
    
  def signalTransferEnded(self, local_path):
    '''
    Has to be called each time a file transfer ends for the 
    workflows to be proceeded.
    '''
    workflow_id = self._database_server.get_transfer_information(local_path)[3]
    if workflow_id != -1:
      self._database_server.add_workflow_ended_transfer(workflow_id, local_path)
    

  ########## JOB SUBMISSION ##################################################

  
  def submit_job( self,
              jobTemplate):
    '''
    Submits a job to the system. 
    
    @type  jobTemplate: L{Job}
    @param jobTemplate: job informations 
    '''

    if len(jobTemplate.command) == 0:
      raise WorkflowEngineError("Submission error: the command must contain at least one element \n", self.logger)

    # check the required_local_input_files, required_local_output_file and stdin ?
    
    
    
    job_id = self._drmaa_wf_engine.submit_job(jobTemplate)
    
    return job_id




  def delete_job( self, job_id ):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
    
    if not self._database_server.is_user_job(job_id, self._user_id):
      #print "Couldn't delete job %d. It doesn't exist or is not owned by the current user \n" % job_id
      return 
    
    self._drmaa_wf_engine.delete_job(job_id)


  ########## WORKFLOW SUBMISSION ############################################
  
  def submit_workflow(self, workflow, expiration_date, name):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
    if not expiration_date:
      expiration_date = datetime.now() + timedelta(days=7)
    return self._drmaa_wf_engine.submit_workflow(workflow, expiration_date, name)
  
  def delete_workflow(self, workflow_id):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
    if not self._database_server.is_user_workflow(workflow_id, self._user_id):
      #print "Couldn't delete workflow %d. It doesn't exist or is not owned by the current user \n" % job_id
      return
    
    self._database_server.delete_workflow(workflow_id)
    
  def change_workflow_expiration_date(self, workflow_id, new_expiration_date):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
    if not self._database_server.is_user_workflow(workflow_id, self._user_id):
      #print "Couldn't delete workflow %d. It doesn't exist or is not owned by the current user \n" % job_id
      return False
    
    if new_expiration_date < datetime.now(): 
      return False
    # TO DO: Add other rules?
    
    self._database_server.change_workflow_expiration_date(workflow_id, new_expiration_date)
    return True

  def restart_workflow(self, workflow_id):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
    if not self._database_server.is_user_workflow(workflow_id, self._user_id):
      #print "Couldn't restart workflow %d. It doesn't exist or is not owned by the current user \n" % job_id
      return False
    
    (status, last_status_update) = self._database_server.get_workflow_status(workflow_id)
    
    
    print "last status update too old = " + repr((datetime.now() - last_status_update < timedelta(seconds=refreshment_interval*5))) 
    if not last_status_update or (datetime.now() - last_status_update < timedelta(seconds=refreshment_interval*5) and status != constants.WORKFLOW_DONE):
      return False
    
    
    print "self._drmaa_wf_engine.restart_workflow(workflow_id)"
    self._drmaa_wf_engine.restart_workflow(workflow_id)
    return True
    
   
  ########## SERVER STATE MONITORING ########################################


  def jobs(self):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
    return self._database_server.get_jobs(self._user_id)
    
  def transfers(self):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
    return self._database_server.get_transfers(self._user_id)
  
  
  def workflows(self):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
    return self._database_server.get_workflows(self._user_id)
  
  def workflow(self, wf_id):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
    if not self._database_server.is_user_workflow(wf_id, self._user_id):
      #print "Couldn't get workflow %d. It doesn't exist or is owned by a different user \n" %wf_id
      return
    return self._database_server.get_workflow(wf_id)

 
  def workflowInformation(self, wf_id):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
    if not self._database_server.is_user_workflow(wf_id, self._user_id):
      #print "Couldn't get workflow %d. It doesn't exist or is owned by a different user \n" %wf_id
      return
    return self._database_server.get_workflow_info(wf_id)
    
    
 
  def transferInformation(self, local_path):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
    #TBI raise an exception if local_path is not valid transfer??
    
    if not self._database_server.is_user_transfer(local_path, self._user_id):
      #print "Couldn't get transfer information of %s. It doesn't exist or is owned by a different user \n" % local_path
      return
      
    return self._database_server.get_transfer_information(local_path)
   


  def job_status( self, job_id ):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
    if not self._database_server.is_user_job(job_id, self._user_id):
      #print "Could get the job status of job %d. It doesn't exist or is owned by a different user \n" %job_id
      return
    
    return self._database_server.get_job_status(job_id)[0]
        
  
  def workflow_nodes_status(self, wf_id, groupe = None):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
    if not self._database_server.is_user_workflow(wf_id, self._user_id):
      #print "Couldn't get workflow %d. It doesn't exist or is owned by a different user \n" %wf_id
      return
    wf_status = self._database_server.get_detailed_workflow_status(wf_id)
    return wf_status
        
        
  def transfer_status(self, local_path):
    if not self._database_server.is_user_transfer(local_path, self._user_id):
      #print "Could not get the job status the transfer associated with %s. It doesn't exist or is owned by a different user \n" %local_path
      return
    transfer_status = self._database_server.get_transfer_status(local_path)  
    return transfer_status
        
  def transferActionInfo(self,local_path):
    return self._database_server.get_transfer_action_info(local_path)
 
  def transferProgressionStatus(self, local_path, transfer_action_info):
    if transfer_action_info[2] == constants.FILE_SENDING:
      (file_size, md5_hash, transfer_type) = transfer_action_info
      transmitted = os.stat(local_path).st_size
      return (file_size, transmitted)
    elif transfer_action_info[2] == constants.DIR_SENDING:
      (cumulated_file_size, dir_element_action_info, transfer_type) = transfer_action_info
      files_transfer_status = []
      for relative_path, (file_size, md5_hash) in dir_element_action_info.iteritems():
        full_path = os.path.join(local_path, relative_path)
        if os.path.isfile(full_path):
          transmitted = os.stat(full_path).st_size
        else:
          transmitted = 0
        files_transfer_status.append((relative_path, file_size, transmitted))
      cumulated_transmissions = reduce( operator.add, (i[2] for i in files_transfer_status) )
      return (cumulated_file_size, cumulated_transmissions, files_transfer_status)
    else:
      return None
    

  def job_termination_status(self, job_id ):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
  
    if not self._database_server.is_user_job(job_id, self._user_id):
      #print "Could get the exit information of job %d. It doesn't exist or is owned by a different user \n" %job_id
      return
  
    dbJob = self._database_server.get_job(job_id)
    exit_status = dbJob.exit_status
    exit_value = dbJob.exit_value
    terminating_signal =dbJob.terminating_signal
    resource_usage = dbJob.resource_usage
    
    return (exit_status, exit_value, terminating_signal, resource_usage)
    
 
  def jobInformation(self, job_id):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
    
    if not self._database_server.is_user_job(job_id, self._user_id):
      #print "Could get information about job %d. It doesn't exist or is owned by a different user \n" %job_id
      return
    
    dbJob = self._database_server.get_job(job_id)
    name_description = dbJob.name_description 
    command = dbJob.command
    submission_date = dbJob.submission_date
    
    return (name_description, command, submission_date)
    
  def getStdOutErrTransferActionInfo(self, job_id):
    if not self._database_server.is_user_job(job_id, self._user_id):
      return
    
    stdout_file, stderr_file = self._database_server.get_std_out_err_file_path(job_id)
    
    stdout_transfer_action_info = None
    stderr_transfer_action_info = None
    if stdout_file and os.path.isfile(stdout_file):
      stat = os.stat(stdout_file)
      stdout_file_size = stat.st_size
      stdout_md5_hash = hashlib.md5(open(stdout_file, 'rb' ).read()).hexdigest()
      stdout_transfer_action_info = (stdout_file_size, stdout_md5_hash)
    if stderr_file and os.path.isfile(stderr_file):
      stat = os.stat(stderr_file)
      stderr_file_size = stat.st_size
      stderr_md5_hash = hashlib.md5(open(stderr_file, 'rb').read()).hexdigest()
      stderr_transfer_action_info = (stderr_file_size, stderr_md5_hash)
    
    return (stdout_file, stdout_transfer_action_info, stderr_file, stderr_transfer_action_info)
    
  ########## JOB CONTROL VIA DRMS ########################################
  
  
  def wait_job( self, job_ids, timeout = -1):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
    for jid in job_ids:
      if not self._database_server.is_user_job(jid, self._user_id):
        raise WorkflowEngineError( "Could not wait for job %d. It doesn't exist or is owned by a different user \n" %jid, self.logger)
      
    #self._drmaa_wf_engine.wait(job_ids, timeout)
    self.logger.debug("        waiting...")
    
    waitForever = timeout < 0
    startTime = datetime.now()
    for jid in job_ids:
      (status, last_status_update) = self._database_server.get_job_status(jid)
      if status:
        self.logger.debug("wait        job %s status: %s", jid, status)
        delta = datetime.now()-startTime
        delta_status_update = datetime.now() - last_status_update
        while status and not status == constants.DONE and not status == constants.FAILED and (waitForever or delta < timedelta(seconds=timeout)):
          time.sleep(refreshment_interval)
          (status, last_status_update) = self._database_server.get_job_status(jid) 
          self.logger.debug("wait        job %s status: %s last update %s, now %s", jid, status, repr(last_status_update), repr(datetime.now()))
          delta = datetime.now() - startTime
          if last_status_update and datetime.now() - last_status_update > timedelta(seconds = refreshment_interval*10):
            raise WorkflowEngineError('Could not wait for job %s. The process updating its status failed.' %(jid), self.logger)
    

  def stop_job( self, job_id ):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
    if not self._database_server.is_user_job(job_id, self._user_id):
      raise WorkflowEngineError( "Could not stop job %d. It doesn't exist or is owned by a different user \n" %job_id, self.logger)
    
    self._drmaa_wf_engine.stop_job(job_id)
   
  
  
  def restart_job( self, job_id ):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
    if not self._database_server.is_user_job(job_id, self._user_id):
      raise WorkflowEngineError( "Could not restart job %d. It doesn't exist or is owned by a different user \n" %job_id, self.logger)
    
    self._drmaa_wf_engine.restart_job(job_id)


  def kill_job( self, job_id ):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''

    if not self._database_server.is_user_job(job_id, self._user_id):
      raise WorkflowEngineError( "Could not kill job %d. It doesn't exist or is owned by a different user \n" %job_id, self.logger)
    
    self._drmaa_wf_engine.kill_job(job_id)


