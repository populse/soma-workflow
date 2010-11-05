'''
@author: Yann Cointepas
@author: Soizic Laguitton
@organization: U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''


from __future__ import with_statement
from soma.pipeline.somadrmaajobssip import DrmaaJobs
import Pyro.naming, Pyro.core
from Pyro.errors import NamingError
from datetime import date
from datetime import timedelta
import pwd
import os
import threading
import time
from datetime import datetime
import logging
import soma.jobs.constants as constants
from soma.jobs.jobClient import JobTemplate, FileTransfer, FileSending, FileRetrieving, Workflow, UniversalResourcePath
import soma.jobs.jobServer 
import copy
import stat, hashlib, operator

__docformat__ = "epytext en"


refreshment_interval = 1 #seconds

class WorkflowEngineError( Exception ): 
  def __init__(self, msg, logger = None):
    self.args = (msg,)
    if logger:
      logger.critical('EXCEPTION ' + msg)


class DrmaaWorkflowEngine( object ):

  '''
  Instances of this class opens a DRMAA session and allows to submit and control 
  the jobs. It updates constantly the jobs status on the L{JobServer}. 
  The L{DrmaaJobScheduler} must be created on one of the machine which is allowed
  to submit jobs by the DRMS.
  '''

  class RegisteredJob(object):
    '''
    Job representation in DrmaaJobScheduler.
    '''
    def __init__(self, jobTemplate):
      '''
      @type  jobTemplate: L{constant.JobTemplate}
      @param jobTemplate: job submittion information. 
      Note a few restrictions concerning the type of some JobTemplate members:
      referenced_input_files, referenced_output_files and command: must be lists of string
      stdout, stderr and stdin: must be strings
      job_id must be a valid job id registered in the JobServer
      @type  drmaa_id: string or None
      @param drmaa_id: submitted job DRMAA identifier, None if the job is not submitted
      @type  status: string
      @param status: job status as defined in constants.JOB_STATUS
      @type  last_status_update: date
      @param last_status_update: last status update date
      @type  exit_status: string 
      @param exit_status: exit status string as defined in L{JobServer}
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


  def __init__( self, job_server, parallel_job_submission_info = None, universal_path_translation = None):
    '''
    Opens a connection to the pool of machines and to the data server L{JobServer}.

    @type  job_server: L{JobServer}
    @type  parallel_job_submission_info: dictionary 
    @param parallel_job_submission_info: DRMAA doesn't provide an unified way of submitting
    parallel jobs. The value of parallel_job_submission is cluster dependant. 
    The keys are:
      - Drmaa job template attributes 
      - parallel configuration name as defined in soma.jobs.constants
    @type  universal_path_translation: dictionary, namespace => uuid => path
    @param universal_path_translation: for each namespace a dictionary holding the traduction (association uuid => local path)
    '''
    self.logger = logging.getLogger('ljp.drmaajs')
    
    self.__drmaa = DrmaaJobs()
    # patch for pbs-torque drmaa ##
    jobTemplateId = self.__drmaa.allocateJobTemplate()
    self.__drmaa.setCommand(jobTemplateId, "echo", [])
    self.__drmaa.setAttribute(jobTemplateId, "drmaa_output_path", "[void]:/dev/null")
    self.__drmaa.setAttribute(jobTemplateId, "drmaa_error_path", "[void]:/dev/null")
    self.__drmaa.runJob(jobTemplateId)
    ################################
    
    self.__jobServer = job_server

    self.logger.debug("Parallel job submission info: %s", repr(parallel_job_submission_info))
    self.__parallel_job_submission_info = parallel_job_submission_info
    self.__universal_path_translation = universal_path_translation

    try:
      userLogin = pwd.getpwuid(os.getuid())[0] 
    except Exception, e:
      self.logger.critical("Couldn't identify user %s: %s \n" %(type(e), e))
      raise SystemExit
    
    self.__user_id = self.__jobServer.registerUser(userLogin) 

    self.__jobs = {} # job_id -> job
    self.__workflows = {} # workflow_id -> workflow 

    self.__lock = threading.RLock()
    
    self.__jobsEnded = False
    
    
    
    def startJobStatusUpdateLoop( self, interval ):
      logger_su = logging.getLogger('ljp.drmaajs.su')
      while True:
        # get rid of all the jobs that doesn't exist anymore
        with self.__lock:
          serverJobs = self.__jobServer.getJobs(self.__user_id)
          removed_from_server = set(self.__jobs.keys()).difference(serverJobs)
          for job_id in removed_from_server:
            del self.__jobs[job_id]
          allJobsEnded = True
          ended = []
          for job in self.__jobs.values():
            if job.drmaa_id:
              # get back the status from DRMAA
              job.status = self.__drmaa.jobStatus(job.drmaa_id)
              logger_su.debug("job " + repr(job.jobTemplate.job_id) + " : " + job.status)
              if job.status == constants.DONE or job.status == constants.FAILED:
                # update the exit status and information on the job server 
                self.logger.debug("End of job %s, drmaaJobId = %s", job.jobTemplate.job_id, job.drmaa_id)
                job.exit_status, job.exit_value, job.term_sig, resource_usage = self.__drmaa.wait(job.drmaa_id, 0)
                self.logger.debug("job " + repr(job.jobTemplate.job_id) + " exit_status " + repr(job.exit_status) + " exit_value " + repr(job.exit_value) + " signal " + repr(job.term_sig))
                str_rusage = ''
                for rusage in resource_usage:
                  str_rusage = str_rusage + rusage + ' '
                self.__jobServer.setJobExitInfo(job.jobTemplate.job_id, job.exit_status, job.exit_value, job.term_sig, str_rusage)
                ended.append(job.jobTemplate.job_id)
              else:
                allJobsEnded = False
            # update the status on the job server 
            self.__jobServer.setJobStatus(job.jobTemplate.job_id, job.status)

          # get back ended transfers
          endedTransfers = []
          for workflow_id in self.__workflows.keys():
            self.__jobServer.setWorkflowStatus(workflow_id, constants.WORKFLOW_IN_PROGRESS)
            workflow_ended_transfers = self.__jobServer.popWorkflowEndedTransfer(workflow_id)
            if workflow_ended_transfers:
              endedTransfers.append((workflow_id, workflow_ended_transfers))
          # get the exit information for terminated jobs and update the jobServer
          if ended or endedTransfers:
            self.__workflowProcessing(endedJobs = ended, endedTransfers = endedTransfers )
          for job_id in ended:
            del self.__jobs[job_id]
          self.__jobsEnded = len( self.__jobs) == 0 
          
        logger_su.debug("---------- all jobs done : " + repr(self.__jobsEnded))
        time.sleep(interval)
    
    
    self.__job_status_thread = threading.Thread(name = "job_status_loop", 
                                                target = startJobStatusUpdateLoop, 
                                                args = (self, refreshment_interval))
    self.__job_status_thread.setDaemon(True)
    self.__job_status_thread.start()


   

  def __del__( self ):
    pass
    '''
    Closes the connection with the pool and the data server L{JobServer} and
    stops updating the L{JobServer}. (should be called when all the jobs are
    done) 
    '''


  ########## JOB SUBMISSION #################################################

  def submit(self, jobTemplate):
    
    '''
    Implementation of the L{JobScheduler} method.
    '''
    self.logger.debug(">> submit")

    jobTemplateCopy = copy.deepcopy(jobTemplate)
      
    registered_job = self.__registerJob(jobTemplateCopy)
    
    self.__drmaaJobSubmission(registered_job)
    self.logger.debug("<< submit")
    return registered_job.jobTemplate.job_id
  
      
  def __registerJob(self,
                    jobTemplate,
                    workflow_id=-1):

    '''
    Register job in the jobServer and in the DrmaaJobScheduler current instance.

    @type  jobTemplate: L{constant.JobTemplate}
    @param jobTemplate: job submittion information. 
    Note a few restrictions concerning the type of some JobTemplate members:
    referenced_input_files, referenced_output_files and command: must be lists of string
    stdout, stderr and stdin: must be strings
    @type  workflow_id: int
    @param workflow_id: workflow id if the job belongs to any, -1 otherwise
    '''
    expiration_date = datetime.now() + timedelta(hours=jobTemplate.disposal_timeout) 
    parallel_config_name = None
    max_node_number = 1

    if not jobTemplate.stdout_file:
      jobTemplate.stdout_file = self.__jobServer.generateLocalFilePath(self.__user_id)
      jobTemplate.stderr_file = self.__jobServer.generateLocalFilePath(self.__user_id)
      custom_submission = False #the std out and err file has to be removed with the job
    else:
      custom_submission = True #the std out and err file won't to be removed with the job
      jobTemplate.stdout_file = jobTemplate.stdout_file
      jobTemplate.stderr_file = jobTemplate.stderr_file
      
      
    if jobTemplate.parallel_job_info:
      parallel_config_name, max_node_number = jobTemplate.parallel_job_info
       
       
    command_info = ""
    for command_element in jobTemplate.command:
      command_info = command_info + " " + command_element
      
    with self.__lock:
      job_id = self.__jobServer.addJob( soma.jobs.jobServer.DBJob(
                                        user_id = self.__user_id, 
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
                                      
      if jobTemplate.referenced_input_files:
        self.__jobServer.registerInputs(job_id, jobTemplate.referenced_input_files)
      if jobTemplate.referenced_output_files:
        self.__jobServer.registerOutputs(job_id, jobTemplate.referenced_output_files)

    jobTemplate.job_id = job_id
    jobTemplate.workflow_id = workflow_id
    registered_job = DrmaaJobScheduler.RegisteredJob(jobTemplate)
    self.__jobs[job_id] = registered_job
    return registered_job
        
  def __drmaaJobSubmission(self, job): 
    '''
    Submit a registered job.
    The job must have been registered in the current DrmaaJobScheduler instance.

    @type  job: L{DrmaaJobScheduler.RegisteredJob}
    @param job: registered job
    
    '''
    if job not in self.__jobs.values():
      raise JobSchedulerError("A job must be registered before submission.", self.logger)
    
    with self.__lock:
      
      drmaaJobTemplateId = self.__drmaa.allocateJobTemplate()
      self.__drmaa.setCommand(drmaaJobTemplateId, job.jobTemplate.command[0], job.jobTemplate.command[1:])
    
      self.__drmaa.setAttribute(drmaaJobTemplateId, "drmaa_output_path", "[void]:" + job.jobTemplate.stdout_file)
      
      if job.jobTemplate.join_stderrout:
        self.__drmaa.setAttribute(drmaaJobTemplateId,"drmaa_join_files", "y")
      else:
        if job.jobTemplate.stderr_file:
          self.__drmaa.setAttribute(drmaaJobTemplateId, "drmaa_error_path", "[void]:" + job.jobTemplate.stderr_file)
     
      if job.jobTemplate.stdin:
        self.__drmaa.setAttribute(drmaaJobTemplateId, "drmaa_input_path", "[void]:" + job.jobTemplate.stdin)
        
      if job.jobTemplate.working_directory:
        self.__drmaa.setAttribute(drmaaJobTemplateId, "drmaa_wd", job.jobTemplate.working_directory)
      
      if job.jobTemplate.parallel_job_info :
        parallel_config_name, max_node_number = job.jobTemplate.parallel_job_info
        self.__setDrmaaParallelJobTemplate(drmaaJobTemplateId, parallel_config_name, max_node_number)
        
      job_env = []
      for var_name in os.environ.keys():
        job_env.append(var_name+"="+os.environ[var_name])
      
      #job_env = ["LD_LIBRARY_PATH="+os.environ["LD_LIBRARY_PATH"]]
      #job_env.append("PATH="+os.environ["PATH"])
      #self.logger.debug("Environment:")
      #self.logger.debug("  PATH="+os.environ["PATH"])
      #self.logger.debug("  LD_LIBRARY_PATH="+os.environ["LD_LIBRARY_PATH"])
      self.__drmaa.setVectorAttribute(drmaaJobTemplateId, 'drmaa_v_env', job_env)

      drmaaSubmittedJobId = self.__drmaa.runJob(drmaaJobTemplateId)
      self.__drmaa.deleteJobTemplate(drmaaJobTemplateId)
     
      if drmaaSubmittedJobId == "":
        self.logger.error("Could not submit job: Drmaa problem.");
        return -1
      
      self.__jobServer.setSubmissionInformation(job.jobTemplate.job_id, drmaaSubmittedJobId, datetime.now())
      job.drmaa_id = drmaaSubmittedJobId
      job.status = constants.UNDETERMINED
      
    self.logger.debug("job %s submitted! drmaa id = %s", job.jobTemplate.job_id, job.drmaa_id)
    
    


  def __setDrmaaParallelJobTemplate(self, drmaa_job_template_id, configuration_name, max_num_node):
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

    self.logger.debug(">> __setDrmaaParallelJobTemplate")
    if not self.__parallel_job_submission_info:
      raise JobSchedulerError("Configuration file : Couldn't find parallel job submission information.", self.logger)
    
    if configuration_name not in self.__parallel_job_submission_info:
      raise JobSchedulerError("Configuration file : couldn't find the parallel configuration %s." %(configuration_name), self.logger)

    cluster_specific_config_name = self.__parallel_job_submission_info[configuration_name]
    
    for drmaa_attribute in constants.PARALLEL_DRMAA_ATTRIBUTES:
      value = self.__parallel_job_submission_info.get(drmaa_attribute)
      if value: 
        #value = value.format(config_name=cluster_specific_config_name, max_node=max_num_node)
        value = value.replace("{config_name}", cluster_specific_config_name)
        value = value.replace("{max_node}", repr(max_num_node))
        with self.__lock:
          self.__drmaa.setAttribute( drmaa_job_template_id, 
                                    drmaa_attribute, 
                                    value)
          self.logger.debug("Parallel job, drmaa attribute = %s, value = %s ", drmaa_attribute, value) 


    job_env = []
    for parallel_env_v in constants.PARALLEL_JOB_ENV:
      value = self.__parallel_job_submission_info.get(parallel_env_v)
      if value: job_env.append(parallel_env_v+'='+value.rstrip())
    
    
    with self.__lock:
        self.__drmaa.setVectorAttribute(drmaa_job_template_id, 'drmaa_v_env', job_env)
        self.logger.debug("Parallel job environment : " + repr(job_env))
        
    self.logger.debug("<< __setDrmaaParallelJobTemplate")
    
  def __UniversalResourcePathTranslation(self, urp):
    '''
    @type urp: L{UniversalResourcePath}
    @rtype: string
    @returns: path in the local environment
    '''
    if not self.__universal_path_translation:
      raise JobSchedulerError("Configuration file: Couldn't find universal path translation files.", self.logger)
    if not urp.namespace in self.__universal_path_translation.keys():
      raise JobSchedulerError("Universal path translation: the namespace %s does'nt exist" %(urp.namespace), self.logger)
    if not urp.uuid in self.__universal_path_translation[urp.namespace].keys():
      raise JobSchedulerError("Universal path translation: the uuid %s does'nt exist for the namespace %s." %(urp.uuid, urp.namespace), self.logger)
    
    local_path = os.path.join(self.__universal_path_translation[urp.namespace][urp.uuid], urp.relative_path)
    return local_path
    

  def dispose( self, job_id ):
    '''
    Implementation of the L{JobScheduler} method.
    '''
    self.logger.debug(">> dispose %s", job_id)
    with self.__lock:
      self.kill(job_id)
      self.__jobServer.deleteJob(job_id)
    self.logger.debug("<< dispose")


  ########## WORKFLOW SUBMISSION ############################################
  
  
  def restartWorkflow(self, wf_id):
    
    workflow = self.__jobServer.getWorkflow(wf_id)
    with self.__lock:
      to_restart = False
      undone_jobs = []
      for node in workflow.full_nodes:
        if isinstance(node, JobTemplate):
          node_status = self.__getNodeStatus(node)
          if node_status == DrmaaJobScheduler.NODE_FAILED:
            #clear all the information related to the previous job submission
            self.__jobServer.setSubmissionInformation(node.job_id, None, None)
            self.__jobServer.setJobStatus(node.job_id, constants.NOT_SUBMITTED)
          if node_status != DrmaaJobScheduler.NODE_ENDED_WITH_SUCCESS:
            registered_job = DrmaaJobScheduler.RegisteredJob(node)
            self.__jobs[node.job_id] = registered_job
            undone_jobs.append(node)
           
      if undone_jobs:
        self.__workflows[workflow.wf_id] = workflow
        # look for nodes to run
        for job in undone_jobs:
          node_to_run = True # a node is run when all its dependencies succeed
          for dep in workflow.full_dependencies:
            if dep[1] == job: 
              node_status = self.__getNodeStatus(dep[0])
              if node_status != DrmaaJobScheduler.NODE_ENDED_WITH_SUCCESS:
                node_to_run = False
                
          if node_to_run: 
            self.logger.debug("  ---- Job to run : " + node.name + " " + repr(node.command))
            self.__drmaaJobSubmission(self.__jobs[job.job_id])

  
  def submitWorkflow(self, workflow_o, expiration_date, name):
    # type checking for the workflow ?
    workflow = copy.deepcopy(workflow_o)
    
    #First, do the universal resource path translation to check possible errors
    for node in workflow.nodes:
      if isinstance(node, JobTemplate):
        for command_el in node.command:
          if isinstance(command_el, UniversalResourcePath):
            command_el.local_path = self.__UniversalResourcePathTranslation(command_el)
        if node.stdout_file and isinstance(node.stdout_file, UniversalResourcePath):
          node.stdout_file = self.__UniversalResourcePathTranslation(node.stdout_file)
        if node.stderr_file and isinstance(node.stderr_file, UniversalResourcePath):
          node.stderr_file = self.__UniversalResourcePathTranslation(node.stderr_file)
        if node.working_directory and isinstance(node.working_directory, UniversalResourcePath):
          node.working_directory = self.__UniversalResourcePathTranslation(node.working_directory)
        if node.stdin and isinstance(node.stdin, UniversalResourcePath):
          node.stdin = self.__UniversalResourcePathTranslation(node.stdin)
     ######################################################
 
    workflow_id = self.__jobServer.addWorkflow(self.__user_id, expiration_date, name)
    workflow.wf_id = workflow_id 
    workflow.name = name
    
    def assert_is_a_workflow_node(local_path):
      matching_node = None
      for node in workflow.full_nodes:
        if isinstance(node, FileTransfer) and node.local_path == input_file:
          matching_node = node 
          break
      if not matching_node: 
        raise JobSchedulerError("Workflow submission: The localfile path \"" + local_path + "\" doesn't match with a workflow FileTransfer node.", self.logger)
      else: 
        return matching_node
    
    if not workflow.full_nodes:
      workflow.full_nodes = set(workflow.nodes)
      # get back the full nodes looking for fileTransfer nodes in the JobTemplate node
      for node in workflow.nodes:
        if isinstance(node, JobTemplate):
          for ft in node.referenced_input_files:
            if isinstance(ft, FileTransfer):  workflow.full_nodes.add(ft)
          for ft in node.referenced_output_files:
            if isinstance(ft, FileTransfer): workflow.full_nodes.add(ft)
          
    # the missing dependencies between JobTemplate and FileTransfer will be added 
    workflow.full_dependencies = set(workflow.dependencies)
   
    w_js = []
    w_fts = []
    # Register FileTransfer to the JobServers
    for node in workflow.full_nodes:
      if isinstance(node, FileSending):
        if node.remote_paths:
          node.local_path = self.__jobServer.generateLocalFilePath(self.__user_id)
          os.mkdir(node.local_path)
        else:
          node.local_path = self.__jobServer.generateLocalFilePath(self.__user_id, node.remote_path)
        self.__jobServer.addTransfer(node.local_path, node.remote_path, expiration_date, self.__user_id, constants.READY_TO_TRANSFER, workflow_id, node.remote_paths)
        w_fts.append(node)
      elif isinstance(node, FileRetrieving):
        if node.remote_paths:
          node.local_path = self.__jobServer.generateLocalFilePath(self.__user_id)
          os.mkdir(node.local_path)
        else:
          node.local_path = self.__jobServer.generateLocalFilePath(self.__user_id, node.remote_path)
        self.__jobServer.addTransfer(node.local_path, node.remote_path, expiration_date, self.__user_id, constants.TRANSFER_NOT_READY, workflow_id, node.remote_paths)
        w_fts.append(node)
      elif isinstance(node, JobTemplate):
        w_js.append(node)
    
    # Job attributs conversion and job registration to the JobServer:
    for job in w_js:
      # command
      new_command = []
      for command_el in job.command:
        if isinstance(command_el, tuple) and isinstance(command_el[0], FileTransfer):
          new_command.append(os.path.join(command_el[0].local_path, command_el[1]))
        elif isinstance(command_el, FileTransfer):
          new_command.append(command_el.local_path)
        elif isinstance(command_el, UniversalResourcePath):
          new_command.append(command_el.local_path)
        else:
          new_command.append(command_el)
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
          new_referenced_output_files.append(output_file)
          workflow.full_dependencies.add((job, oft_node))
      job.referenced_output_files = new_referenced_output_files
      
      # stdin => replace JobTransfer object by corresponding
      if job.stdin:
        if isinstance(job.stdin, FileTransfer):
          job.stdin = job.stdin.local_path 
        
      # Job registration
      registered_job = self.__registerJob(job, workflow_id)
      job.job_id = registered_job.jobTemplate.job_id
     
    self.__jobServer.setWorkflow(workflow_id, workflow, self.__user_id)
    self.__workflows[workflow_id] = workflow
    
    # run nodes without dependencies
    for node in workflow.nodes:
      torun=True
      for dep in workflow.full_dependencies:
        torun = torun and not dep[1] == node
      if torun:
        if isinstance(node, JobTemplate):
          self.__drmaaJobSubmission(self.__jobs[node.job_id])
          
    return workflow
     
  #def __isWFNodeCompleted(self, node):
    #competed = False
    #if isinstance(node, JobTemplate):
      #if node.job_id: 
        #completed = True
        #if node.job_id in self.__jobs :
          #status = self.__jobs[node.job_id].status
          #completed = status == constants.DONE or status == constants.FAILED
    #if isinstance(node, FileSending):
      #if node.local_path:
        #status = self.__jobServer.getTransferStatus(node.local_path)
        #completed = status == constants.TRANSFERED
    #if isinstance(node, FileRetrieving):
      #if node.local_path:
        #status = self.__jobServer.getTransferStatus(node.local_path)
        #completed = status == constants.READY_TO_TRANSFER
    #return completed
      
  NODE_NOT_PROCESSED="node_not_processed"
  NODE_IN_PROGRESS="node_in_progress"
  NODE_ENDED_WITH_SUCCESS="node_ended_with_success"
  NODE_FAILED="node_failed"

  def __getNodeStatus(self, node):
    if isinstance(node, JobTemplate):
      if not node.job_id:
        return DrmaaJobScheduler.NODE_NOT_PROCESSED
      if not node.job_id in self.__jobs:
        status = self.__jobServer.getJobStatus(node.job_id)[0]
        if status == constants.DONE:
          job = self.__jobServer.getJob(node.job_id)
      else:
        job = self.__jobs[node.job_id]
        status = job.status
      if status == constants.NOT_SUBMITTED:
        return DrmaaJobScheduler.NODE_NOT_PROCESSED
      if status == constants.FAILED:
        return DrmaaJobScheduler.NODE_FAILED
      if status == constants.DONE:
        if not job.exit_value == 0 or job.terminating_signal != None or not job.exit_status == constants.FINISHED_REGULARLY:
          return DrmaaJobScheduler.NODE_FAILED
        return DrmaaJobScheduler.NODE_ENDED_WITH_SUCCESS
      return DrmaaJobScheduler.NODE_IN_PROGRESS
    if isinstance(node, FileSending):
      if not node.local_path: return DrmaaJobScheduler.NODE_NOT_PROCESSED
      status = self.__jobServer.getTransferStatus(node.local_path)
      if status == constants.TRANSFERED:
        return DrmaaJobScheduler.NODE_ENDED_WITH_SUCCESS
      if status == constants.TRANSFER_NOT_READY or status == constants.READY_TO_TRANSFER:
        return DrmaaJobScheduler.NODE_NOT_PROCESSED
      if status == constants.TRANSFERING:
        return DrmaaJobScheduler.NODE_IN_PROGRESS
    if isinstance(node, FileRetrieving):
      if not node.local_path: return DrmaaJobScheduler.NODE_NOT_PROCESSED
      status = self.__jobServer.getTransferStatus(node.local_path)
      if status == constants.TRANSFERED or status == constants.READY_TO_TRANSFER:
        return DrmaaJobScheduler.NODE_ENDED_WITH_SUCCESS
      if status == constants.TRANSFER_NOT_READY:
        return DrmaaJobScheduler.NODE_NOT_PROCESSED
      if status == constants.TRANSFERING:
        return DrmaaJobScheduler.NODE_IN_PROGRESS
   
  def __workflowProcessing(self, endedJobs=[], endedTransfers=[]):
    '''
    Explore the submitted workflows to submit jobs and/or change transfer status.

    @type  endedJobs: list of job id
    @param endedJobs: list of the ended jobs
    @type  endedTransfers: sequence of tuple (workflow_id, set of local_file_path)
    @param endedTransfers: list of ended transfers for each workflow
    '''
    self.logger.debug(">> workflowProcessing")

    with self.__lock:
      wf_to_process = set([])
      for job_id in endedJobs:
        job = self.__jobs[job_id]
        self.logger.debug("  ==> ended job: " + job.jobTemplate.name)     
      
        if job.jobTemplate.referenced_output_files:
          for local_path in job.jobTemplate.referenced_output_files:
            self.__jobServer.setTransferStatus(local_path, constants.READY_TO_TRANSFER)
            
        if not job.jobTemplate.workflow_id == -1 and job.jobTemplate.workflow_id in self.__workflows:
          workflow = self.__workflows[job.jobTemplate.workflow_id]
          wf_to_process.add(workflow)

      for workflow_id, w_ended_transfers in endedTransfers:
        for local_path in w_ended_transfers:
          self.logger.debug("  ==> ended Transfer: " + local_path + " workflow_id " + repr(workflow_id))
          workflow = self.__workflows[workflow_id]
          wf_to_process.add(workflow)
        
      to_run = []
      to_abort = set([])
      for workflow in wf_to_process:
        for node in workflow.full_nodes:
          if isinstance(node, JobTemplate):
            to_inspect = False
            if node.job_id in self.__jobs:
              status = self.__jobs[node.job_id].status
              to_inspect = status == constants.NOT_SUBMITTED
            #print "node " + node.name + " status " + status[0] + " to inspect " + repr(to_inspect)
          if isinstance(node, FileTransfer):
            status = self.__jobServer.getTransferStatus(node.local_path)
            to_inspect = status == constants.TRANSFER_NOT_READY
            #print "node " + node.name + " status " + status + " to inspect " + repr(to_inspect)
          if to_inspect:
            #self.logger.debug("  -- to inspect : " + node.name)
            node_to_run = True # a node is run when all its dependencies succeed
            for dep in workflow.full_dependencies:
              if dep[1] == node: 
                #self.logger.debug("   node " + node.name + " dep: " + dep[0].name + " ---> " + dep[1].name)
                node_status = self.__getNodeStatus(dep[0])
                #self.logger.debug("   dep[0] status" + repr(node_status))
                if node_status != DrmaaJobScheduler.NODE_ENDED_WITH_SUCCESS:
                  node_to_run = False
                  if isinstance(dep[1], JobTemplate) and isinstance(dep[0], JobTemplate) and not dep[1] in to_abort and node_status == DrmaaJobScheduler.NODE_FAILED  :
                      to_abort.add(dep[1]) 
                      break
                  
            if node_to_run: 
              if isinstance(node, JobTemplate):
                self.logger.debug("  ---- Job to run : " + node.name + " " + repr(node.command))
              to_run.append(node)
        
      for node in to_run:
        if isinstance(node, JobTemplate):
          self.__drmaaJobSubmission(self.__jobs[node.job_id])
        if isinstance(node,FileTransfer):
          self.__jobServer.setTransferStatus(node.local_path, constants.READY_TO_TRANSFER)
      
      # if a job fails the whole workflow branch has to be stopped
      # look for the node in the branch to abort
      previous_size = 0
      while previous_size != len(to_abort):
        previous_size = len(to_abort)
        for dep in workflow.full_dependencies:
          if isinstance(dep[1], JobTemplate) and dep[0] in to_abort and not dep[1] in to_abort:
            to_abort.add(dep[1])
            break
          
      # stop the whole branch
      for node in to_abort:
        if isinstance(node, JobTemplate) and node.job_id in self.__jobs.keys():
          self.logger.debug("  ---- Failure: job to abort " + node.name)
          assert(self.__jobs[node.job_id].status == constants.NOT_SUBMITTED)
          self.__jobServer.setJobStatus(node.job_id, constants.FAILED)
          self.__jobServer.setJobExitInfo(node.job_id, constants.EXIT_ABORTED, None, None, None)
          del self.__jobs[node.job_id]
        
    # delete ended workflows:
    finished_workfows = []
    for workflow_id, workflow in self.__workflows.items():
      finished = True
      for node in workflow.full_nodes:
        if isinstance(node, JobTemplate):
          node_status = self.__getNodeStatus(node)
          finished = finished and (node_status == DrmaaJobScheduler.NODE_ENDED_WITH_SUCCESS or node_status == DrmaaJobScheduler.NODE_FAILED)
          if not finished: break
      if finished: 
        finished_workfows.append(workflow_id)
    for workflow_id in finished_workfows:
      self.logger.debug("  ~~~~ END OF WORKFLOW " + repr(workflow_id) + " ~~~~")
      del self.__workflows[workflow_id]
      self.__jobServer.setWorkflowStatus(workflow_id, constants.WORKFLOW_DONE)
    self.logger.debug("<<< workflowProcessing")
    


  ########### DRMS MONITORING ################################################

  def areJobsDone(self):
    return self.__jobsEnded
    
  ########## JOB CONTROL VIA DRMS ########################################
  

  def stop( self, job_id ):
    '''
    Implementation of the L{JobScheduler} method.
    '''
    self.logger.debug(">> stop")
    status_changed = False
    with self.__lock:
      if job_id in self.__jobs:
        drmaaJobId = self.__jobs[job_id].drmaa_id
      else:
        drmaaJobId = self.__jobServer.getDrmaaJobId(job_id)
      if drmaaJobId:
        status = self.__drmaa.jobStatus(drmaaJobId) 
        self.logger.debug("   status : " + status)
        if status==constants.RUNNING:
          self.__drmaa.suspend(drmaaJobId)
          status_changed = True
        
        if status==constants.QUEUED_ACTIVE:
          self.__drmaa.hold(drmaaJobId)
          status_changed = True
        
        
    if status_changed:
      self.__waitForStatusUpdate(job_id)
    self.logger.debug("<< stop")
    
    
  def restart( self, job_id ):
    '''
    Implementation of the L{JobScheduler} method.
    '''
    self.logger.debug(">> restart")
    status_changed = False
    with self.__lock:
      if job_id in self.__jobs:
        drmaaJobId = self.__jobs[job_id].drmaa_id
      else:
        drmaaJobId = self.__jobServer.getDrmaaJobId(job_id)
      if drmaaJobId:
        status = self.__drmaa.jobStatus(drmaaJobId) 
        
        if status==constants.USER_SUSPENDED or status==constants.USER_SYSTEM_SUSPENDED:
          self.__drmaa.resume(drmaaJobId)
          status_changed = True
          
        if status==constants.USER_ON_HOLD or status==constants.USER_SYSTEM_ON_HOLD :
          self.__drmaa.release(drmaaJobId)
          status_changed = True
        
    if status_changed:
      self.__waitForStatusUpdate(job_id)
    self.logger.debug("<< restart")
    
  


  def kill( self, job_id ):
    '''
    Implementation of the L{JobScheduler} method.
    '''
    self.logger.debug(">> kill")
        
    with self.__lock:
      (status, last_status_update) = self.__jobServer.getJobStatus(job_id)

      if status and not status == constants.DONE and not status == constants.FAILED:
        drmaaJobId = self.__jobServer.getDrmaaJobId(job_id)
        if drmaaJobId:
          self.logger.debug("terminate job %s drmaa id %s with status %s", job_id, drmaaJobId, status)
          self.__drmaa.terminate(drmaaJobId)
        
          self.__jobServer.setJobExitInfo(job_id, 
                                          constants.USER_KILLED,
                                          None,
                                          None,
                                          None)
          
          self.__jobServer.setJobStatus(job_id, constants.FAILED)
        if job_id in self.__jobs.keys():
          del self.__jobs[job_id]
        
    self.logger.debug("<< kill")


  def __waitForStatusUpdate(self, job_id):
    
    self.logger.debug(">> __waitForStatusUpdate")
    drmaaActionTime = datetime.now()
    time.sleep(refreshment_interval)
    (status, last_status_update) = self.__jobServer.getJobStatus(job_id)
    while status and not status == constants.DONE and not status == constants.FAILED and last_status_update < drmaaActionTime:
      time.sleep(refreshment_interval)
      (status, last_status_update) = self.__jobServer.getJobStatus(job_id) 
      if last_status_update and datetime.now() - last_status_update > timedelta(seconds = refreshment_interval*5):
        raise JobSchedulerError('Could not get back status of job %s. The process updating its status failed.' %(job_id), self.logger)
    self.logger.debug("<< __waitForStatusUpdate")


class WorkflowEngine( object ):
  
  def __init__( self, job_server, drmaa_job_scheduler = None,  parallel_job_submission_info = None, universal_path_translation = None):
    ''' 
    @type  job_server: L{JobServer}
    @type  drmaa_job_scheduler: L{DrmaaJobScheduler} or None
    @param drmaa_job_scheduler: object of type L{DrmaaJobScheduler} to delegate all the tasks related to the DRMS. If None a new instance is created.
    '''
    
    self.logger = logging.getLogger('ljp.js')
    
    Pyro.core.initClient()

    # Drmaa Job Scheduler
    if drmaa_job_scheduler:
      self.__drmaaJS = drmaa_job_scheduler
    else:
      #print "parallel_job_submission_info" + repr(parallel_job_submission_info)
      self.__drmaaJS = DrmaaJobScheduler(job_server, parallel_job_submission_info, universal_path_translation)
    
    # Job Server
    self.__jobServer= job_server
    
    try:
      userLogin = pwd.getpwuid(os.getuid())[0]
    except Exception, e:
      raise JobSchedulerError("Couldn't identify user %s: %s \n" %(type(e), e), self.logger)
    
    self.__user_id = self.__jobServer.registerUser(userLogin)
    

  def __del__( self ):
    pass

  ########## FILE TRANSFER ###############################################
  
  '''
  For the following methods:
    Local means that it is located on a directory shared by the machine of the pool
    Remote means that it is located on a remote machine or on any directory 
    owned by the user. 
    A transfer will associate remote file path to unique local file path.
  
  Use L{registerTransfer} then L{writeLine} or scp or 
  shutil.copy to transfer input file from the remote to the local 
  environment.
  Use L{registerTransfer} and once the job has run use L{readline} or scp or
  shutil.copy to transfer the output file from the local to the remote environment.
  '''

  def registerTransfer(self, remote_path, disposal_timeout=168, remote_paths = None): 
    '''
    Implementation of soma.jobs.jobClient.Jobs API
    '''
    if remote_paths:
      local_path = self.__jobServer.generateLocalFilePath(self.__user_id)
      os.mkdir(local_path)
    else:
      local_path = self.__jobServer.generateLocalFilePath(self.__user_id, remote_path)
    expirationDate = datetime.now() + timedelta(hours=disposal_timeout) 
    self.__jobServer.addTransfer(local_path, remote_path, expirationDate, self.__user_id, -1, remote_paths)
    return local_path
  
  @staticmethod
  def __contents(path_seq, md5_hash=False):
    result = []
    for path in path_seq:
      s = os.stat(path)
      if stat.S_ISDIR(s.st_mode):
        full_path_list = []
        for element in os.listdir(path):
          full_path_list.append(os.path.join(path, element))
        contents = JobScheduler.__contents(full_path_list, md5_hash)
        result.append((os.path.basename(path), contents, None))
      else:
        if md5_hash:
          result.append( ( os.path.basename(path), s.st_size, hashlib.md5( open( path, 'rb' ).read() ).hexdigest() ) )
        else:
          result.append( ( os.path.basename(path), s.st_size, None ) )
    return result


  def initializeRetrivingTransfer(self, local_path):
    local_path, remote_path, expiration_date, workflow_id, remote_paths = self.transferInformation(local_path)
    status = self.transferStatus(local_path)
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
        contents = JobScheduler.__contents([local_path])
        (cumulated_file_size, dir_element_action_info) = self.__initializeDirectory(local_path, contents)
        transfer_action_info = (cumulated_file_size, dir_element_action_info, constants.DIR_RETRIEVING)
    else: #remote_paths
      full_path_list = []
      for element in os.listdir(local_path):
        full_path_list.append(os.path.join(local_path, element))
      contents = JobScheduler.__contents(full_path_list)
      (cumulated_file_size, dir_element_action_info) = self.__initializeDirectory(local_path, contents)
      transfer_action_info = (cumulated_file_size, dir_element_action_info, constants.DIR_RETRIEVING)

    self.__jobServer.setTransferActionInfo(local_path, transfer_action_info)     
    return (transfer_action_info, contents)

  def initializeFileSending(self, local_path, file_size, md5_hash = None):
    '''
    Initialize the transfer of a file.
    '''
    transfer_action_info = (file_size, md5_hash, constants.FILE_SENDING)
    f = open(local_path, 'w')
    f.close()
    self.__jobServer.setTransferStatus(local_path, constants.TRANSFERING)
    self.__jobServer.setTransferActionInfo(local_path, transfer_action_info)
    return transfer_action_info


  def __initializeDirectory(self, local_path, contents, subdirectory = ""):
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
        sub_size, sub_dir_element_action_info = self.__initializeDirectory( local_path, description, relative_path)
        cumulated_file_size += sub_size
        dir_element_action_info.update(sub_dir_element_action_info)
      else:
        file_size = description
        dir_element_action_info[relative_path] = (file_size, md5_hash)
        cumulated_file_size += file_size

    return (cumulated_file_size, dir_element_action_info)

  def __createDirStructure(self, local_path, contents, subdirectory = ""):
    for item, description, md5_hash in contents:
      relative_path = os.path.join(subdirectory,item)
      full_path = os.path.join(local_path, relative_path)
      if isinstance(description, list):
        if not os.path.isdir(full_path):
          os.mkdir(full_path)
        self.__createDirStructure( local_path, description, relative_path)
    

  def initializeDirSending(self, local_path, contents):
    '''
    Initialize the transfer of a directory.
    '''
    cumulated_file_size, dir_element_action_info = self.__initializeDirectory(local_path, contents)
    transfer_action_info = (cumulated_file_size, dir_element_action_info, constants.DIR_SENDING)
    self.__createDirStructure(local_path, contents)
    self.__jobServer.setTransferStatus(local_path, constants.TRANSFERING)
    self.__jobServer.setTransferActionInfo(local_path, transfer_action_info)
    return transfer_action_info
    
  
  def __sendToFile(self, local_path, data, file_size, md5_hash = None):
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
      raise JobSchedulerError('sendPiece: Transmitted data exceed expected file size.')
    elif fs == file_size:
      if md5_hash is not None:
        if hashlib.md5( open( local_path, 'rb' ).read() ).hexdigest() != md5_hash:
          # Reset file
          open( local_path, 'wb' )
          raise JobSchedulerError('sendPiece: Transmission error detected.')
        else:
          return True
      else:
        return True
    else:
      return False

  def sendPiece(self, local_path, data, relative_path=None):
    '''
    @rtype : boolean
    @return : transfer ended
    '''
    if not relative_path:
      # File case
      (file_size, md5_hash, transfer_type) = self.__jobServer.getTransferActionInfo(local_path)
      transfer_ended = self.__sendToFile(local_path, data, file_size, md5_hash)
      if transfer_ended:
        self.__jobServer.setTransferStatus(local_path, constants.TRANSFERED)
        self.signalTransferEnded(local_path)
      
    else:
      # Directory case
      (cumulated_size, dir_element_action_info, transfer_type) = self.__jobServer.getTransferActionInfo(local_path)
      if not relative_path in dir_element_action_info:
        raise JobSchedulerError('sendPiece: the file %s doesn t belong to the transfer %s' %(relative_path, local_path))
      (file_size, md5_hash) = dir_element_action_info[relative_path]
      transfer_ended = self.__sendToFile(os.path.join(local_path, relative_path), data, file_size, md5_hash)
      
      if transfer_ended:
        cumulated_file_size, cumulated_transmissions, files_transfer_status = self.transferProgressionStatus(local_path, (cumulated_size, dir_element_action_info, transfer_type))
        if cumulated_transmissions == cumulated_file_size:
          self.__jobServer.setTransferStatus(local_path, constants.TRANSFERED)
          self.signalTransferEnded(local_path)
      
    return transfer_ended
  

  def retrievePiece(self, local_path, buffer_size, transmitted, relative_path = None):
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

    
  def setTransferStatus(self, local_path, status):
    '''
    Set a transfer status. 
    '''
     
    if not self.__jobServer.isUserTransfer(local_path, self.__user_id) :
      #print "Couldn't set transfer status %s. It doesn't exist or is not owned by the current user \n" % local_path
      return
    
    self.__jobServer.setTransferStatus(local_path, status)

  def cancelTransfer(self, local_path):
    '''
    Implementation of the L{Jobs} method.
    '''
    
    if not self.__jobServer.isUserTransfer(local_path, self.__user_id) :
      #print "Couldn't cancel transfer %s. It doesn't exist or is not owned by the current user \n" % local_path
      return

    self.__jobServer.removeTransfer(local_path)
    
  def signalTransferEnded(self, local_path):
    '''
    Has to be called each time a file transfer ends for the 
    workflows to be proceeded.
    '''
    workflow_id = self.__jobServer.getTransferInformation(local_path)[3]
    if workflow_id != -1:
      self.__jobServer.addWorkflowEndedTransfer(workflow_id, local_path)
    

  ########## JOB SUBMISSION ##################################################

  
  def submit( self,
              jobTemplate):
    '''
    Submits a job to the system. 
    
    @type  jobTemplate: L{JobTemplate}
    @param jobTemplate: job informations 
    '''

    if len(jobTemplate.command) == 0:
      raise JobSchedulerError("Submission error: the command must contain at least one element \n", self.logger)

    # check the required_local_input_files, required_local_output_file and stdin ?
    
    
    
    job_id = self.__drmaaJS.submit(jobTemplate)
    
    return job_id




  def dispose( self, job_id ):
    '''
    Implementation of soma.jobs.jobClient.Jobs API
    '''
    
    if not self.__jobServer.isUserJob(job_id, self.__user_id):
      #print "Couldn't dispose job %d. It doesn't exist or is not owned by the current user \n" % job_id
      return 
    
    self.__drmaaJS.dispose(job_id)


  ########## WORKFLOW SUBMISSION ############################################
  
  def submitWorkflow(self, workflow, expiration_date, name):
    '''
    Implementation of soma.jobs.jobClient.Jobs API
    '''
    if not expiration_date:
      expiration_date = datetime.now() + timedelta(days=7)
    return self.__drmaaJS.submitWorkflow(workflow, expiration_date, name)
  
  def disposeWorkflow(self, workflow_id):
    '''
    Implementation of soma.jobs.jobClient.Jobs API
    '''
    if not self.__jobServer.isUserWorkflow(workflow_id, self.__user_id):
      #print "Couldn't dispose workflow %d. It doesn't exist or is not owned by the current user \n" % job_id
      return
    
    self.__jobServer.deleteWorkflow(workflow_id)
    
  def changeWorkflowExpirationDate(self, workflow_id, new_expiration_date):
    '''
    Implementation of soma.jobs.jobClient.Jobs API
    '''
    if not self.__jobServer.isUserWorkflow(workflow_id, self.__user_id):
      #print "Couldn't dispose workflow %d. It doesn't exist or is not owned by the current user \n" % job_id
      return False
    
    if new_expiration_date < datetime.now(): 
      return False
    # TO DO: Add other rules?
    
    self.__jobServer.changeWorkflowExpirationDate(workflow_id, new_expiration_date)
    return True

  def restartWorkflow(self, workflow_id):
    '''
    Implementation of soma.jobs.jobClient.Jobs API
    '''
    if not self.__jobServer.isUserWorkflow(workflow_id, self.__user_id):
      #print "Couldn't restart workflow %d. It doesn't exist or is not owned by the current user \n" % job_id
      return False
    
    (status, last_status_update) = self.__jobServer.getWorkflowStatus(workflow_id)
    
    
    print "last status update too old = " + repr((datetime.now() - last_status_update < timedelta(seconds=refreshment_interval*5))) 
    if not last_status_update or (datetime.now() - last_status_update < timedelta(seconds=refreshment_interval*5) and status != constants.WORKFLOW_DONE):
      return False
    
    
    print "self.__drmaaJS.restartWorkflow(workflow_id)"
    self.__drmaaJS.restartWorkflow(workflow_id)
    return True
    
   
  ########## SERVER STATE MONITORING ########################################


  def jobs(self):
    '''
    Implementation of soma.jobs.jobClient.Jobs API
    '''
    return self.__jobServer.getJobs(self.__user_id)
    
  def transfers(self):
    '''
    Implementation of soma.jobs.jobClient.Jobs API
    '''
    return self.__jobServer.getTransfers(self.__user_id)
  
  
  def workflows(self):
    '''
    Implementation of soma.jobs.jobClient.Jobs API
    '''
    return self.__jobServer.getWorkflows(self.__user_id)
  
  def submittedWorkflow(self, wf_id):
    '''
    Implementation of soma.jobs.jobClient.Jobs API
    '''
    if not self.__jobServer.isUserWorkflow(wf_id, self.__user_id):
      #print "Couldn't get workflow %d. It doesn't exist or is owned by a different user \n" %wf_id
      return
    return self.__jobServer.getWorkflow(wf_id)

 
  def workflowInformation(self, wf_id):
    '''
    Implementation of soma.jobs.jobClient.Jobs API
    '''
    if not self.__jobServer.isUserWorkflow(wf_id, self.__user_id):
      #print "Couldn't get workflow %d. It doesn't exist or is owned by a different user \n" %wf_id
      return
    return self.__jobServer.getWorkflowInfo(wf_id)
    
    
 
  def transferInformation(self, local_path):
    '''
    Implementation of soma.jobs.jobClient.Jobs API
    '''
    #TBI raise an exception if local_path is not valid transfer??
    
    if not self.__jobServer.isUserTransfer(local_path, self.__user_id):
      #print "Couldn't get transfer information of %s. It doesn't exist or is owned by a different user \n" % local_path
      return
      
    return self.__jobServer.getTransferInformation(local_path)
   


  def status( self, job_id ):
    '''
    Implementation of soma.jobs.jobClient.Jobs API
    '''
    if not self.__jobServer.isUserJob(job_id, self.__user_id):
      #print "Could get the job status of job %d. It doesn't exist or is owned by a different user \n" %job_id
      return
    
    return self.__jobServer.getJobStatus(job_id)[0]
        
  
  def detailedWorkflowStatus(self, wf_id, groupe = None):
    '''
    Implementation of soma.jobs.jobClient.Jobs API
    '''
    if not self.__jobServer.isUserWorkflow(wf_id, self.__user_id):
      #print "Couldn't get workflow %d. It doesn't exist or is owned by a different user \n" %wf_id
      return
    wf_status = self.__jobServer.getDetailedWorkflowStatus(wf_id)
    return wf_status
        
        
  def transferStatus(self, local_path):
    if not self.__jobServer.isUserTransfer(local_path, self.__user_id):
      #print "Could not get the job status the transfer associated with %s. It doesn't exist or is owned by a different user \n" %local_path
      return
    transfer_status = self.__jobServer.getTransferStatus(local_path)  
    return transfer_status
        
  def transferActionInfo(self,local_path):
    return self.__jobServer.getTransferActionInfo(local_path)
 
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
    

  def exitInformation(self, job_id ):
    '''
    Implementation of soma.jobs.jobClient.Jobs API
    '''
  
    if not self.__jobServer.isUserJob(job_id, self.__user_id):
      #print "Could get the exit information of job %d. It doesn't exist or is owned by a different user \n" %job_id
      return
  
    dbJob = self.__jobServer.getJob(job_id)
    exit_status = dbJob.exit_status
    exit_value = dbJob.exit_value
    terminating_signal =dbJob.terminating_signal
    resource_usage = dbJob.resource_usage
    
    return (exit_status, exit_value, terminating_signal, resource_usage)
    
 
  def jobInformation(self, job_id):
    '''
    Implementation of soma.jobs.jobClient.Jobs API
    '''
    
    if not self.__jobServer.isUserJob(job_id, self.__user_id):
      #print "Could get information about job %d. It doesn't exist or is owned by a different user \n" %job_id
      return
    
    dbJob = self.__jobServer.getJob(job_id)
    name_description = dbJob.name_description 
    command = dbJob.command
    submission_date = dbJob.submission_date
    
    return (name_description, command, submission_date)
    
  def getStdOutErrTransferActionInfo(self, job_id):
    if not self.__jobServer.isUserJob(job_id, self.__user_id):
      return
    
    stdout_file, stderr_file = self.__jobServer.getStdOutErrFilePath(job_id)
    
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
  
  
  def wait( self, job_ids, timeout = -1):
    '''
    Implementation of soma.jobs.jobClient.Jobs API
    '''
    for jid in job_ids:
      if not self.__jobServer.isUserJob(jid, self.__user_id):
        raise JobSchedulerError( "Could not wait for job %d. It doesn't exist or is owned by a different user \n" %jid, self.logger)
      
    #self.__drmaaJS.wait(job_ids, timeout)
    self.logger.debug("        waiting...")
    
    waitForever = timeout < 0
    startTime = datetime.now()
    for jid in job_ids:
      (status, last_status_update) = self.__jobServer.getJobStatus(jid)
      if status:
        self.logger.debug("wait        job %s status: %s", jid, status)
        delta = datetime.now()-startTime
        delta_status_update = datetime.now() - last_status_update
        while status and not status == constants.DONE and not status == constants.FAILED and (waitForever or delta < timedelta(seconds=timeout)):
          time.sleep(refreshment_interval)
          (status, last_status_update) = self.__jobServer.getJobStatus(jid) 
          self.logger.debug("wait        job %s status: %s last update %s, now %s", jid, status, repr(last_status_update), repr(datetime.now()))
          delta = datetime.now() - startTime
          if last_status_update and datetime.now() - last_status_update > timedelta(seconds = refreshment_interval*10):
            raise JobSchedulerError('Could not wait for job %s. The process updating its status failed.' %(jid), self.logger)
    

  def stop( self, job_id ):
    '''
    Implementation of soma.jobs.jobClient.Jobs API
    '''
    if not self.__jobServer.isUserJob(job_id, self.__user_id):
      raise JobSchedulerError( "Could not stop job %d. It doesn't exist or is owned by a different user \n" %job_id, self.logger)
    
    self.__drmaaJS.stop(job_id)
   
  
  
  def restart( self, job_id ):
    '''
    Implementation of soma.jobs.jobClient.Jobs API
    '''
    if not self.__jobServer.isUserJob(job_id, self.__user_id):
      raise JobSchedulerError( "Could not restart job %d. It doesn't exist or is owned by a different user \n" %job_id, self.logger)
    
    self.__drmaaJS.restart(job_id)


  def kill( self, job_id ):
    '''
    Implementation of soma.jobs.jobClient.Jobs API
    '''

    if not self.__jobServer.isUserJob(job_id, self.__user_id):
      raise JobSchedulerError( "Could not kill job %d. It doesn't exist or is owned by a different user \n" %job_id, self.logger)
    
    self.__drmaaJS.kill(job_id)


