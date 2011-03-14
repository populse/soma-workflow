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
import threading
import pwd
import os
import time
import logging
import copy
import stat, hashlib, operator
import itertools
import types

#import cProfile

import soma.workflow.constants as constants
from soma.workflow.client import Job, FileTransfer, Workflow, SharedResourcePath, Group, WorkflowController
#, dir_content, create_dir_structure
import soma.workflow.database_server 
from soma.workflow.somadrmaajobssip import DrmaaJobs, DrmaaError
from soma.workflow.errors import JobError, WorkflowError, UnknownObjectError, EngineError, DRMError


#-----------------------------------------------------------------------------
# Globals and constants
#-----------------------------------------------------------------------------

__docformat__ = "epytext en"
refreshment_interval = 1 #seconds
# if the last status update is older than the refreshment_timeout 
# the status is changed into WARNING
refreshment_timeout = 60 #seconds

#-----------------------------------------------------------------------------
# Classes and functions
#-----------------------------------------------------------------------------

class EngineLoopThread(threading.Thread):
  def __init__(self, engine_loop):
    super(EngineLoopThread, self).__init__()
    self.engine_loop = engine_loop
    self.time_interval = refreshment_interval
  
  def run(self):
    #cProfile.runctx("self.engine_loop.start_loop(self.time_interval)", globals(), locals(), "/home/soizic/profile/profile_loop_thread")
    self.engine_loop.start_loop(self.time_interval)

  def stop(self):
    self.engine_loop.stop_loop()
    self.join()
  
  
class Drmaa(object):
  '''
  Manipulation of the Drmaa session. 
  Contains possible patch depending on the DRMAA impementation. 
  '''

  # DRMAA session. DrmaaJobs
  _drmaa = None
  # string
  _drmaa_implementation = None
  # DRMAA doesn't provide an unified way of submitting
  # parallel jobs. The value of parallel_job_submission is cluster dependant. 
  # The keys are:
  #  -Drmaa job template attributes 
  #  -parallel configuration name as defined in soma.workflow.constants
  # dict
  parallel_job_submission_info = None
  
  logger = None

  def __init__(self, drmaa_implementation, parallel_job_submission_info):

    self.logger = self.logger = logging.getLogger('ljp.drmaajs')

    self._drmaa = DrmaaJobs()
    try:
      self._drmaa.initSession()
    except DrmaaError, e:
      raise DRMError("Could not create the DRMAA session: %s" %e )

    self._drmaa_implementation = drmaa_implementation

    self.parallel_job_submission_info = parallel_job_submission_info

    self.logger.debug("Parallel job submission info: %s", 
                      repr(parallel_job_submission_info))

    # patch for the PBS-torque DRMAA implementation
    if self._drmaa_implementation == "PBS":
      try:
        jobTemplateId = self._drmaa.allocateJobTemplate()
        self._drmaa.setCommand(jobTemplateId, "echo", [])
        self._drmaa.setAttribute(jobTemplateId, 
                                "drmaa_output_path", 
                                "[void]:/tmp/soma-workflow-empty-job.o")
        self._drmaa.setAttribute(jobTemplateId, 
                                "drmaa_error_path", 
                                "[void]:/tmp/soma-workflow-empty-job.e")
        self._drmaa.runJob(jobTemplateId)
      except DrmaaError, e:
        raise DRMError("%s" %e)
      ################################
    

  def job_submission(self, job):
    '''
    @type  job: soma.workflow.client.Job
    @param job: job to be submitted
    @rtype: string
    @return: drmaa job id 
    '''

    # patch for the PBS-torque DRMAA implementation
    command = []
    job_command = job.plain_command()
    if self._drmaa_implementation == "PBS":
      for command_el in job_command:
        command_el = command_el.replace('"', '\\\"')
        command.append("\"" + command_el + "\"")
      self.logger.debug("PBS case, new command:" + repr(command))
    else:
      command = job_command

    self.logger.debug("command: " + repr(command))
    
    stdout_file = job.plain_stdout()
    stderr_file = job.plain_stderr()
    stdin = job.plain_stdin()
    job_env = []
    for var_name in os.environ.keys():
      job_env.append(var_name+"="+os.environ[var_name])

    try:
      drmaaJobId = self._drmaa.allocateJobTemplate()

      self._drmaa.setCommand(drmaaJobId, command[0], command[1:])

      self._drmaa.setAttribute(drmaaJobId, 
                              "drmaa_output_path", 
                              "[void]:" + stdout_file)
      
      if job.join_stderrout:
        self._drmaa.setAttribute(drmaaJobId,
                                "drmaa_join_files", 
                                "y")
      else:
        if stderr_file:
          self._drmaa.setAttribute(drmaaJobId, 
                                  "drmaa_error_path", 
                                  "[void]:" + stderr_file)
      
      if job.stdin:
        self.logger.debug("stdin: " + repr(stdin))
        self._drmaa.setAttribute(drmaaJobId, 
                                "drmaa_input_path", 
                                "[void]:" + stdin)
        
      working_directory = job.plain_working_directory()
      if working_directory:
        self._drmaa.setAttribute(drmaaJobId, "drmaa_wd", working_directory)

      if job.queue:
        self._drmaa.setAttribute(drmaaJobId, "drmaa_native_specification", "-q " + str(job.queue))

      #self._drmaa.setAttribute(drmaaJobId, "drmaa_native_specification", "-l h_rt=0:0:30" )
      
      if job.parallel_job_info :
        parallel_config_name, max_node_number = job.parallel_job_info
        self._setDrmaaParallelJob(drmaaJobId, 
                                  parallel_config_name, 
                                  max_node_number)
        
      self._drmaa.setVectorAttribute(drmaaJobId, 'drmaa_v_env', job_env)

      drmaaSubmittedJobId = self._drmaa.runJob(drmaaJobId)
      self._drmaa.deleteJobTemplate(drmaaJobId)
    except DrmaaError, e:
      raise DRMError("Job submission error: %s" %(e))

    return drmaaSubmittedJobId
    

  def get_job_status(self, drmaa_job_id):
    try:
      status = self._drmaa.jobStatus(drmaa_job_id)
    except DrmaaError, e:
      raise DRMError("%s" %(e))
    return status


  def get_job_exit_info(self, drmaa_job_id):
    exit_status, exit_value, term_sig, resource_usage = self._drmaa.wait(drmaa_job_id, 0)

    str_rusage = ''
    for rusage in resource_usage:
      str_rusage = str_rusage + rusage + ' '

    return (exit_status, exit_value, term_sig, str_rusage)

  def _setDrmaaParallelJob(self, 
                           drmaa_job_template_id, 
                           configuration_name, 
                           max_num_node):
    '''
    Set the DRMAA job template information for a parallel job submission.
    The configuration file must provide the parallel job submission 
    information specific to the cluster in use. 

    @type  drmaa_job_template_id: string 
    @param drmaa_job_template_id: id of drmaa job template
    @type  parallel_job_info: tuple (string, int)
    @param parallel_job_info: (configuration_name, max_node_num)
    configuration_name: type of parallel job as defined in soma.workflow.constants 
    (eg MPI, OpenMP...)
    max_node_num: maximum node number the job requests (on a unique machine or 
    separated machine depending on the parallel configuration)
    ''' 

    self.logger.debug(">> _setDrmaaParallelJob")
  
    cluster_specific_cfg_name = self.parallel_job_submission_info[configuration_name]
    
    for drmaa_attribute in constants.PARALLEL_DRMAA_ATTRIBUTES:
      value = self.parallel_job_submission_info.get(drmaa_attribute)
      if value: 
        #value = value.format(config_name=cluster_specific_cfg_name, max_node=max_num_node)
        value = value.replace("{config_name}", cluster_specific_cfg_name)
        value = value.replace("{max_node}", repr(max_num_node))
        self._drmaa.setAttribute( drmaa_job_template_id, 
                                    drmaa_attribute, 
                                    value)
        self.logger.debug("Parallel job, drmaa attribute = %s, value = %s ",
                          drmaa_attribute, value) 


    job_env = []
    for parallel_env_v in constants.PARALLEL_JOB_ENV:
      value = self.parallel_job_submission_info.get(parallel_env_v)
      if value: job_env.append(parallel_env_v+'='+value.rstrip())
    
    self._drmaa.setVectorAttribute(drmaa_job_template_id, 'drmaa_v_env', job_env)
    self.logger.debug("Parallel job environment : " + repr(job_env))
        
    self.logger.debug("<< _setDrmaaParallelJob")


  def kill_job(self, job_drmaa_id):
    try:
      self._drmaa.terminate(job_drmaa_id)
    except DrmaaError, e:
      raise DRMError('%s' %e)

class EngineTransfer(FileTransfer):
  
  engine_path = None

  status = None

  disposal_timeout = None

  workflow_id = None

  def __init__(self, client_file_transfer):
 
    exist_on_client = client_file_transfer.initial_status == constants.FILES_ON_CLIENT
    super(EngineTransfer, self).__init__( exist_on_client,
                                          client_file_transfer.client_path,
                                          client_file_transfer.disposal_timeout,
                                          client_file_transfer.name,
                                          client_file_transfer.client_paths)

    self.status = self.initial_status

    workflow_id = -1


  def files_exist_on_server(self):

    exist = self.status == constants.FILES_ON_CR or \
            self.status == constants.FILES_ON_CLIENT_AND_CR or \
            self.status == constants.TRANSFERING_FROM_CR_TO_CLIENT
    return exist


class EngineJob(soma.workflow.client.Job):
  
  # job id
  job_id = None
  # workflow id 
  workflow_id = None
  # user_id
  _user_id = None  
  # string
  drmaa_id = None
  # name of the queue to be used to submit jobs, str
  queue = None
  # job status as defined in constants.JOB_STATUS. string
  status = None
  # last status update date
  last_status_update = None
  # exit status string as defined in constants. JOB_EXIT_STATUS
  exit_status = None
  # contains operating system exit value if the status is FINISHED_REGULARLY. 
  # int or None
  exit_value = None

  str_rusage = None
  # contain a representation of the signal if the status is FINISHED_TERM_SIG.
  # string or None
  terminating_signal = None

  expiration_date = None

  # mapping between FileTransfer and actual EngineTransfer which are valid on
  # the system. 
  # dictionary: FileTransfer -> EngineTransfer
  transfer_mapping = None

  # mapping between SharedResourcePath and actual path which are valid on the
  # system
  # dictonary: SharedResourcePath -> string (path)
  srp_mapping = None

  path_translation = None

  logger = None

  def __init__(self, 
               client_job, 
               queue, 
               workflow_id=-1, 
               path_translation=None,
               transfer_mapping=None,
               parallel_job_submission_info=None):

    super(EngineJob, self).__init__(client_job.command,
                                    client_job.referenced_input_files,
                                    client_job.referenced_output_files ,
                                    client_job.stdin,
                                    client_job.join_stderrout,
                                    client_job.disposal_timeout,
                                    client_job.name,
                                    client_job.stdout_file,
                                    client_job.stderr_file ,
                                    client_job.working_directory ,
                                    client_job.parallel_job_info)
    
    self.job_id = -1

    self.drmaa_id = None
    self.status = constants.NOT_SUBMITTED
    self.exit_status = None
    self.exit_value = None
    self.terminating_signal = None

    self.workflow_id = workflow_id
    self.queue = queue

    self.path_translation = path_translation

    if not transfer_mapping:
      self.transfer_mapping = {}
    else:
      self.transfer_mapping = transfer_mapping
    self.srp_mapping = {}

    self._map(parallel_job_submission_info)

  def _map(self, parallel_job_submission_info):
    '''
    Fill the transfer_mapping and srp_mapping attributes.
    + check the types of the Job arguments.
    '''
    if not self.command:
      raise JobError("The command attribute is the only required "
                     "attribute of Job.")

    if self.parallel_job_info:
      parallel_config_name, max_node_number = self.parallel_job_info
      if not parallel_job_submission_info:
        raise JobError("No parallel information was registered for the "
                       " current resource. A parallel job can not be submitted")
      if parallel_config_name not in parallel_job_submission_info:
        raise JobError("The parallel job can not be submitted because the "
                        "parallel configuration %s is missing." %(configuration_name))

    # transfer_mapping from referenced_input_files and referenced_output_files
    # + type checking
    for ft in self.referenced_input_files:
      if not isinstance(ft, FileTransfer):
        raise JobError("%s: Wrong type in referenced_input_files. "
                       " FileTransfer object required." %(repr(ft)))
      elif isinstance(ft, EngineTransfer) and \
          ft not in self.transfer_mapping:
        # TBI check that the transfer exist in the database 
        self.transfer_mapping[ft] = ft
      elif ft not in self.transfer_mapping:
        eft = EngineTransfer(ft)
        self.transfer_mapping[ft] = eft
   
    for ft in self.referenced_output_files:
      if not isinstance(ft, FileTransfer):
        raise JobError("%s: Wrong type in referenced_output_files. "
                       " FileTransfer object required." %(repr(ft)))
      elif isinstance(ft, EngineTransfer) and \
          ft not in self.transfer_mapping:
        # TBI check that the transfer exist in the database 
        self.transfer_mapping[ft] = ft
      elif ft not in self.transfer_mapping:
        eft = EngineTransfer(ft)
        self.transfer_mapping[ft] = eft

    # filling the srp_mapping exploring the command, stdin, stdout_file, 
    #stderr_file, workflow_directory 
    # + type checking
    for command_el in self.command:
      if isinstance(command_el, SharedResourcePath):
        self.srp_mapping[command_el] = self._translate(command_el)
      elif isinstance(command_el, FileTransfer):
        if not command_el in self.transfer_mapping:
          raise JobError("The FileTransfer objets used in the "
                         "command must be declared in the Job "
                         "attributes: referenced_input_files "
                         "and referenced_output_files.")

      elif isinstance(command_el, tuple) and \
           isinstance(command_el[0], FileTransfer):
        if not command_el[0] in self.transfer_mapping:
          raise JobError("The FileTransfer objets used in the "
                         "command must be declared in the Job "
                         "attributes: referenced_input_files "
                         "and referenced_output_files.")
      elif isinstance(command_el, list):
        new_list = []
        for list_el in command_el:
          if isinstance(list_el, SharedResourcePath):
            self.srp_mapping[list_el] = self._translate(list_el) 
          elif isinstance(list_el, FileTransfer):
            if not list_el in self.transfer_mapping:
              raise JobError("The FileTransfer objets used in the "
                             "command must be declared in the Job "
                             "attributes: referenced_input_files "
                             "and referenced_output_files.")
          elif isinstance(list_el, tuple) and \
              isinstance(list_el[0], FileTransfer):
            if not list_el[0] in self.transfer_mapping:
              raise JobError("The FileTransfer objets used in the "
                             "command must be declared in the Job "
                             "attributes: referenced_input_files "
                             "and referenced_output_files.")
          else:
            if not type(list_el) in types.StringTypes:
              raise JobError("Wrong command element type: %s" %(repr(list_el)))
      else:
        if not type(command_el) in types.StringTypes:
          raise JobError("Wrong command element type: %s" %(repr(command_el)))
      
    if self.stdin:
      if isinstance(self.stdin, FileTransfer):
        if not self.stdin in self.transfer_mapping:
          self.referenced_input_files.append(self.stdin)
          if isinstance(self.stdin, EngineTransfer):
            # TBI check that the transfer exist in the database 
            self.transfer_mapping[self.stdin] = self.stdin
          else:
            eft = EngineTransfer(self.stdin)
            self.transfer_mapping[self.stdin] = eft
      elif isinstance(self.stdin, SharedResourcePath):
        self.srp_mapping[self.stdin] = self._translate(self.stdin) 
      else:
        if not type(self.stdin) in types.StringTypes:
          raise JobError("Wrong stdin type: %s" %(repr(self.stdin))) 

    if self.working_directory:
      if isinstance(self.working_directory, FileTransfer):
        if not self.working_directory in self.transfer_mapping:
          self.referenced_input_files.append(self.working_directory)
          self.referenced_output_files.append(self.working_directory)
          if isinstance(self.working_directory, EngineTransfer):
            # TBI check that the transfer exist in the database 
            self.transfer_mapping[self.working_directory] = self.working_directory
          else:
            eft = EngineTransfer(self.working_directory)
            self.transfer_mapping[self.working_directory] = eft
      elif isinstance(self.working_directory, SharedResourcePath):
        self.srp_mapping[self.working_directory] = self._translate(self.working_directory)
      else:
        if not type(self.working_directory) in types.StringTypes:
          raise JobError("Wrong working directory type: %s " %
                         (repr(self.working_directory)))

    if self.stdout_file:
      if isinstance(self.stdout_file, FileTransfer):
        if not self.stdout_file in self.transfer_mapping:
          self.referenced_output_files.append(self.stdout_file)
          if isinstance(self.stdout_file, EngineTransfer):
            # TBI check that the transfer exist in the database 
            self.transfer_mapping[self.stdout_file] = self.stdout_file
          else:
            eft = EngineTransfer(self.stdout_file)
            self.transfer_mapping[self.stdout_file] = eft
      elif isinstance(self.stdout_file, SharedResourcePath):
        self.srp_mapping[self.stdout_file] = self._translate(self.stdout_file) 
      else:
        if not type(self.stdout_file) in types.StringTypes:
          raise JobError("Wrong stdout_file type: %s" %(repr(self.stdout_file))) 

    if self.stderr_file:
      if isinstance(self.stderr_file, FileTransfer):
        if not self.stderr_file in self.transfer_mapping:
          self.referenced_output_files.append(self.stderr_file)
          if isinstance(self.stderr_file, EngineTransfer):
            # TBI check that the transfer exist in the database 
            self.transfer_mapping[self.stderr_file] = self.stderr_file
          else:
            eft = EngineTransfer(self.stderr_file)
            self.transfer_mapping[self.stderr_file] = eft
      elif isinstance(self.stderr_file, SharedResourcePath):
        self.srp_mapping[self.stderr_file] = self._translate(self.stderr_file) 
      else:
        if not type(self.stderr_file) in types.StringTypes:
          raise JobError("Wrong stderr_file type: %s" %(repr(self.stderr_file))) 

  def _translate(self, srp):
    '''
    srp: SharedResourcePath
    returns: the translated path
    '''
    
    if not self.path_translation:
      raise JobError("The job uses SharedResourcePath while no "
                     "translation were found.")
    if not srp.namespace in self.path_translation.keys():
      raise JobError("SharedResourcePath translation: the "               
                     "namespace %s does not exist" %(srp.namespace))
    if not srp.uuid in self.path_translation[srp.namespace]:
      raise JobError("SharedResourcePath translation: "
                                   "the uuid %s does not exist for the " 
                                   "namespace %s." %
                                    (srp.uuid, srp.namespace))
    translated_path = os.path.join(self.path_translation[srp.namespace][srp.uuid],
                                   srp.relative_path)
    return translated_path

  def plain_command(self):
    '''
    Compute the actual job command (sequence of string) from the command 
    holding FileTransfer and SharedResourcePath objects.

    returns: sequence of string
    '''
    plain_command = []
    for command_el in self.command:
      if isinstance(command_el, SharedResourcePath):
        plain_command.append(self.srp_mapping[command_el])
      elif isinstance(command_el, FileTransfer):
        plain_command_el = self.transfer_mapping[command_el].engine_path
        plain_command.append(plain_command_el)
      elif isinstance(command_el, tuple) and \
           isinstance(command_el[0], FileTransfer):
        plain_command_el = self.transfer_mapping[command_el[0]].engine_path
        plain_command_el = os.path.join(plain_command_el, command_el[1])
        plain_command.append(plain_command_el)
      elif isinstance(command_el, list):
        new_list = []
        for list_el in command_el:
          if isinstance(list_el, SharedResourcePath):
            new_list.append(self.srp_mapping[list_el])
          elif isinstance(list_el, FileTransfer):
            new_list_el = self.transfer_mapping[list_el].engine_path
            new_list.append(new_list_el)
          elif isinstance(list_el, tuple) and \
              isinstance(list_el[0], FileTransfer):
            new_list_el = self.transfer_mapping[list_el[0]].engine_path
            new_list_el = os.path.join(new_list_el, list_el[1])
            new_list.append(new_list_el)
          else:
            assert(type(list_el) in types.StringTypes)
            new_list.append(list_el)
        plain_command.append(str(repr(new_list)))
      else:
        assert(type(command_el) in types.StringTypes)
        plain_command.append(command_el)
    return plain_command
    
  def plain_stdin(self):
    if self.stdin and isinstance(self.stdin, FileTransfer):
      return self.transfer_mapping[self.stdin].engine_path
    if self.stdin and isinstance(self.stdin, SharedResourcePath):
      return self.srp_mapping[self.stdin]
    return self.stdin

  def plain_stdout(self):
    if self.stdout_file and isinstance(self.stdout_file, FileTransfer):
      return self.transfer_mapping[self.stdout_file].engine_path
    if self.stdout_file and isinstance(self.stdout_file, SharedResourcePath):
      return self.srp_mapping[self.stdout_file]
    return self.stdout_file

  def plain_stderr(self):
    if self.stderr_file and isinstance(self.stderr_file, FileTransfer):
      return self.transfer_mapping[self.stderr_file].engine_path
    if self.stderr_file and isinstance(self.stderr_file, SharedResourcePath):
      return self.srp_mapping[self.stderr_file]
    return self.stderr_file

  def plain_working_directory(self):
    if self.working_directory and \
       isinstance(self.working_directory, FileTransfer):
      return self.transfer_mapping[self.working_directory].engine_path
    if self.working_directory and \
       isinstance(self.working_directory, SharedResourcePath):
      return self.srp_mapping[self.working_directory]
    return self.working_directory


  def is_running(self):
    running = self.status != constants.NOT_SUBMITTED and \
              self.status != constants.FAILED and \
              self.status != constants.DONE 
    return running

  def is_done(self):
    done = self.status == constants.DONE or self.status == constants.FAILED
    return done

  def failed(self):
    failed = (self.is_done() and \
              ((self.exit_value != 0 and self.exit_value != None) or \
              self.exit_status != constants.FINISHED_REGULARLY or \
              self.terminating_signal != None)) or \
             self.status == constants.WARNING
    return failed

  def ended_with_success(self):
    success = self.is_done() and \
              self.exit_value == 0 and \
              self.exit_status == constants.FINISHED_REGULARLY and \
              self.terminating_signal == None
    return success



class EngineWorkflow(soma.workflow.client.Workflow):
  
  # workflow id
  wf_id = None
  # user id
  _user_id = None
  # path translation for each namespace a dictionary holding the traduction 
  #(association uuid => engine path)
  # dictionary, namespace => uuid => path
  _path_translation = None
  # workflow status as defined in constants.WORKFLOW_STATUS
  status = None
  # expidation date
  expiration_date = None
  # name of the queue to be used to submit jobs, str
  queue = None
  
  # mapping between Job and actual EngineJob which are valid on the system
  # dictionary: Job -> EngineJob
  job_mapping = None
  
  # mapping between SharedResourcePath and actual path which are valid on the
  # system
  # dictonary: SharedResourcePath -> string (path)
  transfer_mapping = None

  # Once registered on the database server each 
  # EngineJob has an job_id.
  # dictonary: job_id -> EngineJob
  registered_jobs = None
  
  # Once registered on the database server each 
  # EngineTransfer has an transfer_id.
  # dictonary: tr_id -> EngineTransfer
  registered_tr = None

  #logger = None
  
  def __init__(self, 
               client_workflow, 
               path_translation, 
               queue, 
               expiration_date, 
               name):
 
    super(EngineWorkflow, self).__init__(client_workflow.jobs,
                                         client_workflow.dependencies,
                                         client_workflow.root_group,
                                         client_workflow.groups)
    self.wf_id = -1

    self.status = constants.WORKFLOW_NOT_STARTED
    self._path_translation = path_translation
    self.queue = queue
    self.expiration_date = expiration_date
    self.name = name

    self.job_mapping = {}
    self.transfer_mapping = {}
    self._map()

    self.registered_tr = {}
    self.registered_jobs = {}

  def _map(self):
    '''
    Fill the job_mapping attributes.
    + type checking
    '''
    # jobs
    for job in self.jobs:
      if not isinstance(job, Job):
        raise WorkflowError("%s: Wrong type in the jobs attribute. "
                            " An object of type Job is required." %(repr(job)))
      if job not in self.job_mapping:
        ejob = EngineJob(client_job=job,
                         queue=self.queue,
                         path_translation=self._path_translation,
                         transfer_mapping=self.transfer_mapping)
        self.transfer_mapping.update(ejob.transfer_mapping)
        self.job_mapping[job]=ejob

    # dependencies
    for dependency in self.dependencies:
      if not isinstance(dependency[0], Job) or \
         not isinstance(dependency[1], Job):
          raise WorkflowError("%s, %s: Wrong type in the workflow dependencies."
                              " An object of type Job is required." %
                              (repr(dependency[0]), repr(dependency[1])))


      if dependency[0] not in self.job_mapping:
        self.jobs.append(dependency[0])
        ejob = EngineJob( client_job=dependency[0],
                          queue=self.queue,
                          path_translation=self.path_translation,
                          transfer_mapping=self.transfer_mapping)
        self.transfer_mapping.update(ejob.transfer_mapping)
        self.job_mapping[dependency[0]]=ejob

      if dependency[1] not in self.job_mapping:
        self.jobs.append(dependency[1])
        ejob = EngineJob( client_job=dependency[1],
                          queue=self.queue,
                          path_translation=self.path_translation,
                          transfer_mapping=self.transfer_mapping)
        self.transfer_mapping.update(ejob.transfer_mapping)
        self.job_mapping[dependency[1]]=ejob
      
    # groups
    groups = self.groups 
    for group in self.groups:
      for elem in group.elements:
        if isinstance(elem, Job):
          if elem not in self.job_mapping:
            self.jobs.append(elem)
            ejob = EngineJob( client_job=elem,
                              queue=self.queue,
                              path_translation=self.path_translation,
                              transfer_mapping=self.transfer_mapping)
            self.transfer_mapping.update(ejob.transfer_mapping)
            self.job_mapping[elem]=ejob
        elif not isinstance(elem, Group):
          raise WorkflowError("%s: Wrong type in the workflow "
                              "groups. Objects of type Job or " 
                              "Group are required." %(repr(elem)))

    # root group
    for elem in self.root_group:
      if isinstance(elem, Job):
        if elem not in self.job_mapping:
          self.jobs.append(elem)
          ejob = EngineJob( client_job=elem,
                            queue=self.queue,
                            path_translation=self.path_translation,
                            transfer_mapping=self.transfer_mapping)
          self.transfer_mapping.update(ejob.transfer_mapping)
          self.job_mapping[elem]=ejob
      elif not isinstance(elem, Group):
        raise WorkflowError("%s: Wrong type in the workflow root_group."
                            " Objects of type Job or Group are required." %
                            (repr(elem)))
    

  def find_out_independant_jobs(self):
    independant_jobs = []
    for job in self.jobs:
      to_run=True
      for ft in job.referenced_input_files:
        if not self.transfer_mapping[ft].files_exist_on_server():
          if self.transfer_mapping[ft].status == constants.TRANSFERING_FROM_CR_TO_CLIENT:
              #TBI stop the transfer
              pass 
          to_run = False
          break 
      if to_run:
        for dep in self.dependencies:
          if dep[1] == job:
            to_run = False
            break
      if to_run:
        independant_jobs.append(self.job_mapping[job])
    if independant_jobs:
      status = constants.WORKFLOW_IN_PROGRESS
    else:
      status = self.status
    return (independant_jobs, status)

  def find_out_jobs_to_process(self):
    '''
    Workflow exploration to find out new node to process.

    @rtype: tuple (sequence of EngineJob,
                   sequence of EngineJob,
                   constanst.WORKFLOW_STATUS)
    @return: (jobs to run,
              ended jobs
              workflow status)
    '''

    logger = logging.getLogger('engine.EngineWorkflow') 
    to_run = []
    to_abort = set([])
    done = []
    running = []
    for client_job in self.jobs:
      job = self.job_mapping[client_job]
      if job.is_done(): 
        done.append(job)
      elif job.is_running(): 
        running.append(job)
      logger.debug("job " + repr(job.name) + " " + repr(job.status) + " r " + repr(job.is_running()) + " d " + repr(job.is_done()))
      if job.status == constants.NOT_SUBMITTED:
        # a job can start to run when all its dependencies succeed and 
        # all its input files are in the FILES_ON_CR or 
        # FILES_ON_CLIENT_AND_CR or TRANSFERING_FROM_CLIENT_TO_CR
        job_to_run = True 
        for ft in job.referenced_input_files:
          eft = job.transfer_mapping[ft]
          if not eft.files_exist_on_server():
            if eft.status == constants.TRANSFERING_FROM_CR_TO_CLIENT:
              #TBI stop the transfer
              pass 
            job_to_run = False
            break
        for dep in self.dependencies:
          job_a = self.job_mapping[dep[0]]
          job_b = self.job_mapping[dep[1]]
          if job_b == job and not job_a.ended_with_success(): 
            job_to_run = False
            if job_a.failed():
              to_abort.add(job)
            break
        if job_to_run: 
          wf_running = True
          to_run.append(job)

    logger.debug(" ")
    logger.debug("to run " + repr(to_run))
    logger.debug("to abort " + repr(to_abort))
    logger.debug("done " + repr(done))
    logger.debug("running " + repr(running))

    # if a job fails the whole workflow branch has to be stopped
    # look for the node in the branch to abort
    previous_size = 0
    while previous_size != len(to_abort):
      previous_size = len(to_abort)
      for dep in self.dependencies:
        job_a = self.job_mapping[dep[0]]
        job_b = self.job_mapping[dep[1]]
        if job_a in to_abort and not job_b in to_abort:
          to_abort.add(job_b)
          break

    # stop the whole branch
    ended_jobs = {}
    for job in to_abort:
      if job.job_id:
        #self.logger.debug("  ---- Failure: job to abort " + job.name)
        assert(job.status == constants.NOT_SUBMITTED)
        ended_jobs[job.job_id] = job
        job.status = constants.FAILED
        job.exit_status = constants.EXIT_ABORTED


    if len(running) + len(to_run) > 0:
      status = constants.WORKFLOW_IN_PROGRESS
    elif len(done) + len(to_abort) == len(self.jobs): 
      status = constants.WORKFLOW_DONE
    elif len(done) > 0:
      status = constants.WORKFLOW_IN_PROGRESS
      # !!!! the workflow may be stuck !!!!
      # TBI
      logger.debug("!!!! The workflow may be stuck !!!!")
    else:
      status = constants.WORKFLOW_NOT_STARTED

    return (to_run, ended_jobs, status)

  def restart(self, database_server):
    assert(self.status == constants.WORKFLOW_DONE or self.status == constants.WARNING)
    to_restart = False
    undone_jobs = []
   
    wf_status = database_server.get_detailed_workflow_status(self.wf_id)

    for job_info in wf_status[0]:
      job_id, status, exit_info, date_info = job_info
      self.registered_jobs[job_id].status = status
      exit_status, exit_value, term_signal, resource_usage = exit_info
      self.registered_jobs[job_id].exit_status = exit_status
      self.registered_jobs[job_id].exit_value = exit_value
      self.registered_jobs[job_id].str_rusage = resource_usage
      self.registered_jobs[job_id].terminating_signal = term_signal
   
    for ft_info in wf_status[1]:
      engine_path, client_path, status, transfer_action_info = ft_info 
      self.registered_tr[engine_path].status = status

    done = True
    for job in self.registered_jobs.itervalues():
      if job.failed():
        #clear all the information related to the previous job submission
        job.status = constants.NOT_SUBMITTED
        job.exit_status = None
        job.exit_value = None
        job.terminating_signal = None
        job.drmaa_id = None
        stdout = open(job.stdout_file, "w")
        stdout.close()
        stderr = open(job.stderr_file, "w")
        stderr.close()
        database_server.set_submission_information(job.job_id, None, None)
        database_server.set_job_status(job.job_id, constants.NOT_SUBMITTED)
      if not job.ended_with_success():
        undone_jobs.append(job)

    to_run = []
    if undone_jobs:
      # look for jobs to run
      for job in undone_jobs:
        job_to_run = True # a node is run when all its dependencies succeed
        for ft in job.referenced_input_files:
          eft = self.transfer_mapping[ft]
          if not eft.files_exist_on_server():
            if eft.status == constants.TRANSFERING_FROM_CR_TO_CLIENT:
              #TBI stop the transfer
              pass 
            job_to_run = False
            break 
        if job_to_run:
          for dep in self.dependencies:
            job_a = self.job_mapping[dep[0]]
            job_b = self.job_mapping[dep[1]]

            if job_b == job and not job_a.ended_with_success():
              job_to_run = False
              break

        if job_to_run: 
          to_run.append(job)
  
    if to_run:
      status = constants.WORKFLOW_IN_PROGRESS
    else:
      status = constants.WORKFLOW_DONE
     
    return (to_run, status)    
 


class WorkflowEngineLoop(object):

  # jobs managed by the current engine process instance. 
  # The workflows jobs are not duplicated here.
  # dict job_id -> registered job
  _jobs = None
  # workflows managed ny the current engine process instance. 
  # each workflow holds a set of EngineJob
  # dict workflow_id -> workflow 
  _workflows = None
  # Drmaa session
  # DrmaaHelper
  _engine_drmaa = None
  # database server proxy
  # soma.workflow.database_server
  _database_server = None
  # user_id
  _user_id = None
  # for each namespace a dictionary holding the traduction 
  #  (association uuid => engine path)
  # dictionary, namespace => uuid => path
  _path_translation = None
  # max number of job for some queues
  # dictionary, queue name (str) => max nb of job (int)
  _queue_limits = None
  # Submission pending queues.
  # For each limited queue, a submission pending queue is needed to store the
  # jobs that couldn't be submitted. 
  # Dictionary queue name (str) => pending jobs (list) 
  _pending_queues = None
  # boolean
  _running = None
  # boolean
  _j_wf_ended = None

  _lock = None

  logger = None

  def __init__(self, 
               database_server, 
               engine_drmaa, 
               path_translation=None,
               queue_limits={}):
    
    self.logger = logging.getLogger('engine.WorkflowEngineLoop')

    self._jobs = {} 
    self._workflows = {}
    
    self._database_server = database_server
    
    self._engine_drmaa = engine_drmaa

    self._path_translation = path_translation

    self._queue_limits = queue_limits

    self.logger.debug('queue_limits ' + repr(self._queue_limits))

    self._pending_queues = {} 

    self._running = False

    try:
      userLogin = pwd.getpwuid(os.getuid())[0] 
    except Exception, e:
      self.logger.critical("Couldn't identify user %s: %s \n" %(type(e), e))
      raise SystemExit
  
    self._user_id = self._database_server.register_user(userLogin) 
    self.logger.debug("user_id : " + repr(self._user_id))

    self._j_wf_ended = True

    self._lock = threading.RLock()

  def are_jobs_and_workflow_done(self):
    with self._lock:
      ended = len(self._jobs) == 0 and len(self._workflows) == 0
      return ended

  def start_loop(self, time_interval):
    '''
    Start the workflow engine loop. The loop will run until stop() is called.
    '''
    self._running = True
    while True:
      if not self._running:
        break
      with self._lock:
        # --- 1. Jobs and workflow deletion and kill ------------------------
        # Get the jobs and workflow with the status DELETE_PENDING 
        # and KILL_PENDING
        (jobs_to_delete, jobs_to_kill) = self._database_server.jobs_to_delete_and_kill(self._user_id)
        (wf_to_delete, wf_to_kill) = self._database_server.workflows_to_delete_and_kill(self._user_id)

        # Delete and kill properly the jobs and workflows in _jobs and _workflows
        for job_id in jobs_to_kill + jobs_to_delete:
          if job_id in self._jobs:
            self.logger.debug(" stop job " + repr(job_id))
            self._stop_job(job_id,  self._jobs[job_id])
            if job_id in jobs_to_delete:
              self.logger.debug("Delete job : " + repr(job_id))
              self._database_server.delete_job(job_id)
              del self._jobs[job_id]

        for wf_id in wf_to_kill + wf_to_delete:
          if wf_id in self._workflows:
            self.logger.debug("Kill workflow : " + repr(wf_id))
            self._stop_wf(wf_id)
            if wf_id in wf_to_delete:
              self.logger.debug("Delete workflow : " + repr(wf_id))
              self._database_server.delete_workflow(wf_id)
              del self._workflows[wf_id]
        
        # --- 2. Update job status from DRMAA -------------------------------
        # get back the termination status and terminate the jobs which ended 
        wf_to_inspect = set() # set of workflow id
        wf_jobs = {}
        wf_transfers = {}
        for wf in self._workflows.itervalues():
          # TBI add a condition on the workflow status
          wf_jobs.update(wf.registered_jobs)
          wf_transfers.update(wf.registered_tr)
        
        ended_jobs = {}
        for job in itertools.chain(self._jobs.itervalues(), wf_jobs.itervalues()):
          if job.exit_status == None and job.drmaa_id != None:
            try:
              job.status = self._engine_drmaa.get_job_status(job.drmaa_id)
            except DrmaaError, e:
              job.status = constants.UNDETERMINED
            self.logger.debug("job " + repr(job.job_id) + " : " + job.status)
            if job.status == constants.DONE or job.status == constants.FAILED:
              self.logger.debug("End of job %s, drmaaJobId = %s", 
                                job.job_id, job.drmaa_id)
              (job.exit_status, 
              job.exit_value, 
              job.terminating_signal, 
              job.str_rusage) = self._engine_drmaa.get_job_exit_info(job.drmaa_id)
              if job.workflow_id != -1: 
                wf_to_inspect.add(job.workflow_id)
              if job.status == constants.DONE:
                for ft in job.referenced_output_files:
                  engine_path = job.transfer_mapping[ft].engine_path
                  self._database_server.set_transfer_status(engine_path, 
                                                  constants.FILES_ON_CR)
                     
              ended_jobs[job.job_id] = job
              self.logger.debug("  => exit_status " + repr(job.exit_status))
              self.logger.debug("  => exit_value " + repr(job.exit_value))
              self.logger.debug("  => signal " + repr(job.terminating_signal))


        # --- 3. Get back transfered status ----------------------------------
        for engine_path, transfer in wf_transfers.iteritems():
          status = self._database_server.get_transfer_status(engine_path)
          transfer.status = status

        for wf_id in self._workflows.iterkeys():
          if self._database_server.pop_workflow_ended_transfer(wf_id):
            self.logger.debug("ended transfer for the workflow " + repr(wf_id))
            wf_to_inspect.add(wf_id)

        # --- 4. Inspect workflows -------------------------------------------
        #self.logger.debug("wf_to_inspect " + repr(wf_to_inspect))
        for wf_id in wf_to_inspect:
          (to_run, 
          aborted_jobs, 
          status) = self._workflows[wf_id].find_out_jobs_to_process()
          self._workflows[wf_id].status = status
          self.logger.debug("NEW status wf " + repr(wf_id) + " " + repr(status))
          #jobs_to_run.extend(to_run)
          ended_jobs.update(aborted_jobs)
          for job in to_run:
            self._pend_for_submission(job)

        # --- 5. Check if pending jobs can now be submitted ------------------
        jobs_to_run = self._get_pending_job_to_submit()

        # --- 6. Submit jobs -------------------------------------------------
        for job in jobs_to_run:
          job.drmaa_id = self._engine_drmaa.job_submission(job)
          self._database_server.set_submission_information(job.job_id,
                                                          job.drmaa_id,
                                                          datetime.now())
          job.status = constants.UNDETERMINED
        

        # --- 7. Update the workflow and jobs status to the database_server -
        ended_job_ids = []
        ended_wf_ids = []
        #self.logger.debug("update job and wf status ~~~~~~~~~~~~~~~ ")
        for job_id, job in itertools.chain(self._jobs.iteritems(),
                                          wf_jobs.iteritems()):
          self._database_server.set_job_status(job.job_id, job.status)
          self._j_wf_ended = self._j_wf_ended and \
                                    (job.status == constants.DONE or \
                                    job.status == constants.FAILED)
          if job_id in self._jobs and \
             (job.status == constants.DONE or \
              job.status == constants.FAILED):
              ended_job_ids.append(job_id)
          self.logger.debug("job " + repr(job_id) + " " + repr(job.status))

        for job_id, job in ended_jobs.iteritems():
          self._database_server.set_job_exit_info(job_id, 
                                                  job.exit_status, 
                                                  job.exit_value, 
                                                  job.terminating_signal, 
                                                  job.str_rusage)
        for wf_id, workflow in self._workflows.iteritems():
          self._database_server.set_workflow_status(wf_id, workflow.status)
          if workflow.status == constants.WORKFLOW_DONE:
            ended_wf_ids.append(wf_id)
          self.logger.debug("wf " + repr(wf_id) + " " + repr(workflow.status))
        #self.logger.debug("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ")
        
        for job_id in ended_job_ids: del self._jobs[job_id]
        for wf_id in ended_wf_ids: del self._workflows[wf_id]

      time.sleep(time_interval)

    
  def stop_loop(self):
    self._running = False

  def add_job(self, client_job, queue):
    # register
    engine_job = EngineJob(client_job=client_job, 
                           queue=queue, 
                           path_translation=self._path_translation)
                          

    engine_job = self._database_server.add_job(self._user_id, engine_job)

    # create standard output files 
    try:  
      tmp = open(engine_job.stdout_file, 'w')
      tmp.close()
    except Exception, e:
      self._database_server.delete_job(engine_job.job_id)
      raise JobError("Could not create the standard output file " 
                     "%s %s: %s \n"  %
                     (repr(engine_job.stdout_file), type(e), e))
    if engine_job.stderr_file:
      try:
        tmp = open(engine_job.stderr_file, 'w')
        tmp.close()
      except Exception, e:
        self._database_server.delete_job(engine_job.job_id)
        raise JobError("Could not create the standard error file "
                       "%s: %s \n"  %(type(e), e))

    for transfer in engine_job.transfer_mapping.itervalues():
      if transfer.client_paths and not os.path.isdir(transfer.engine_path):
        try:
          os.mkdir(transfer.engine_path) 
        except Exception, e:
          self._database_server.delete_job(engine_job.job_id)
          raise JobError("Could not create the directory %s %s: %s \n"  %
                         (repr(transfer.engine_path), type(e), e))

    # submit
    self._pend_for_submission(engine_job)
    # add to the engine managed job list
    with self._lock:
      self._jobs[engine_job.job_id] = engine_job

    return engine_job


  def _pend_for_submission(self, engine_job):
    '''
    All the job submission are actually done in the loop (start_loop method).
    The jobs to submit after add_job, add_workflow and restart_workflow are 
    first stored in _pending_queues waiting to be submitted.
    '''
    if engine_job.queue in self._pending_queues:
      self._pending_queues[engine_job.queue].append(engine_job)
    else:
      self._pending_queues[engine_job.queue] = [engine_job]
    engine_job.status = constants.SUBMISSION_PENDING


  def _get_pending_job_to_submit(self):
    '''
    @rtype: list of EngineJob
    @return: the list of job to be submitted 
    '''    
    to_run = []
    for queue_name, jobs in self._pending_queues.iteritems():
      if jobs and queue_name in self._queue_limits:
        nb_queued_jobs = self._database_server.nb_queued_jobs(self._user_id, 
                                                              queue_name)
        nb_jobs_to_run = self._queue_limits[queue_name] - nb_queued_jobs
        while nb_jobs_to_run > 0 and \
              len(self._pending_queues[queue_name]) > 0:
          to_run.append(self._pending_queues[queue_name].pop(0))
          nb_jobs_to_run = nb_jobs_to_run - 1
      else:
        to_run.extend(jobs)
        self._pending_queues[queue_name] = []
    return to_run
    


  def add_workflow(self, client_workflow, expiration_date, name, queue):
    '''
    @type client_workflow: soma.workflow.client.Workflow
    @type expiration_date: datetime.datetime
    @type name: str
    @type queue: str
    '''
    # register
    engine_workflow = EngineWorkflow(client_workflow, 
                                     self._path_translation,
                                     queue,
                                     expiration_date,
                                     name)
   
    engine_workflow = self._database_server.add_workflow(self._user_id, engine_workflow)

    for job in engine_workflow.job_mapping.itervalues():
      try:  
        tmp = open(job.stdout_file, 'w')
        tmp.close()
      except Exception, e:
        self._database_server.delete_workflow(engine_workflow.wf_id)
        raise JobError("Could not create the standard output file " 
                       "%s %s: %s \n"  %
                       (repr(job.stdout_file), type(e), e))
      if job.stderr_file:
        try:
          tmp = open(job.stderr_file, 'w')
          tmp.close()
        except Exception, e:
          self._database_server.delete_workflow(engine_workflow.wf_id)
          raise JobError("Could not create the standard error file "
                         "%s %s: %s \n"  %
                         (repr(job.stderr_file), type(e), e))

    for transfer in engine_workflow.transfer_mapping.itervalues():
      if transfer.client_paths and not os.path.isdir(transfer.engine_path):
        try:
          os.mkdir(transfer.engine_path) 
        except Exception, e:
          self._database_server.delete_workflow(engine_workflow.wf_id)
          raise JobError("Could not create the directory %s %s: %s \n"  %
                         (repr(transfer.engine_path), type(e), e))

    # submit independant jobs
    (jobs_to_run, 
     engine_workflow.status) = engine_workflow.find_out_independant_jobs()
    for job in jobs_to_run:
      self._pend_for_submission(job)
    # add to the engine managed workflow list
    with self._lock:
      self._workflows[engine_workflow.wf_id] = engine_workflow

    return engine_workflow.wf_id

  def _stop_job(self, job_id, job):
    if job.status == constants.DONE or job.status == constants.FAILED:
      self._database_server.set_job_status( job_id, 
                                            job.status, 
                                            force = True)
    else:
      if job.drmaa_id:
        self.logger.debug("Kill job " + repr(job_id) + " drmaa id: " + repr(job.drmaa_id) + " status " + repr(job.status))
        self._engine_drmaa.kill_job(job.drmaa_id)
      elif job.queue and job in self._pending_queues[job.queue]:
        self._pending_queues[job.queue].remove(job)
      job.status = constants.FAILED
      job.exit_status = constants.USER_KILLED
      self._database_server.set_job_exit_info(job_id,
                                              constants.USER_KILLED,
                                              None,
                                              None,
                                              None)
      self._database_server.set_job_status(job_id, 
                                            constants.FAILED, 
                                            force = True)
    

  def _stop_wf(self, wf_id):
    wf = self._workflows[wf_id]
    self.logger.debug("wf.registered_jobs " + repr(wf.registered_jobs))
    for job_id, job in wf.registered_jobs.iteritems():
      self._stop_job(job_id, job)
    self._database_server.set_workflow_status(wf_id, 
                                              constants.WORKFLOW_DONE, 
                                              force = True)


  def restart_workflow(self, wf_id, status):
    workflow = self._database_server.get_engine_workflow(wf_id)
    workflow.status = status
    (jobs_to_run, workflow.status) = workflow.restart(self._database_server)
    for job in jobs_to_run:
      self._pend_for_submission(job)
    # add to the engine managed workflow list
    with self._lock:
      self._workflows[wf_id] = workflow

  def restart_job(self, job_id, status):
    (job, workflow_id) = self._database_server.get_engine_job(job_id)
    if workflow_id == -1: 
      job.status = status
      # submit
      self._pend_for_submission(job)
      # add to the engine managed job list
      with self._lock:
        self._jobs[job.job_id] = job
    else:
      
      pass
      #TBI


class WorkflowEngine(object):
  '''
  '''
  # database server
  # soma.workflow.database_server.WorkflowDatabaseServer
  _database_server = None
  # WorkflowEngineLoop
  _engine_loop = None
  # id of the user on the database server
  _user_id = None


  def __init__( self, database_server, engine_loop):
    ''' 
    @type  database_server:
           L{soma.workflow.database_server.WorkflowDatabaseServer}
    @type  engine_loop: L{WorkflowEngineLoop}
    '''
    
    self.logger = logging.getLogger('engine.WorkflowEngine')
    
    self._database_server= database_server
    self._engine_loop = engine_loop
    
    try:
      user_login = pwd.getpwuid(os.getuid())[0]
    except Exception, e:
      raise EngineError("Couldn't identify user %s: %s \n" %(type(e), e))
    
    self._user_id = self._database_server.register_user(user_login)
    self.logger.debug("user_id : " + repr(self._user_id))

  def __del__( self ):
    pass

  ########## FILE TRANSFER ###############################################

  def register_transfer(self, file_transfer): 
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
    engine_transfer = EngineTransfer(file_transfer)
    #engine_transfer.engine_path =  
    engine_transfer = self._database_server.add_transfer(engine_transfer, 
                                                         self._user_id)

    if engine_transfer.client_paths:
      os.mkdir(engine_transfer.engine_path)

    return engine_transfer

  def transfer_information(self, engine_path):
    '''
    @rtype: tuple (string, string, date, int, sequence)
    @return: (engine_file_path, 
              client_file_path, 
              expiration_date, 
              workflow_id,
              client_paths)
    '''
    return self._database_server.get_transfer_information(engine_path)


  def init_transfer_from_cr(self, engine_path):
    '''
    Initialize the transfer of a file or a directory from the computing resource 
    to the client.
    '''
    engine_path, client_path, expiration_date, workflow_id, client_paths = self.transfer_information(engine_path)
    status = self.transfer_status(engine_path)
    if status != constants.FILES_ON_CR and \
       status != constants.FILES_ON_CLIENT_AND_CR and \
       status != constants.TRANSFERING_FROM_CR_TO_CLIENT:
      self.logger.debug("!!!! transfer " + engine_path + " doesn't exist on engine side")
      # TBI raise
      return (None, None)
    content = None
    transfer_action_info = None
    if not client_paths:
      if os.path.isfile(engine_path):
        stat = os.stat(engine_path)
        file_size = stat.st_size
        md5_hash = hashlib.md5( open( engine_path, 'rb' ).read() ).hexdigest() 
        transfer_action_info = (file_size, md5_hash, constants.TR_FILE_CR_TO_C)
      elif os.path.isdir(engine_path):
        full_path_list = []
        for element in os.listdir(engine_path):
          full_path_list.append(os.path.join(engine_path, element))
        content = WorkflowController.dir_content(full_path_list)
        (cumulated_file_size, dir_element_action_info) = self._initializeDirectory(engine_path, content)
        transfer_action_info = (cumulated_file_size, dir_element_action_info, constants.TR_DIR_CR_TO_C)
    else: #client_paths
      full_path_list = []
      for element in os.listdir(engine_path):
        full_path_list.append(os.path.join(engine_path, element))
      content = WorkflowController.dir_content(full_path_list)
      (cumulated_file_size, dir_element_action_info) = self._initializeDirectory(engine_path, content)
      transfer_action_info = (cumulated_file_size, dir_element_action_info, constants.TR_MFF_CR_TO_C)

    self._database_server.set_transfer_status(engine_path,    
    constants.TRANSFERING_FROM_CR_TO_CLIENT)
    self._database_server.set_transfer_action_info(engine_path, transfer_action_info)     
    return (transfer_action_info, content)


  def init_file_transfer_to_cr(self, engine_path, file_size, md5_hash = None):
    '''
    Initialize the transfer of a file from the client to the computing resource
    '''
    transfer_action_info = (file_size, md5_hash, constants.TR_FILE_C_TO_CR)
    f = open(engine_path, 'w')
    f.close()
    self._database_server.set_transfer_status(engine_path,    
    constants.TRANSFERING_FROM_CLIENT_TO_CR)
    self._database_server.set_transfer_action_info(engine_path,     
                                                   transfer_action_info)
    return transfer_action_info

  def init_dir_transfer_to_cr(self, engine_path, content, transfer_type):
    '''
    Initialize the transfer of a directory from the client to the computing 
    resource. 
    Transfer_type => TR_DIR_C_TO_CR or TR_MFF_C_TO_CR
    '''
    cumulated_file_size, dir_element_action_info = self._initializeDirectory(engine_path, content)
    transfer_action_info = (cumulated_file_size, 
                            dir_element_action_info, 
                            transfer_type)
    WorkflowController.create_dir_structure(engine_path, content)
    self._database_server.set_transfer_status(engine_path, 
                                        constants.TRANSFERING_FROM_CLIENT_TO_CR)
    self._database_server.set_transfer_action_info(engine_path, 
                                                   transfer_action_info)
    return transfer_action_info



  def _initializeDirectory(self, engine_path, content, subdirectory = ""):
    '''
    Initialize engine directory from the content of client directory.

    @rtype : tuple (int, dictionary)
    @return : (cumulated file size, dictionary : relative file path => (file_size, md5_hash))
    '''
    dir_element_action_info = {}
    cumulated_file_size = 0
    for item, description, md5_hash in content:
      relative_path = os.path.join(subdirectory,item)
      full_path = os.path.join(engine_path, relative_path)
      if isinstance(description, list):
        #os.mkdir(full_path)
        sub_size, sub_dir_element_action_info = self._initializeDirectory( engine_path, description, relative_path)
        cumulated_file_size += sub_size
        dir_element_action_info.update(sub_dir_element_action_info)
      else:
        file_size = description
        dir_element_action_info[relative_path] = (file_size, md5_hash)
        cumulated_file_size += file_size

    return (cumulated_file_size, dir_element_action_info)

  def write_to_computing_resource_file(self, 
                                       engine_path, 
                                       data, 
                                       relative_path=None):
    '''
    @rtype : boolean
    @return : transfer ended
    '''
    if not relative_path:
      # File case
      (file_size, md5_hash, transfer_type) = self._database_server.get_transfer_action_info(engine_path)
      transfer_ended = self._write_to_file(engine_path, data, file_size, md5_hash)
      if transfer_ended:
        self._database_server.set_transfer_status(engine_path, constants.FILES_ON_CLIENT_AND_CR)
        self.signalTransferEnded(engine_path)
      
    else:
      # Directory case
      (cumulated_size, dir_element_action_info, transfer_type) = self._database_server.get_transfer_action_info(engine_path)
      if not relative_path in dir_element_action_info:
        raise TransferError("write_to_computing_resource_file error: "
                            " the file %s do not belong to the transfer %s" %
                            (relative_path, engine_path))
      (file_size, md5_hash) = dir_element_action_info[relative_path]
      transfer_ended = self._write_to_file(os.path.join(engine_path, relative_path), data, file_size, md5_hash)
      
      if transfer_ended:
        cumulated_file_size, cumulated_transmissions, files_transfer_status = self.transfer_progression_status(engine_path, (cumulated_size, dir_element_action_info, transfer_type))
        if cumulated_transmissions == cumulated_file_size:
          self._database_server.set_transfer_status(engine_path, constants.FILES_ON_CLIENT_AND_CR)
          self.signalTransferEnded(engine_path)
      
    return transfer_ended

  def _write_to_file(self, engine_path, data, file_size, md5_hash = None):
    '''
    @rtype: boolean
    @return: transfer ended
    '''
    file = open(engine_path, 'ab')
    file.write(data)
    fs = file.tell()
    file.close()
    if fs > file_size:
      # Reset file_size   
      open(engine_path, 'wb')
      raise TransferError("write_to_computing_resource_file error: " 
                          "Transmitted data exceed expected file size.")
    elif fs == file_size:
      if md5_hash is not None:
        if hashlib.md5( open( engine_path, 'rb' ).read() ).hexdigest() != md5_hash:
          # Reset file
          open( engine_path, 'wb' )
          raise TransferError("write_to_computing_resource_file error: "  
                              "A transmission error was detected.")
        else:
          return True
      else:
        return True
    else:
      return False



  def read_from_computing_resource_file(self, 
                                        engine_path, 
                                        buffer_size, 
                                        transmitted, 
                                        relative_path = None):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
    if relative_path:
      engine_full_path = os.path.join(engine_path, relative_path)
    else:
      engine_full_path = engine_path
      
    f = open(engine_full_path, 'rb')
    if transmitted:
      f.seek(transmitted)
    data = f.read(buffer_size)
    f.close()
    
    return data

    
  def set_transfer_status(self, engine_path, status):
    '''
    Set a transfer status. 
    '''
    self._database_server.set_transfer_status(engine_path, status)


  def delete_transfer(self, engine_path):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
    if not self._database_server.is_user_transfer(engine_path, self._user_id) :
      #print "Couldn't cancel transfer %s. It doesn't exist or is not owned by the current user \n" % engine_path
      return

    self._database_server.remove_transfer(engine_path)

    
  def signalTransferEnded(self, engine_path):
    '''
    Has to be called each time a file transfer ends for the 
    workflows to be proceeded.
    '''
    workflow_id = self._database_server.get_transfer_information(engine_path)[3]
    if workflow_id != -1:
      self._database_server.add_workflow_ended_transfer(workflow_id, engine_path)
    

  ########## JOB SUBMISSION ##################################################

  
  def submit_job( self, job, queue):
    '''
    Submits a job to the system. 
    
    @type  job: L{soma.workflow.client.Job}
    @param job: job informations 
    '''
    engine_job = self._engine_loop.add_job(job, queue)

    return engine_job


  def delete_job( self, job_id ):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
    if not self._database_server.is_user_job(job_id, self._user_id):
      #print "Couldn't delete job %d. It doesn't exist or is not owned by the current user \n" % job_id
      return 
    
    status = self._database_server.get_job_status(job_id)[0]
    if status == constants.DONE or status == constants.FAILED:
      self._database_server.delete_job(job_id)
    else:
      self._database_server.set_job_status(job_id, constants.DELETE_PENDING)
      if not self._wait_job_status_update(job_id):
        self._database_server.delete_job(job_id)

  ########## WORKFLOW SUBMISSION ############################################
  
  def submit_workflow(self, workflow, expiration_date, name, queue):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
    if not expiration_date:
      expiration_date = datetime.now() + timedelta(days=7)
    
    wf_id = self._engine_loop.add_workflow(workflow, expiration_date, name, queue)

    return wf_id

  
  def delete_workflow(self, workflow_id):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
    if not self._database_server.is_user_workflow(workflow_id, self._user_id):
      #print "Couldn't delete workflow %d. It doesn't exist or is not owned by the current user \n" % job_id
      return
    
    status = self._database_server.get_workflow_status(workflow_id)[0]
    if status == constants.WORKFLOW_DONE:
      self._database_server.delete_workflow(workflow_id)
    else:
      self._database_server.set_workflow_status(workflow_id, 
                                                constants.DELETE_PENDING)
      if not self._wait_wf_status_update(workflow_id):
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
    
    self._database_server.change_workflow_expiration_date(workflow_id, 
                                                          new_expiration_date)
    return True


  def restart_workflow(self, workflow_id):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
    if not self._database_server.is_user_workflow(workflow_id, self._user_id):
      #print "Couldn't restart workflow %d. It doesn't exist or is not owned by the current user \n" % job_id
      return False
    
    (status, last_status_update) = self._database_server.get_workflow_status(workflow_id)
    
    if status != constants.WORKFLOW_DONE and status != constants.WARNING:
      return False

    self._engine_loop.restart_workflow(workflow_id, status)
    return True
    
   
  ########## SERVER STATE MONITORING ########################################


  def jobs(self, job_ids=None):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
    return self._database_server.get_jobs(self._user_id, job_ids)
    

  def transfers(self, transfer_ids=None):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
    return self._database_server.get_transfers(self._user_id, transfer_ids)
  
  
  def workflows(self, workflow_ids=None):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
    return self._database_server.get_workflows(self._user_id, workflow_ids)
  

  def workflow(self, wf_id):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
    if not self._database_server.is_user_workflow(wf_id, self._user_id):
      #print "Couldn't get workflow %d. It doesn't exist or is owned by a different user \n" %wf_id
      return None
    return self._database_server.get_engine_workflow(wf_id)
  
  def job_status(self, job_id):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
    if not self._database_server.is_user_job(job_id, self._user_id):
      #print "Could get the job status of job %d. It doesn't exist or is owned by a different user \n" %job_id
      return
    
    # check the date of the last status update
    status, last_status_update = self._database_server.get_job_status(job_id)
    if status and not status == constants.DONE and \
       not status == constants.FAILED and \
       last_status_update and \
       datetime.now() - last_status_update > timedelta(seconds = refreshment_interval*refreshment_timeout):
      self._database_server.set_job_status(job_id, constants.WARNING)
      return constants.WARNING

    return status
        
  
  def workflow_status(self, wf_id):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
    if not self._database_server.is_user_workflow(wf_id, self._user_id):
      #print "Could get the workflow status of workflow %d. It doesn't exist or is owned by a different user \n" %wf_id
      return
    
    status, last_status_update = self._database_server.get_workflow_status(wf_id)

    if status and \
       not status == constants.WORKFLOW_DONE and \
       last_status_update and \
       datetime.now() - last_status_update > timedelta(seconds = refreshment_interval*refreshment_timeout):
      self._database_server.set_workflow_status(wf_id, constants.WARNING)
      return constants.WARNING

    return status
    
  
  def workflow_elements_status(self, wf_id, groupe = None):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
    if not self._database_server.is_user_workflow(wf_id, self._user_id):
      #print "Couldn't get workflow %d. It doesn't exist or is owned by a different user \n" %wf_id
      return

    status, last_status_update = self._database_server.get_workflow_status(wf_id)
    if status and \
       not status == constants.WORKFLOW_DONE and \
       last_status_update and \
       datetime.now() - last_status_update > timedelta(seconds = refreshment_interval*refreshment_timeout):
      self._database_server.set_workflow_status(wf_id, constants.WARNING)

    wf_status = self._database_server.get_detailed_workflow_status(wf_id)
    return wf_status
        
        
  def transfer_status(self, engine_path):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
    if not self._database_server.is_user_transfer(engine_path, self._user_id):
      #print "Could not get the job status the transfer associated with %s. It doesn't exist or is owned by a different user \n" %engine_path
      return
    transfer_status = self._database_server.get_transfer_status(engine_path)  
    return transfer_status
        

  def transfer_action_info(self,engine_path):
    return self._database_server.get_transfer_action_info(engine_path)

 
  def transfer_progression_status(self, engine_path, transfer_action_info):
    if transfer_action_info[2] == constants.TR_FILE_C_TO_CR:
      (file_size, md5_hash, transfer_type) = transfer_action_info
      transmitted = os.stat(engine_path).st_size
      return (file_size, transmitted)
    elif transfer_action_info[2] == constants.TR_DIR_C_TO_CR or \
         transfer_action_info[2] == constants.TR_MFF_C_TO_CR:
      (cumulated_file_size, dir_element_action_info, transfer_type) = transfer_action_info
      files_transfer_status = []
      for relative_path, (file_size, md5_hash) in dir_element_action_info.iteritems():
        full_path = os.path.join(engine_path, relative_path)
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
    
    job_exit_info= self._database_server.get_job_exit_info(job_id)
    
    return job_exit_info
    

  def stdouterr_transfer_action_info(self, job_id):
    if not self._database_server.is_user_job(job_id, self._user_id):
      return
    
    stdout_file, stderr_file = self._database_server.get_std_out_err_file_path(job_id)
    self.logger.debug("stdout_file " + repr(stdout_file) + " stderr_file " + repr(stderr_file))

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
        raise UnknownObjectError("Could not wait for job %d." %jid)
      
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
          self.logger.debug("wait        job %s status: %s last update %s," 
                            " now %s", 
                            jid, 
                            status, 
                            repr(last_status_update), 
                            repr(datetime.now()))
          delta = datetime.now() - startTime
          if last_status_update and datetime.now() - last_status_update > timedelta(seconds = refreshment_interval*refreshment_timeout):
            raise EngineError("wait_job: Could not wait for job %s. " 
                              "The process updating its status failed." %(jid))
       
 
  def restart_job( self, job_id ):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
    if not self._database_server.is_user_job(job_id, self._user_id):
      raise UnknownObjectError("Could not restart job %d." %job_id)

    (status, last_status_update) = self._database_server.get_job_status(job_id)
    
    if status != constants.FAILED and status != constants.WARNING:
      return False

    self._engine_loop.restart_job(job_id, status)
    return True


  def kill_job( self, job_id ):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''

    if not self._database_server.is_user_job(job_id, self._user_id):
      raise UnknownObjectError("Could not kill job %d" %job_id)
    
    status = self._database_server.get_job_status(job_id)[0]
    if status != constants.DONE and status != constants.FAILED:
      self._database_server.set_job_status(job_id, 
                                          constants.KILL_PENDING)
      
      if not self._wait_job_status_update(job_id):
        self._database_server.set_job_status(job_id, 
                                             constants.WARNING)

  def _wait_job_status_update(self, job_id):
    
    self.logger.debug(">> _wait_job_status_update")
    action_time = datetime.now()
    time.sleep(refreshment_interval)
    (status, 
     last_status_update) = self._database_server.get_job_status(job_id)
    while status and not status == constants.DONE and not status == constants.FAILED and last_status_update < action_time:
      time.sleep(refreshment_interval)
      (status, last_status_update) = self._database_server.get_job_status(job_id) 
      if last_status_update and datetime.now() - last_status_update > timedelta(seconds = refreshment_interval*refreshment_timeout):
        self.logger.debug("<< _wait_job_status_update: could not wait for job status update of %s. "
                          "The process updating its status failed." %(job_id))
        return False
    self.logger.debug("<< _wait_job_status_update")
    return True

  def _wait_wf_status_update(self, wf_id):  
    self.logger.debug(">> _wait_wf_status_update")
    action_time = datetime.now()
    time.sleep(refreshment_interval)
    (status, 
     last_status_update) = self._database_server.get_workflow_status(wf_id)
    while status and not status == constants.WORKFLOW_DONE and \
          last_status_update < action_time:
      time.sleep(refreshment_interval)
      (status, 
       last_status_update) = self._database_server.get_workflow_status(wf_id) 
      if last_status_update and \
         datetime.now() - last_status_update > timedelta(seconds=refreshment_interval*refreshment_timeout):
        self.logger.debug("<< _wait_wf_status_update")
        return False
    self.logger.debug("<< _wait_wf_status_update")
    return True
