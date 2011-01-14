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

import soma.workflow.constants as constants
from soma.workflow.client import Job, FileTransfer, FileSending, FileRetrieving, Workflow, SharedResourcePath, WorkflowNodeGroup
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


class EngineLoopThread(threading.Thread):
  def __init__(self, engine_loop):
    super(EngineLoopThread, self).__init__()
    self.engine_loop = engine_loop
    self.time_interval = refreshment_interval
  
  def run(self):
    #cProfile.runctx("self.engine_loop.start_loop(self.time_interval)", globals(), locals(), "/home/sl225510/profile")
    self.engine_loop.start_loop(self.time_interval)

  def stop(self):
    self.engine_loop.stop_loop()
    self.join()
  
  
class Drmaa(object):
  '''time_interval
  Manipulation of the Drmaa session. 
  Contains possible patch depending on DRMAA impementation. 
  '''

  # DRMAA session. DrmaaJobs
  _drmaa = None
  # string
  _drmaa_implementation = None
  # DRMAA doesn't provide an unified way of submitting
  # parallel jobs. The value of parallel_job_submission is cluster dependant. 
  # The keys are:
  #  -Drmaa job template attributes 
  #  -parallel configuration name as defined in soma.jobs.constants
  # dict
  _parallel_job_submission_info = None
  
  logger = None

  def __init__(self, drmaa_implementation, parallel_job_submission_info):

    self.logger = self.logger = logging.getLogger('ljp.drmaajs')

    self._drmaa = DrmaaJobs()
    self._drmaa_implementation = drmaa_implementation

    self._parallel_job_submission_info = parallel_job_submission_info

    self.logger.debug("Parallel job submission info: %s", 
                      repr(parallel_job_submission_info))

    
    # patch for the PBS-torque DRMAA implementation
    if self._drmaa_implementation == "PBS":
      jobTemplateId = self._drmaa.allocateJobTemplate()
      self._drmaa.setCommand(jobTemplateId, "echo", [])
      self._drmaa.setAttribute(jobTemplateId, 
                               "drmaa_output_path", 
                               "[void]:/tmp/soma-workflow-empty-job.o")
      self._drmaa.setAttribute(jobTemplateId, 
                               "drmaa_error_path", 
                               "[void]:/tmp/soma-workflow-empty-job.e")
      self._drmaa.runJob(jobTemplateId)
      ################################
    

  def job_submission(self, job):
    '''
    @type  job: soma.workflow.client.Job
    @param job: job to be submitted
    @rtype: string
    @return: drmaa job id 
    '''

    drmaaJobId = self._drmaa.allocateJobTemplate()

    # patch for the PBS-torque DRMAA implementation
    command = []
    if self._drmaa_implementation == "PBS":
      for command_el in job.command:
        command_el = command_el.replace('"', '\\\"')
        command.append("\"" + command_el + "\"")
      self.logger.debug("PBS case, new command:" + repr(command))
    else:
      command = job.command
    self.logger.debug("command: " + repr(command))
          
    self._drmaa.setCommand(drmaaJobId, command[0], command[1:])
  
    self._drmaa.setAttribute(drmaaJobId, 
                             "drmaa_output_path", 
                             "[void]:" + job.stdout_file)
    
    if job.join_stderrout:
      self._drmaa.setAttribute(drmaaJobId,
                               "drmaa_join_files", 
                               "y")
    else:
      if job.stderr_file:
        self._drmaa.setAttribute(drmaaJobId, 
                                 "drmaa_error_path", 
                                 "[void]:" + job.stderr_file)
    
    if job.stdin:
      self.logger.debug("stdin: " + job.stdin)
      self._drmaa.setAttribute(drmaaJobId, 
                               "drmaa_input_path", 
                               "[void]:" + job.stdin)
      
    if job.working_directory:
      self._drmaa.setAttribute(drmaaJobId, "drmaa_wd", job.working_directory)

    if job.queue:
      self._drmaa.setAttribute(drmaaJobId, "drmaa_native_specification", "-q " + str(job.queue))
    
    if job.parallel_job_info :
      parallel_config_name, max_node_number = job.parallel_job_info
      self._setDrmaaParallelJob(drmaaJobId, 
                                parallel_config_name, 
                                max_node_number)
      
    job_env = []
    for var_name in os.environ.keys():
      job_env.append(var_name+"="+os.environ[var_name])
    
    self._drmaa.setVectorAttribute(drmaaJobId, 'drmaa_v_env', job_env)

    drmaaSubmittedJobId = self._drmaa.runJob(drmaaJobId)
    self._drmaa.deleteJobTemplate(drmaaJobId)
    
    if drmaaSubmittedJobId == "":
      self.logger.error("Could not submit job: Drmaa problem.");
      return -1

    return drmaaSubmittedJobId
    

  def get_job_status(self, drmaa_job_id):
    status = self._drmaa.jobStatus(drmaa_job_id)
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
    configuration_name: type of parallel job as defined in soma.jobs.constants 
    (eg MPI, OpenMP...)
    max_node_num: maximum node number the job requests (on a unique machine or 
    separated machine depending on the parallel configuration)
    ''' 

    self.logger.debug(">> _setDrmaaParallelJob")
    if not self._parallel_job_submission_info:
      raise WorkflowEngineError("Configuration file : Couldn't find" 
                                "parallel job submission information.", 
                                 self.logger)
    
    if configuration_name not in self._parallel_job_submission_info:
      raise WorkflowEngineError("Configuration file : couldn't find" 
                                "the parallel configuration %s." %
                                (configuration_name), self.logger)

    cluster_specific_cfg_name = self._parallel_job_submission_info[configuration_name]
    
    for drmaa_attribute in constants.PARALLEL_DRMAA_ATTRIBUTES:
      value = self._parallel_job_submission_info.get(drmaa_attribute)
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
      value = self._parallel_job_submission_info.get(parallel_env_v)
      if value: job_env.append(parallel_env_v+'='+value.rstrip())
    
    self._drmaa.setVectorAttribute(drmaa_job_template_id, 'drmaa_v_env', job_env)
    self.logger.debug("Parallel job environment : " + repr(job_env))
        
    self.logger.debug("<< _setDrmaaParallelJob")


  def kill_job(self, job_drmaa_id):
    self._drmaa.terminate(job_drmaa_id)



class EngineSendTransfer(FileSending):

  engine_path = None

  status = None

  def __init__(self, client_file_sending):
    client_ft_copy = copy.deepcopy(client_file_sending)
    super(EngineSendTransfer, self).__init__(client_ft_copy.client_path,
                                             client_ft_copy.disposal_timeout,
                                             client_ft_copy.name,
                                             client_ft_copy.client_paths)

  def register(self, 
               database_server, 
               user_id, 
               exp_date=None,
               wf_id=-1):
    
    if exp_date == None:
      exp_date = datetime.now() + timedelta(hours=self.disposal_timeout) 

    if self.client_paths:
      self.engine_path = database_server.generate_file_path(user_id)
      os.mkdir(self.engine_path)
    else:
      self.engine_path = database_server.generate_file_path(user_id, 
                                                               self.client_path)
    database_server.add_transfer( self.engine_path, 
                                  self.client_path, 
                                  exp_date, 
                                  user_id,
                                  constants.READY_TO_TRANSFER, 
                                  wf_id, 
                                  self.client_paths)

    self.status = constants.READY_TO_TRANSFER

    return self.engine_path


  def files_exist_on_server(self):
    exist = self.status == constants.TRANSFERED
    return exist
  


class EngineRetrieveTransfer(FileRetrieving):

  engine_path = None

  status = None

  def __init__(self, client_file_retrieving):
    client_ft_copy = copy.deepcopy(client_file_retrieving)
    super(EngineRetrieveTransfer, self).__init__(client_ft_copy.client_path,
                                             client_ft_copy.disposal_timeout,
                                             client_ft_copy.name,
                                             client_ft_copy.client_paths)
    
  def register(self, 
               database_server, 
               user_id, 
               exp_date=None, 
               wf_id=-1):

    if exp_date == None:
      exp_date = datetime.now() + timedelta(hours=self.disposal_timeout) 

    if self.client_paths:
      self.engine_path = database_server.generate_file_path(user_id)
      os.mkdir(self.engine_path)
    else:
      self.engine_path = database_server.generate_file_path(user_id, 
                                                               self.client_path)
    database_server.add_transfer( self.engine_path, 
                                  self.client_path, 
                                  exp_date, 
                                  user_id, 
                                  constants.TRANSFER_NOT_READY, 
                                  wf_id, 
                                  self.client_paths)

    self.status = constants.TRANSFER_NOT_READY
    
    return self.engine_path


  def files_exist_on_server(self):
    exist = self.status == constants.TRANSFERED or \
            self.status == constants.READY_TO_TRANSFER
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
  exit_status = None

  str_rusage = None
  # contain a representation of the signal if the status is FINISHED_TERM_SIG.
  # string or None
  terminating_signal = None

  logger = None

  def __init__(self, client_job, queue, workflow_id=-1):
    
    if workflow_id == -1:
      client_job_copy = copy.deepcopy(client_job)
    else:
      client_job_copy = client_job
    super(EngineJob, self).__init__(client_job_copy.command,
                                    client_job_copy.referenced_input_files,
                                    client_job_copy.referenced_output_files ,
                                    client_job_copy.stdin,
                                    client_job_copy.join_stderrout,
                                    client_job_copy.disposal_timeout,
                                    client_job_copy.name,
                                    client_job_copy.stdout_file,
                                    client_job_copy.stderr_file ,
                                    client_job_copy.working_directory ,
                                    client_job_copy.parallel_job_info)
    
    self.drmaa_id = None
    self.status = constants.NOT_SUBMITTED
    self.exit_status = None
    self.exit_value = None
    self.terminating_signal = None

    self.workflow_id = workflow_id
    self.queue = queue

  def convert_command(self, ft_conv):
    '''
    @type ft_conv: dictionary FileSending => EngineSendTransfer 
                              FileRetrieving => EngineRetrieveTransfer
    '''
    logger = logging.getLogger('engine.EngineJob')
    new_command = []
    logger.debug("job command " + repr(self.command))
    for command_el in self.command:
      if isinstance(command_el, list):
        new_list = []
        for list_el in command_el:
          if isinstance(list_el, tuple) and \
            ( isinstance(list_el[0], EngineSendTransfer) or \
              isinstance(list_el[0], EngineRetrieveTransfer)):
            new_list.append(os.path.join(list_el[0].engine_path,
                                         list_el[1])) 
          elif isinstance(list_el, tuple) and \
            isinstance(list_el[0], FileTransfer):
            new_list.append(os.path.join(ft_conv[list_el[0]].engine_path,
                                         list_el[1])) 
          elif isinstance(list_el, EngineSendTransfer) or \
               isinstance(list_el, EngineRetrieveTransfer):
            new_list.append(list_el.engine_path)
          elif isinstance(list_el, FileTransfer):
            new_list.append(ft_conv[list_el].engine_path)
          elif type(list_el) in types.StringTypes: 
            new_list.append(list_el)
          else:
            logger.debug("!!!!!!!!!!!!! " + repr(list_el) + " doesn't have an appropriate type !!!")
            ##TBI raise an exception and unregister everything on the server
            ## or do this type checking before any registration on the server ?
            pass
        new_command.append(str(repr(new_list)))
      elif isinstance(command_el, tuple) and \
          ( isinstance(command_el[0], EngineSendTransfer) or \
            isinstance(command_el[0], EngineRetrieveTransfer) ):
        new_command.append(os.path.join(command_el[0].engine_path, 
                                        command_el[1]))
      elif isinstance(command_el, tuple) and \
           isinstance(command_el[0], FileTransfer):
        new_command.append(os.path.join(ft_conv[command_el[0]].engine_path,
                                        command_el[1]))
      elif isinstance(command_el, EngineSendTransfer) or \
           isinstance(command_el, EngineRetrieveTransfer):
        new_command.append(command_el.engine_path)
      elif isinstance(command_el, FileTransfer):
        new_command.append(ft_conv[command_el].engine_path)
      elif type(command_el) in types.StringTypes: 
        new_command.append(command_el)
      else:
        logger.debug("!!!!!!!!!!!!! " + repr(command_el) + " doesn't have an appropriate type !!!")
        ##TBI raise an exception and unregister everything on the server
        ## or do this type checking before any registration on the server ?
        pass
      
    logger.debug("new_command " + repr(new_command))
    self.command = new_command
  

  def convert_stdin(self, ft_conv):
    if self.stdin:
      if isinstance(self.stdin, EngineSendTransfer) or \
         isinstance(self.stdin, EngineRetrieveTransfer) :
        self.stdin = self.stdin.engine_path 
      elif isinstance(self.stdin, FileTransfer):
        self.stdin = ft_conv[self.stdin].engine_path 

  def convert_referenced_transfers(self, ft_conv):
    # referenced_input_files => replace the FileTransfer objects by the corresponding engine_path
    new_referenced_input_files = []
    for input_file in self.referenced_input_files:
      if isinstance(input_file, EngineSendTransfer) or \
         isinstance(input_file, EngineRetrieveTransfer) :
        #new_referenced_input_files.append(input_file.engine_path)
        new_referenced_input_files.append(input_file)
      elif isinstance(input_file, FileTransfer):
        #new_referenced_input_files.append(ft_conv[input_file].engine_path)
        new_referenced_input_files.append(ft_conv[input_file])
      else: 
        #ift_node = self._assert_is_a_workflow_node(input_file)
        ## TBI error management !!! => unregister the workflow !!
        new_referenced_input_files.append(input_file)
    self.referenced_input_files= new_referenced_input_files

    # referenced_input_files => replace the FileTransfer objects by the corresponding engine_path
    new_referenced_output_files = []
    for output_file in self.referenced_output_files:
      if isinstance(output_file, EngineSendTransfer) or \
         isinstance(output_file, EngineRetrieveTransfer):
        #new_referenced_output_files.append(output_file.engine_path)
        new_referenced_output_files.append(output_file)
      elif isinstance(output_file, FileTransfer):
        #new_referenced_output_files.append(ft_conv[output_file].engine_path)
        new_referenced_output_files.append(ft_conv[output_file])
      else:
        #oft_node = self._assert_is_a_workflow_node(output_file) 
        ## TBI error management !!! => unregister the workflow !!
        new_referenced_output_files.append(output_file)
    self.referenced_output_files = new_referenced_output_files
  

  def register(self, database_server, user_id, exp_date = None):
    '''
    Register the job to the database server.

    Finds out the value of job expiration date, stdout, stderr, 
    custom_submission and command_info.
    Creates the stdout and stderr files.

    @rtype: int
    @return: job identifier on the database server
    '''
    self._user_id = user_id
    logger = logging.getLogger('engine.EngineJob')

    if exp_date == None:
      exp_date = datetime.now() + timedelta(hours=self.disposal_timeout) 
    parallel_config_name = None
    max_node_number = 1

    if not self.stdout_file:
      self.stdout_file = database_server.generate_file_path(self._user_id)
      self.stderr_file = database_server.generate_file_path(self._user_id)
      custom_submission = False #the std out and err file has to be removed with the job
    else:
      custom_submission = True #the std out and err file won't to be removed with the job
      self.stdout_file = self.stdout_file
      self.stderr_file = self.stderr_file
      
    if self.parallel_job_info:
      parallel_config_name, max_node_number = self.parallel_job_info   
       
    command_info = ""
    for command_element in self.command:
      command_info = command_info + " " + repr(command_element)
      
    db_job = soma.workflow.database_server.DBJob(
                          user_id = self._user_id, 
                          custom_submission = custom_submission,
                          expiration_date = exp_date, 
                          command = command_info,
                          workflow_id = self.workflow_id,
                          queue = self.queue,
                          
                          stdin_file = self.stdin, 
                          join_errout = self.join_stderrout,
                          stdout_file = self.stdout_file,
                          stderr_file = self.stderr_file,
                          working_directory = self.working_directory,
                          
                          parallel_config_name = parallel_config_name,
                          max_node_number = max_node_number,
                          name = self.name)
                                       
    job_id = database_server.add_job(db_job)
                                        
    # create standard output files 
    try:  
      tmp = open(self.stdout_file, 'w')
      tmp.close()
    except IOError, e:
      raise WorkflowEngineError("Could not create the standard output file " 
                                "%s: %s \n"  %(type(e), e), logger)
    if self.stderr_file:
      try:
        tmp = open(self.stderr_file, 'w')
        tmp.close()
      except IOError, e:
        raise WorkflowEngineError("Could not create the standard error file "
                                  "%s: %s \n"  %(type(e), e), logger)
      
    
    if self.referenced_input_files:
      str_referenced_input_files = []
      for ft in self.referenced_input_files:
        if isinstance(ft, FileTransfer):
          engine_path = ft.engine_path
        else:
          engine_path = ft
        str_referenced_input_files.append(engine_path)
      database_server.register_inputs(job_id, str_referenced_input_files)
    if self.referenced_output_files:
      str_referenced_ouput_files = []
      for ft in self.referenced_output_files:
        if isinstance(ft, FileTransfer):
          engine_path = ft.engine_path
        else:
          engine_path = ft
        str_referenced_ouput_files.append(engine_path)
      database_server.register_outputs(job_id, str_referenced_ouput_files)

    
    self.job_id = job_id
    return job_id

  def is_running(self):
    running = self.status != constants.NOT_SUBMITTED and \
              self.status != constants.FAILED and \
              self.status != constants.DONE 
    return running

  def is_done(self):
    done = self.status == constants.DONE or self.status == constants.FAILED
    return done

  def failed(self):
    failed = self.is_done() and \
              ((self.exit_value != 0 and self.exit_value != None) or \
              self.exit_status != constants.FINISHED_REGULARLY or \
              self.terminating_signal != None)
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
  exp_date = None
  # name of the queue to be used to submit jobs, str
  queue = None
  # dict job_id => EngineJob
  jobs = None
  # dict tr_id => EngineSendTransfer or EngineRetrieveTransfer
  transfers = None

  #logger = None
  
  def __init__(self, client_workflow, path_translation, queue):
 
    client_workflow_copy = copy.deepcopy(client_workflow)
    super(EngineWorkflow, self).__init__(client_workflow_copy.nodes,
                                         client_workflow_copy.dependencies,
                                         client_workflow_copy.mainGroup,
                                         client_workflow_copy.groups)

    self.status = constants.WORKFLOW_NOT_STARTED
    self._path_translation = path_translation
    self.queue = queue


  def register(self, database_server, user_id, expiration_date, name):
    '''
    Register the workflow to the database_server.
    Register also the associated jobs and transfers.

    @rtype: int
    @return: workflow identifier on the database server
    '''

    self._user_id = user_id

    self._type_checking() # throw possible exceptions !!!
    self._shared_path_translation() # throw possible exceptions !!!
    
    #self.exp_date = datetime.now() + timedelta(hours=self.disposal_timeout) 
    self.exp_date = expiration_date
    self.name = name
    wf_id = database_server.add_workflow(self._user_id, 
                                         self.exp_date, 
                                         self.name)
    self.wf_id = wf_id

    self._register_jobs(database_server) # throw possible exceptions !!!
  
    database_server.set_workflow(self.wf_id, self, self._user_id)
    return wf_id

    
  def _register_jobs(self, database_server):
    '''
    Register the transfers and jobs.
    Transform every Job to and EngineJob
    and FileTransfer to EngineSendTransfer or EngineRetrieveTransfer
    '''
    logger = logging.getLogger('engine.EngineWorkflow') 

    # recover all the file transfers
    file_transfers = set([])
    for job in self.nodes:
      for command_el in job.command:
        if isinstance(command_el, list):
          for list_el in command_el:
            if isinstance(list_el, tuple) and \
              isinstance(list_el[0], FileTransfer):
              file_transfers.add(list_el[0])
            elif isinstance(list_el, FileTransfer):
              file_transfers.add(list_el)
        elif isinstance(command_el, tuple) and isinstance(command_el[0], FileTransfer):
          file_transfers.add(command_el[0])
        elif isinstance(command_el, FileTransfer):
          file_transfers.add(command_el)
      for ft in job.referenced_input_files:
        if isinstance(ft, FileTransfer):  
          file_transfers.add(ft)
      for ft in job.referenced_output_files:
        if isinstance(ft, FileTransfer): 
          file_transfers.add(ft)

    # First loop to register FileTransfer to the database server.
    ft_conv = {} # dictonary FileTransfer => EngineRetrieveTransfer or  EngineSendTransfer
    self.transfers =  {}
    
    for ft in file_transfers:
      if isinstance(ft, FileSending):
        engine_transfer = EngineSendTransfer(ft)
        engine_transfer.register(database_server, 
                                 self._user_id,
                                 self.exp_date,
                                 self.wf_id)
        self.transfers[engine_transfer.engine_path] = engine_transfer
        ft_conv[ft] = engine_transfer
        
      elif isinstance(ft, FileRetrieving):
        engine_transfer = EngineRetrieveTransfer(ft)
        engine_transfer.register(database_server, 
                                 self._user_id,
                                 self.exp_date,
                                 self.wf_id)
        self.transfers[engine_transfer.engine_path] = engine_transfer
        ft_conv[ft] = engine_transfer


    # Second loop to convert Job attributs and 
    # register it to the database server. 
    job_conv = {}  # Job => EngineJob
    self.jobs =  {}
    for job in self.nodes:
      engine_job = EngineJob(client_job=job,
                             queue=self.queue,
                             workflow_id=self.wf_id)
      engine_job.convert_command(ft_conv)
      engine_job.convert_stdin(ft_conv)
      engine_job.convert_referenced_transfers(ft_conv)
      engine_job.register(database_server, 
                          self._user_id, 
                          self.exp_date)
      job_conv[job] = engine_job
      self.jobs[engine_job.job_id] = engine_job

    
    # End converting the nodes to EngineNodes
    new_dependencies = []
    for dep in self.dependencies:
      new_dependencies.append((job_conv[dep[0]], job_conv[dep[1]]))
    self.dependencies = new_dependencies

    new_nodes = []
    for job in self.nodes:
      new_nodes.append(job_conv[job])
    self.nodes = new_nodes

    group_conv = {}
    new_groups = []
    for group in self.groups:
      (new_group,
       group_conv) = self._convert_workflow_node_group(group, job_conv, group_conv)
      new_groups.append(new_group)
    self.groups = new_groups

    if self.mainGroup:
      (new_main_group,
       group_conv) = self._convert_workflow_node_group(self.mainGroup, job_conv, group_conv)
      self.mainGroup = new_main_group

    #for g, gc in group_conv.iteritems():
      #print repr(g) + " ===> " + repr(gc)

    #for group in self.groups:
      #print " "
      #print "group " + repr(group) + " " + repr(group.name) + "  contains:"
      #for element in group.elements:
        #print "     element: " + repr(element.name) + " " + repr(element)

    #print " "
    #print "main_group " + repr(self.mainGroup) + " contains:"
    #for element in self.mainGroup.elements:
        #print "     element: " + repr(element.name) + " " + repr(element)

  
  def _convert_workflow_node_group(self, group, job_conv, group_conv):
    if group in group_conv:
      return (group_conv[group], group_conv)

    new_elements = []
    for el in group.elements:
      if isinstance(el, WorkflowNodeGroup):
        (new_el, group_conv) = self._convert_workflow_node_group(el, job_conv, group_conv)
        new_elements.append(new_el)
      elif isinstance(el, EngineJob):
        new_elements.append(el)
      elif isinstance(el, Job):
        new_elements.append(job_conv[el])
      else:
        logger.debug("!!!! Wrong group element type: " + repr(el))
        # TBI raise ???

    new_group = WorkflowNodeGroup(new_elements, group.name)
    group_conv[group] = new_group
    return (new_group, group_conv)

  def _type_checking(self):
    # TBI
    pass

  def _shared_path_translation(self):

    for job in self.nodes:
      new_command = []
      for command_el in job.command:
        if isinstance(command_el, SharedResourcePath):
          command_el.engine_path = self._translate(command_el, 
                                                  self._path_translation)
          new_command.append(command_el.engine_path)
        elif isinstance(command_el, list):
          new_list = []
          for list_el in command_el:
            if isinstance(list_el, SharedResourcePath):
              list_el.engine_path =  self._translate(list_el, 
                                                    self._path_translation)
              new_list.append(list_el.engine_path)
            else:
              new_list.append(list_el)
          new_command.append(new_list)
        else:
          new_command.append(command_el)
        job.command = new_command
      if job.stdout_file and \
          isinstance(job.stdout_file, SharedResourcePath):
          job.stdout_file = self._translate(job.stdout_file, 
                                              self._path_translation)
      if job.stderr_file and \
          isinstance(job.stderr_file, SharedResourcePath):
          job.stderr_file = self._translate(job.stderr_file, 
                                              self._path_translation)
      if job.working_directory and \
          isinstance(job.working_directory, SharedResourcePath):
            job.working_directory = self._translate(job.working_directory, 
                                                    self._path_translation)
      if job.stdin and \
          isinstance(job.stdin, SharedResourcePath):
            job.stdin = self._translate(job.stdin, self._path_translation)
    

  def find_out_independant_jobs(self):
    independant_jobs = []
    for job in self.nodes:
      to_run=True
      for ft in job.referenced_input_files:
        if not ft.files_exist_on_server():
          to_run = False
          break 
      if to_run:
        for dep in self.dependencies:
          if dep[1] == job:
            to_run = False
            break
      if to_run:
        independant_jobs.append(job)
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
    for job in self.nodes:
      if job.is_done(): 
        done.append(job)
      elif job.is_running(): 
        running.append(job)
      logger.debug("job " + repr(job.name) + " " + repr(job.status) + " r " + repr(job.is_running()) + " d " + repr(job.is_done()))
      if job.status == constants.NOT_SUBMITTED:
        # a job is run when all its dependencies succeed and all its 
        # referenced_input_files are in the TRANSFERED or READY_TO_TRANSFER state
        job_to_run = True 
        for ft in job.referenced_input_files:
          if not ft.files_exist_on_server():
            job_to_run = False
            break
        for dep in self.dependencies:
          if dep[1] == job and not dep[0].ended_with_success(): 
            job_to_run = False
            if dep[0].failed():
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
        if dep[0] in to_abort and not dep[1] in to_abort:
          to_abort.add(dep[1])
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
    elif len(done) + len(to_abort) == len(self.nodes): 
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
      self.jobs[job_id].status = status
      exit_status, exit_value, term_signal, resource_usage = exit_info
      self.jobs[job_id].exit_status = exit_status
      self.jobs[job_id].exit_value = exit_value
      self.jobs[job_id].str_rusage = resource_usage
      self.jobs[job_id].terminating_signal = term_signal
   
    for ft_info in wf_status[1]:
      engine_path, client_path, status, transfer_action_info = ft_info 
      self.transfers[engine_path].status = status

    done = True
    for job in self.nodes:
      if job.failed():
        #clear all the information related to the previous job submission
        job.status = constants.NOT_SUBMITTED
        job.exit_status = None
        job.exit_value = None
        job.terminating_signal = None
        job.drmaa_id = None
        database_server.set_submission_information(job.job_id, None, None)
        database_server.set_job_status(job.job_id, constants.NOT_SUBMITTED)
      if not job.ended_with_success():
        undone_jobs.append(job)
       
    to_run = []
    if undone_jobs:
      # look for nodes to run
      for job in undone_jobs:
        job_to_run = True # a node is run when all its dependencies succeed
        for ft in job.referenced_input_files:
          if not ft.files_exist_on_server():
            job_to_run = False
            break 
        if job_to_run:
          for dep in self.dependencies:
            if dep[1] == job and not dep[0].ended_with_success():
              job_to_run = False
              break

        if job_to_run: 
          to_run.append(job)
  
    if to_run:
      status = constants.WORKFLOW_IN_PROGRESS
    else:
      status = constants.WORKFLOW_DONE
     
    return (to_run, status)



  @staticmethod
  def _translate(urp, path_translation):
    '''
    @type urp: L{SharedResourcePath}
    @rtype: string
    @returns: path in the engine environment
    '''
    logger = logging.getLogger('engine.EngineWorkflow')
    if not path_translation:
      raise WorkflowEngineError("Configuration file: Couldn't find path" 
                                "translation files.", logger)
    if not urp.namespace in path_translation.keys():
      raise WorkflowEngineError("Path translation: the namespace %s " 
                                "does'nt exist" %
                                (urp.namespace), logger)
    if not urp.uuid in path_translation[urp.namespace].keys():
      raise WorkflowEngineError("Path translation: the uuid %s " 
                                "does'nt exist for the namespace %s." %
                                (urp.uuid, urp.namespace), logger)
    
    engine_path = os.path.join(path_translation[urp.namespace][urp.uuid],
                              urp.relative_path)
    return engine_path

    
    
 


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
          wf_jobs.update(wf.jobs)
          wf_transfers.update(wf.transfers)
        
        ended_jobs = {}
        for job in itertools.chain(self._jobs.itervalues(), wf_jobs.itervalues()):
          if job.exit_status == None and job.drmaa_id != None:
            job.status = self._engine_drmaa.get_job_status(job.drmaa_id)
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
                  if isinstance(ft, FileTransfer):
                    engine_path = ft.engine_path
                  else:
                    engine_path = ft
                  self._database_server.set_transfer_status(engine_path, 
                                                  constants.READY_TO_TRANSFER)
                     
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
    engine_job = EngineJob(client_job, queue)
    job_id = engine_job.register(self._database_server, self._user_id)
    # submit
    self._pend_for_submission(engine_job)
    # add to the engine managed job list
    with self._lock:
      self._jobs[job_id] = engine_job

    return job_id

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
                                     queue)
    wf_id = engine_workflow.register(self._database_server, 
                                     self._user_id, 
                                     expiration_date, 
                                     name)
    # submit independant jobs
    (jobs_to_run, 
     engine_workflow.status) = engine_workflow.find_out_independant_jobs()
    for job in jobs_to_run:
      self._pend_for_submission(job)
    # add to the engine managed workflow list
    with self._lock:
      self._workflows[wf_id] = engine_workflow

    return wf_id

  def _stop_job(self, job_id, job):
    if job.status == constants.DONE or job.status == constants.FAILED:
      self._database_server.set_job_status( job_id, 
                                            job.status, 
                                            force = True)
    else:
      if job.drmaa_id:
        self.logger.debug("Kill job " + repr(job_id) + " drmaa id: " + repr(job.drmaa_id))
        self._engine_drmaa.kill_job(job.drmaa_id)
      elif job in self._pending_queues[job.queue]:
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
    self.logger.debug("wf.jobs " + repr(wf.jobs))
    for job_id, job in wf.jobs.iteritems():
      self._stop_job(job_id, job)
    self._database_server.set_workflow_status(wf_id, 
                                              constants.WORKFLOW_DONE, 
                                              force = True)


  def restart_workflow(self, wf_id):
    workflow = self._database_server.get_workflow(wf_id)
    workflow.status = self._database_server.get_workflow_status(workflow.wf_id)[0]
    (jobs_to_run, workflow.status) = workflow.restart(self._database_server)
    for job in jobs_to_run:
      self._pend_for_submission(job)
    # add to the engine managed workflow list
    with self._lock:
      self._workflows[wf_id] = workflow

  def restart_job(self, job_id):
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
      raise WorkflowEngineError("Couldn't identify user %s: %s \n" %(type(e), e), self.logger)
    
    self._user_id = self._database_server.register_user(user_login)
    self.logger.debug("user_id : " + repr(self._user_id))

  def __del__( self ):
    pass

  ########## FILE TRANSFER ###############################################
  
  '''
  For the following methods:
    Local means that it is located on a directory shared by the machines 
    of the computing resource.
    Remote means that it is located on a client machine or on any directory 
    owned by the user. 
    A transfer will associate client file path to unique engine file path.
  
  Use L{register_transfer} then L{writeLine} or scp or 
  shutil.copy to transfer input file from the client to the engine 
  environment.
  Use L{register_transfer} and once the job has run use L{readline} or scp or
  shutil.copy to transfer the output file from the engine to the client 
  environment.
  '''

  def register_transfer(self, 
                        client_path, 
                        disposal_timeout=168, 
                        client_paths = None): 
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
    if client_paths:
      engine_path = self._database_server.generate_file_path(self._user_id)
      os.mkdir(engine_path)
    else:
      engine_path = self._database_server.generate_file_path(self._user_id, client_path)
    expirationDate = datetime.now() + timedelta(hours=disposal_timeout) 
    self._database_server.add_transfer(engine_path, client_path, expirationDate, self._user_id, -1, client_paths)
    return engine_path
  

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


  def initializeRetrivingTransfer(self, engine_path):
    engine_path, client_path, expiration_date, workflow_id, client_paths = self.transfer_information(engine_path)
    status = self.transfer_status(engine_path)
    if status != constants.READY_TO_TRANSFER:
      self.logger.debug("!!!! transfer " + engine_path + "is not READY_TO_TRANSFER")
      # TBI raise
      return (None, None)
    contents = None
    transfer_action_info = None
    if not client_paths:
      if os.path.isfile(engine_path):
        stat = os.stat(engine_path)
        file_size = stat.st_size
        md5_hash = hashlib.md5( open( engine_path, 'rb' ).read() ).hexdigest() 
        transfer_action_info = (file_size, md5_hash, constants.FILE_RETRIEVING)
      elif os.path.isdir(engine_path):
        contents = WorkflowEngine._contents([engine_path])
        (cumulated_file_size, dir_element_action_info) = self._initializeDirectory(engine_path, contents)
        transfer_action_info = (cumulated_file_size, dir_element_action_info, constants.DIR_RETRIEVING)
    else: #client_paths
      full_path_list = []
      for element in os.listdir(engine_path):
        full_path_list.append(os.path.join(engine_path, element))
      contents = WorkflowEngine._contents(full_path_list)
      (cumulated_file_size, dir_element_action_info) = self._initializeDirectory(engine_path, contents)
      transfer_action_info = (cumulated_file_size, dir_element_action_info, constants.DIR_RETRIEVING)

    self._database_server.set_transfer_action_info(engine_path, transfer_action_info)     
    return (transfer_action_info, contents)


  def initializeFileSending(self, engine_path, file_size, md5_hash = None):
    '''
    Initialize the transfer of a file.
    '''
    transfer_action_info = (file_size, md5_hash, constants.FILE_SENDING)
    f = open(engine_path, 'w')
    f.close()
    self._database_server.set_transfer_status(engine_path, constants.TRANSFERING)
    self._database_server.set_transfer_action_info(engine_path, transfer_action_info)
    return transfer_action_info


  def _initializeDirectory(self, engine_path, contents, subdirectory = ""):
    '''
    Initialize engine directory from the contents of client directory.

    @rtype : tuple (int, dictionary)
    @return : (cumulated file size, dictionary : relative file path => (file_size, md5_hash))
    '''
    dir_element_action_info = {}
    cumulated_file_size = 0
    for item, description, md5_hash in contents:
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


  def _createDirStructure(self, engine_path, contents, subdirectory = ""):
    if not os.path.isdir(engine_path):
      os.makedirs(engine_path)
    
    for item, description, md5_hash in contents:
      relative_path = os.path.join(subdirectory,item)
      full_path = os.path.join(engine_path, relative_path)
      if isinstance(description, list):
        if not os.path.isdir(full_path):
          os.mkdir(full_path)
        self._createDirStructure( engine_path, description, relative_path)
    

  def initializeDirSending(self, engine_path, contents):
    '''
    Initialize the transfer of a directory.
    '''
    cumulated_file_size, dir_element_action_info = self._initializeDirectory(engine_path, contents)
    transfer_action_info = (cumulated_file_size, dir_element_action_info, constants.DIR_SENDING)
    self._createDirStructure(engine_path, contents)
    self._database_server.set_transfer_status(engine_path, constants.TRANSFERING)
    self._database_server.set_transfer_action_info(engine_path, transfer_action_info)
    return transfer_action_info
    
  
  def _sendToFile(self, engine_path, data, file_size, md5_hash = None):
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
      raise WorkflowEngineError('send_piece: Transmitted data exceed expected file size.')
    elif fs == file_size:
      if md5_hash is not None:
        if hashlib.md5( open( engine_path, 'rb' ).read() ).hexdigest() != md5_hash:
          # Reset file
          open( engine_path, 'wb' )
          raise WorkflowEngineError('send_piece: Transmission error detected.')
        else:
          return True
      else:
        return True
    else:
      return False


  def send_piece(self, engine_path, data, relative_path=None):
    '''
    @rtype : boolean
    @return : transfer ended
    '''
    if not relative_path:
      # File case
      (file_size, md5_hash, transfer_type) = self._database_server.get_transfer_action_info(engine_path)
      transfer_ended = self._sendToFile(engine_path, data, file_size, md5_hash)
      if transfer_ended:
        self._database_server.set_transfer_status(engine_path, constants.TRANSFERED)
        self.signalTransferEnded(engine_path)
      
    else:
      # Directory case
      (cumulated_size, dir_element_action_info, transfer_type) = self._database_server.get_transfer_action_info(engine_path)
      if not relative_path in dir_element_action_info:
        raise WorkflowEngineError('send_piece: the file %s doesn t belong to the transfer %s' %(relative_path, engine_path))
      (file_size, md5_hash) = dir_element_action_info[relative_path]
      transfer_ended = self._sendToFile(os.path.join(engine_path, relative_path), data, file_size, md5_hash)
      
      if transfer_ended:
        cumulated_file_size, cumulated_transmissions, files_transfer_status = self.transfer_progression_status(engine_path, (cumulated_size, dir_element_action_info, transfer_type))
        if cumulated_transmissions == cumulated_file_size:
          self._database_server.set_transfer_status(engine_path, constants.TRANSFERED)
          self.signalTransferEnded(engine_path)
      
    return transfer_ended


  def retrieve_piece(self, engine_path, buffer_size, transmitted, relative_path = None):
    '''
    Implementation of the L{Jobs} method.
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
    if not self._database_server.is_user_transfer(engine_path, self._user_id) :
      #print "Couldn't set transfer status %s. It doesn't exist or is not owned by the current user \n" % engine_path
      return
    
    self._database_server.set_transfer_status(engine_path, status)


  def delete_transfer(self, engine_path):
    '''
    Implementation of the L{Jobs} method.
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
    if len(job.command) == 0:
      raise WorkflowEngineError("Submission error: the command must" 
                                "contain at least one element \n", self.logger)
    
    job_id = self._engine_loop.add_job(job, queue)

    return job_id


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

    self._engine_loop.restart_workflow(workflow_id)
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
      return
    return self._database_server.get_workflow(wf_id)
  
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
       datetime.now() - last_status_update > timedelta(seconds = refreshment_interval*5):
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
       datetime.now() - last_status_update > timedelta(seconds = refreshment_interval*5):
      self._database_server.set_workflow_status(wf_id, constants.WARNING)
      return constants.WARNING

    return status
    
  
  def workflow_nodes_status(self, wf_id, groupe = None):
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
       datetime.now() - last_status_update > timedelta(seconds = refreshment_interval*5):
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
    if transfer_action_info[2] == constants.FILE_SENDING:
      (file_size, md5_hash, transfer_type) = transfer_action_info
      transmitted = os.stat(engine_path).st_size
      return (file_size, transmitted)
    elif transfer_action_info[2] == constants.DIR_SENDING:
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
  
    dbJob = self._database_server.get_job(job_id)
    exit_status = dbJob.exit_status
    exit_value = dbJob.exit_value
    terminating_signal =dbJob.terminating_signal
    resource_usage = dbJob.resource_usage
    
    return (exit_status, exit_value, terminating_signal, resource_usage)
    

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
        raise WorkflowEngineError( "Could not wait for job %d. It doesn't exist or is owned by a different user \n" %jid, self.logger)
      
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
          if last_status_update and datetime.now() - last_status_update > timedelta(seconds = refreshment_interval*10):
            raise WorkflowEngineError("wait_job: Could not wait for job %s. " 
                                      "The process updating its " 
                                      "status failed." %(jid), 
                                       self.logger)
       
 
  def restart_job( self, job_id ):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''
    if not self._database_server.is_user_job(job_id, self._user_id):
      raise WorkflowEngineError("Could not restart job %d. It doesn't exist" 
                                "or is owned by a different user \n" %job_id,
                                self.logger)
    # TBI 


  def kill_job( self, job_id ):
    '''
    Implementation of soma.workflow.client.WorkflowController API
    '''

    if not self._database_server.is_user_job(job_id, self._user_id):
      raise WorkflowEngineError("Could not kill job %d. It doesn't exist" 
                                "or is owned by a different user \n" %job_id,
                                self.logger)
    
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
      if last_status_update and datetime.now() - last_status_update > timedelta(seconds = refreshment_interval*5):
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
         datetime.now() - last_status_update > timedelta(seconds=refreshment_interval*5):
        self.logger.debug("<< _wait_wf_status_update")
        return False
    self.logger.debug("<< _wait_wf_status_update")
    return True
