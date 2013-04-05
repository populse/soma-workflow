
'''
@author: Soizic Laguitton

@organization: I2BM, Neurospin, Gif-sur-Yvette, France
@organization: CATI, France
@organization: U{IFR 49<http://www.ifr49.org>}

@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''


'''
Engine jobs, workflows and file transfers are defined in a separated 
module because of the dependencies of the engine module.
The class EngineJob, EngineWorkflow and EngineTransfer can thus be 
uses on the client side (in the GUI for example) without importing
module required in the engine module. 
'''

import types
import os
import logging

from soma.workflow.errors import JobError, WorkflowError
import soma.workflow.constants as constants
from soma.workflow.client import Job, FileTransfer, Workflow, SharedResourcePath, Group


class EngineJob(Job):
  
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
                                    client_job.parallel_job_info,
                                    client_job.priority,
                                    client_job.native_specification)
    
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


    if self.stdin:
      if isinstance(self.stdin, FileTransfer):
        if not self.stdin in self.referenced_input_files:
          self.referenced_input_files.append(self.stdin)
        #if not self.stdin in self.transfer_mapping:
          #if isinstance(self.stdin, EngineTransfer):
            ## TBI check that the transfer exist in the database 
            #self.transfer_mapping[self.stdin] = self.stdin
          #else:
            #eft = EngineTransfer(self.stdin)
            #self.transfer_mapping[self.stdin] = eft
      elif isinstance(self.stdin, SharedResourcePath):
        self.srp_mapping[self.stdin] = self._translate(self.stdin) 
      else:
        if not type(self.stdin) in types.StringTypes:
          raise JobError("Wrong stdin type: %s" %(repr(self.stdin))) 
        self.stdin = os.path.abspath(self.stdin)

    if self.working_directory:
      if isinstance(self.working_directory, FileTransfer):
        if not self.working_directory in self.referenced_input_files:
          self.referenced_input_files.append(self.working_directory)
        if not self.working_directory in self.referenced_output_files:
          self.referenced_output_files.append(self.working_directory)
        #if not self.working_directory in self.transfer_mapping:
          #if isinstance(self.working_directory, EngineTransfer):
            ## TBI check that the transfer exist in the database 
            #self.transfer_mapping[self.working_directory] = self.working_directory
          #else:
            #eft = EngineTransfer(self.working_directory)
            #self.transfer_mapping[self.working_directory] = eft
      elif isinstance(self.working_directory, SharedResourcePath):
        self.srp_mapping[self.working_directory] = self._translate(self.working_directory)
      else:
        if not type(self.working_directory) in types.StringTypes:
          raise JobError("Wrong working directory type: %s " %
                         (repr(self.working_directory)))
        self.working_directory = os.path.abspath(self.working_directory)

    if self.stdout_file:
      if isinstance(self.stdout_file, FileTransfer):
        if not self.stdout_file in self.referenced_output_files:
          self.referenced_output_files.append(self.stdout_file)
        #if not self.stdout_file in self.transfer_mapping:
          #if isinstance(self.stdout_file, EngineTransfer):
            ## TBI check that the transfer exist in the database 
            #self.transfer_mapping[self.stdout_file] = self.stdout_file
          #else:
            #eft = EngineTransfer(self.stdout_file)
            #self.transfer_mapping[self.stdout_file] = eft
      elif isinstance(self.stdout_file, SharedResourcePath):
        self.srp_mapping[self.stdout_file] = self._translate(self.stdout_file) 
      else:
        if not type(self.stdout_file) in types.StringTypes:
          raise JobError("Wrong stdout_file type: %s" %(repr(self.stdout_file))) 
        self.stdout_file = os.path.abspath(self.stdout_file)

    if self.stderr_file:
      if isinstance(self.stderr_file, FileTransfer):
        if not self.stderr_file in self.referenced_output_files:
          self.referenced_output_files.append(self.stderr_file)
        #if not self.stderr_file in self.transfer_mapping:
          #if isinstance(self.stderr_file, EngineTransfer):
            ## TBI check that the transfer exist in the database 
            #self.transfer_mapping[self.stderr_file] = self.stderr_file
          #else:
            #eft = EngineTransfer(self.stderr_file)
            #self.transfer_mapping[self.stderr_file] = eft
      elif isinstance(self.stderr_file, SharedResourcePath):
        self.srp_mapping[self.stderr_file] = self._translate(self.stderr_file) 
      else:
        if not type(self.stderr_file) in types.StringTypes:
          raise JobError("Wrong stderr_file type: %s" %(repr(self.stderr_file))) 
        self.stderr_file = os.path.abspath(self.stderr_file)
    

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
      
 

  def _translate(self, srp):
    '''
    srp: SharedResourcePath
    returns: the translated path
    '''
    
    if not self.path_translation:
      raise JobError("Could not submit workflows or jobs with shared resource "
                     "path: no translation found in the configuration file.")
    if not srp.namespace in self.path_translation.keys():
      raise JobError("Could not translate shared resource path. The "               
                     "namespace %s is not configured." %(srp.namespace))
    if not srp.uuid in self.path_translation[srp.namespace]:
      raise JobError("Could not translate shared resource path. The uuid %s "
                     "does not exist for the namespace %s." %
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
        str_list = str(repr(new_list)) 
        plain_command.append(str_list.replace("'", "\""))
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
              self.terminating_signal != None)) 
    return failed

  def ended_with_success(self):
    success = self.is_done() and \
              self.exit_value == 0 and \
              self.exit_status == constants.FINISHED_REGULARLY and \
              self.terminating_signal == None
    return success



class EngineWorkflow(Workflow):
  
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
  
  # mapping between FileTransfer and actual EngineTransfer which are valid on
  # the system. 
  # dictionary: FileTransfer -> EngineTransfer
  transfer_mapping = None

  # Once registered on the database server each 
  # EngineJob has an job_id.
  # dictonary: job_id -> EngineJob
  registered_jobs = None
  
  # Once registered on the database server each 
  # EngineTransfer has an transfer_id.
  # dictonary: tr_id -> EngineTransfer
  registered_tr = None

  # for each job: list of all the jobs which have to end before a job can start
  #dictionary: job_id -> list of job id
  _dependency_dict = None

  logger = None
  
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

    self.user_storage = client_workflow.user_storage

    self.job_mapping = {}
    self.transfer_mapping = {}
    self._map()

    self.registered_tr = {}
    self.registered_jobs = {}

    self._dependency_dict = {}
    for dep in self.dependencies:
      if dep[1] in self._dependency_dict:
        self._dependency_dict[dep[1]].append(dep[0])
      else:
        self._dependency_dict[dep[1]] = [dep[0]] 




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
                          path_translation=self._path_translation,
                          transfer_mapping=self.transfer_mapping)
        self.transfer_mapping.update(ejob.transfer_mapping)
        self.job_mapping[dependency[0]]=ejob

      if dependency[1] not in self.job_mapping:
        self.jobs.append(dependency[1])
        ejob = EngineJob( client_job=dependency[1],
                          queue=self.queue,
                          path_translation=self._path_translation,
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
                              path_translation=self._path_translation,
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
                            path_translation=self._path_translation,
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

    self.logger = logging.getLogger('engine.EngineWorkflow') 
    self.logger.debug("self.jobs="+repr(self.jobs))
    to_run = []
    to_abort = set([])
    done = []
    running = []
    for client_job in self.jobs:
      self.logger.debug("client_job="+repr(client_job))
      job = self.job_mapping[client_job]
      if job.is_done(): 
        done.append(job)
      elif job.is_running(): 
        running.append(job)
      elif job.status == constants.NOT_SUBMITTED:
        job_to_run = True
        job_to_abort = False
        for ft in job.referenced_input_files:
          eft = job.transfer_mapping[ft]
          if not eft.files_exist_on_server():
            if eft.status == constants.TRANSFERING_FROM_CR_TO_CLIENT:
              #TBI stop the transfer
              pass 
            job_to_run = False
            break
        if client_job in self._dependency_dict:
          for dep_client_job in self._dependency_dict[client_job]:
            dep_job = self.job_mapping[dep_client_job]
            if not dep_job.ended_with_success():
              job_to_run = False
              if dep_job.failed():
                job_to_abort = True
                break
            # TO DO to abort
        if job_to_run:
          to_run.append(job)
        if job_to_abort:
          to_abort.add(job)
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
      if job.job_id and job.status != constants.FAILED:
        self.logger.debug("  ---- Failure: job to abort " + job.name)
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
      self.logger.debug("!!!! The workflow may be stuck !!!!")
    else:
      status = constants.WORKFLOW_NOT_STARTED

    return (to_run, ended_jobs, status)

  
  def _update_state_from_database_server(self, database_server):
    wf_status = database_server.get_detailed_workflow_status(self.wf_id)

    for job_info in wf_status[0]:
      job_id, status, queue, exit_info, date_info = job_info
      self.registered_jobs[job_id].status = status
      exit_status, exit_value, term_signal, resource_usage = exit_info
      self.registered_jobs[job_id].exit_status = exit_status
      self.registered_jobs[job_id].exit_value = exit_value
      self.registered_jobs[job_id].str_rusage = resource_usage
      self.registered_jobs[job_id].terminating_signal = term_signal
   
    for ft_info in wf_status[1]:
      (engine_path, 
       client_path, 
       client_paths,
       status, 
       transfer_type) = ft_info 
      self.registered_tr[engine_path].status = status

    self.queue = wf_status[3]


  def force_stop(self, database_server):
    self._update_state_from_database_server(database_server)

    new_status = {}
    new_exit_info = {}

    self.status = constants.WORKFLOW_DONE
  
    for client_job in self.jobs:
      job = self.job_mapping[client_job]
      if not job.is_done():
        job.status = constants.FAILED
        job.exit_status = constants.EXIT_ABORTED
        job.exit_value = None
        job.terminating_signal = None
        job.drmaa_id = None
        job.str_rusage = None
        stdout = open(job.stdout_file, "w")
        stdout.close()
        stderr = open(job.stderr_file, "w")
        stderr.close()
        new_status[job.job_id] = constants.FAILED
        new_exit_info[job.job_id] = job

    database_server.set_jobs_status(new_status)
    database_server.set_jobs_exit_info(new_exit_info)
    database_server.set_workflow_status(self.wf_id, self.status)


  def restart(self, database_server, queue):
   
    self._update_state_from_database_server(database_server)

    self.queue = queue
    to_restart = False
    undone_jobs = []
    done = True
    sub_info_to_resert = {}
    new_status = {}
    jobs_queue_changed = []
    for client_job in self.jobs:
      job = self.job_mapping[client_job]
      if job.failed():
        #clear all the information related to the previous job submission
        job.status = constants.NOT_SUBMITTED
        job.exit_status = None
        job.exit_value = None
        job.terminating_signal = None
        job.drmaa_id = None
        job.queue = self.queue
        jobs_queue_changed.append(job.job_id)
        stdout = open(job.stdout_file, "w")
        stdout.close()
        stderr = open(job.stderr_file, "w")
        stderr.close()

        sub_info_to_resert[job.job_id] = None
        new_status[job.job_id] = constants.NOT_SUBMITTED

      if not job.ended_with_success():
        undone_jobs.append(job)
        job.queue = self.queue
        jobs_queue_changed.append(job.job_id)
        
    database_server.set_submission_information(sub_info_to_resert, None)
    database_server.set_jobs_status(new_status)
    database_server.set_queue(self.queue, jobs_queue_changed, self.wf_id)

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
 
