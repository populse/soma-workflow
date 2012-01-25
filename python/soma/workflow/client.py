# -*- coding: utf-8 -*-
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
objects to be used in the WorkflowController interface.

Definitions:
A Parallel job is a job requiring more than one node to run.
DRMS: distributed resource management system
'''

#-------------------------------------------------------------------------------
# Imports
#-------------------------------------------------------------------------------

import os
import hashlib
import stat
import operator
import random
import pickle
import types
import subprocess
import sys
import posixpath

#import cProfile
#import traceback

import soma.workflow.connection as connection
from soma.workflow.transfer import PortableRemoteTransfer, TransferSCP, TransferRsync, TransferMonitoring, TransferLocal
import soma.workflow.constants as constants
import soma.workflow.configuration as configuration
from soma.workflow.errors import TransferError, SerializationError

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

  **priority**: *int*
    Job priority: 0 = low priority. If several Jobs are ready to run at the 
    same time the jobs with higher priority will be submitted first.

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

  # int 
  priority = None

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
                parallel_job_info=None,
                priority=0):
    if not name:
      self.name = command[0]
    else:
      self.name = name
    self.command = command
    if referenced_input_files:
      self.referenced_input_files = referenced_input_files
    else: self.referenced_input_files = []
    if referenced_output_files:
      self.referenced_output_files = referenced_output_files
    else: self.referenced_output_files = []
    self.stdin = stdin
    self.join_stderrout = join_stderrout
    self.disposal_timeout = disposal_timeout
    self.stdout_file = stdout_file
    self.stderr_file = stderr_file
    self.working_directory = working_directory
    self.parallel_job_info = parallel_job_info
    self.priority = priority


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

  # for special user storage
  user_storage = None

  def __init__(self,
               jobs,
               dependencies=None,
               root_group=None,
               disposal_timeout=168,
               user_storage=None,
               name=None):

    self.name = name
    self.jobs = jobs
    if dependencies != None:
      self.dependencies = dependencies
    else:
      self.dependencies = []
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
      self.initial_status = constants.FILES_ON_CLIENT
    else:
      self.initial_status = constants.FILES_DO_NOT_EXIST


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

  _connection = None

  _engine_proxy = None

  _transfer = None

  _transfer_stdouterr = None

  config = None

  engine_config_proxy = None

  _resource_id = None

  scheduler_config = None

  def __init__(self,
               resource_id=None,
               login=None,
               password=None,
               config=None,
               rsa_key_pass=None):
    '''
    Sets up the connection to the computing resource.
    Looks for a soma-workflow configuration file (if not specified in the
    *config* argument).

    * resource_id *string*
        Identifier of the computing resource to connect to.
        If None, the number of cpu of the current machine is detected and the basic scheduler is lauched.

    * login *string*
        Required if the computing resource is remote.

    * password *string*
        Required if the computing resource is remote and not RSA key where
        configured to log on the remote machine with ssh.

    * config *configuration.Configuration*
        Optional configuration.

    * rsa_key_pass *string*
        Required if the RSA key is protected with a password.

    .. note::
      The login and password are only required for a remote computing resource.
    '''

    if config == None:
      self.config = configuration.Configuration.load_from_file(resource_id)
    else:
      self.config = config

    self.scheduler_config = None

    mode = self.config.get_mode()
    print  mode + " mode"

    self._resource_id = resource_id
    
    

    # LOCAL MODE
    if mode == configuration.LOCAL_MODE:
      self._connection = connection.LocalConnection(resource_id, "")
      self._engine_proxy = self._connection.get_workflow_engine()
      self.engine_config_proxy = self._connection.get_configuration()
      self._transfer = TransferLocal(self._engine_proxy)
      #self._transfer = PortableRemoteTransfer(self._engine_proxy)
      #self._transfer = TransferSCP(self._engine_proxy,
                                    #username=None,
                                    #hostname=None)

      #self._transfer = TransferRsync(self._engine_proxy,
                                    #username=None,
                                    #hostname=None)
      self._transfer_stdouterr = TransferLocal(self._engine_proxy)
      #self._transfer = PortableRemoteTransfer(self._engine_proxy)
      

    # REMOTE MODE
    elif mode == configuration.REMOTE_MODE:
      submitting_machines = self.config.get_submitting_machines()
      sub_machine = submitting_machines[random.randint(0,
                                                    len(submitting_machines)-1)]
      cluster_address = self.config.get_cluster_address()
      print 'cluster address: ' + cluster_address
      print 'submission machine: ' + sub_machine
      self._connection = connection.RemoteConnection(login,
                                                    password,
                                                    cluster_address,
                                                    sub_machine,
                                                    resource_id,
                                                    "",
                                                    rsa_key_pass)
      self._engine_proxy = self._connection.get_workflow_engine()
      self.engine_config_proxy = self._connection.get_configuration()

      if not password and not rsa_key_pass:
        self._transfer = TransferSCP(self._engine_proxy,
                                    username=login,
                                    hostname=sub_machine)
      else:
        self._transfer = PortableRemoteTransfer(self._engine_proxy)
      self._transfer_stdouterr = PortableRemoteTransfer(self._engine_proxy)

    # LIGHT MODE
    elif mode == configuration.LIGHT_MODE:
      local_scdl_cfg_path = configuration.LocalSchedulerCfg.search_config_path()
      if local_scdl_cfg_path == None:
        cpu_count = Helper.cpu_count()
        self.scheduler_config = configuration.LocalSchedulerCfg(proc_nb=cpu_count)
      else:
        self.scheduler_config = configuration.LocalSchedulerCfg.load_from_file(local_scdl_cfg_path)
      self._engine_proxy = _embedded_engine_and_server(self.config, 
                                                       self.scheduler_config)
      self.engine_config_proxy = self.config
      self._connection = None
      self._transfer = TransferLocal(self._engine_proxy)
      #self._transfer = PortableRemoteTransfer(self._engine_proxy)
      self._transfer_stdouterr = TransferLocal(self._engine_proxy)


    self._transfer_monitoring = TransferMonitoring(self._engine_proxy)



  def disconnect(self):
    '''
    Simulates a disconnection for TEST PURPOSE ONLY.
    !!! The current instance will not be usable anymore after this call !!!!
    '''
    if self._connection:
      self._connection.stop()



  ########## SUBMISSION / REGISTRATION ####################################

  def submit_workflow(self,
                      workflow,
                      expiration_date=None,
                      name=None,
                      queue=None):
    '''
    Submits a workflow and returns a workflow identifier.

    * workflow *client.Workflow*
        Workflow description.

    * expiration_date *datetime.datetime*
        After this date the workflow will be deleted.

    * name *string*
        Optional workflow name.

    * queue *string*
        Optional name of the queue where to submit jobs. If it is not specified
        the jobs will be submitted to the default queue.

    * returns: *int*
        Workflow identifier.

    Raises *WorkflowError* or *JobError* if the workflow is not correct.
    '''

    #cProfile.runctx("wf_id = self._engine_proxy.submit_workflow(workflow, expiration_date, name, queue)", globals(), locals(), "/home/soizic/profile/profile_submit_workflow")

    wf_id = self._engine_proxy.submit_workflow(workflow,
                                               expiration_date,
                                               name,
                                               queue)
    return wf_id


  def submit_job(self, job, queue=None):
    '''
    Submits a job which is not part of a workflow.
    Returns a job identifier.

    If the job used transfered files the list of involved file transfer **must
    be** specified setting the arguments: *referenced_input_files* and
    *referenced_output_files*.

    Each path must be reachable from the computing resource.

    * job *client.Job*

    * queue *string*
        Name of the queue where to submit the jobs. If it is not
        specified the job will be submitted to the default queue.

    * returns: int
      Job identifier.

    Raises *JobError* if the job is not correct.
    '''

    job_id = self._engine_proxy.submit_job(job, queue)
    return job_id


  def register_transfer(self, file_transfer):
    '''
    Registers a file transfer which is not part of a workflow and returns a
    file transfer identifier.

    * file_transfer *client.FileTransfer*

    * returns *EngineTransfer*
    '''

    engine_transfer = self._engine_proxy.register_transfer(file_transfer)


    return engine_transfer

  ########## WORKFLOWS, JOBS and FILE TRANSFERS RETRIEVAL ###################

  def workflow(self, workflow_id):
    '''
    * workflow_id *workflow_identifier*

    * returns: *Workflow*

    Raises *UnknownObjectError* if the workflow_id is not valid
    '''
    return self._engine_proxy.workflow(workflow_id)

  def workflows(self, workflow_ids=None):
    '''
    Lists the identifiers and general information about all the workflows
    submitted by the user, or about the workflows specified in the
    *workflow_ids* argument.

    * workflow_ids *sequence of workflow identifiers*

    * returns: *dictionary: workflow identifier -> tuple(date, string)*
        workflow_id -> (workflow_name, expiration_date)
    '''
    return self._engine_proxy.workflows(workflow_ids)


  def jobs(self, job_ids=None):
    '''
    Lists the identifiers and general information about all the jobs submitted
    by the user and which are not part of a workflow, or about the jobs
    specified in the *job_ids* argument.

    * job_ids *sequence of job identifiers*

    * returns: *dictionary: job identifiers -> tuple(string, string, date)*
        job_id -> (name, command, submission date)
    '''
    return self._engine_proxy.jobs(job_ids)


  def transfers(self, transfer_ids=None):
    '''
    Lists the identifiers and information about all the user's file transfers
    which are not part of a workflow or about the file transfers specified in
    the *transfer_ids* argument.

    * transfer_ids *sequence of FileTransfer identifiers*

    * returns: *dictionary: string -> tuple(string, date, None or sequence of string)*
        transfer_id -> (
                        * client_path: client file or directory path
                        * expiration_date: after this date the file copied
                          on the computing resource and all the transfer
                          information will be deleted, unless an existing
                          job has declared this file as output or input.
                        * client_paths: sequence of file or directory path or None)
    '''
    return self._engine_proxy.transfers(transfer_ids)

  ########## WORKFLOW MONITORING #########################################

  def workflow_status(self, workflow_id):
    '''
    * workflow_id *workflow identifier*

    * returns: *string or None*
        Status of the workflow: see :ref:`workflow-status` or the
        constants.WORKFLOW_STATUS list.

    Raises *UnknownObjectError* if the workflow_id is not valid
    '''
    return self._engine_proxy.workflow_status(workflow_id)


  def workflow_elements_status(self, workflow_id, group = None):
    '''
    Gets back the status of all the workflow elements at once, minimizing the
    communication with the server and request to the database.
    TO DO => make it more user friendly.

    * workflow_id *workflow identifier*

    * returns: tuple (sequence of tuple (job_id, status, queue, exit_info, 
      (submission_date, execution_date, ending_date)), sequence of tuple
      (transfer_id, (status, progression_info)), workflow_status, workflow_queue)

    Raises *UnknownObjectError* if the workflow_id is not valid
    '''
    wf_status = self._engine_proxy.workflow_elements_status(workflow_id)
     # special processing for transfer status:
    new_transfer_status = []
    for engine_path, client_path, client_paths, status, transfer_type in wf_status[1]:
      progression = self._transfer_progression(status,
                                               transfer_type,
                                               client_path,
                                               client_paths,
                                               engine_path)

      new_transfer_status.append((engine_path, (status, progression)))

    new_wf_status = (wf_status[0], new_transfer_status, wf_status[2], wf_status[3])
    return new_wf_status


  ########## JOB MONITORING #############################################

  def job_status( self, job_id ):
    '''
    * job_id *job identifier*

    * returns: *string*
        Status of the job: see :ref:`job-status` or the list
        constants.JOB_STATUS.

    Raises *UnknownObjectError* if the job_id is not valid
    '''
    return self._engine_proxy.job_status(job_id)


  def job_termination_status(self, job_id ):
    '''
    Information related to the end of the job.

    * job_id *job identifier*

    * returns: *tuple(string, int or None, string or None, string) or None*
        * exit status: status of the terminated job: see
          :ref:`job-exit-status` or the constants.JOB_EXIT_STATUS list.
        * exit value: operating system exit code of the job if the job
          terminated normally.
        * terminating signal: representation of the signal that caused the
          termination of the job if the job terminated due to the receipt of
          a signal.
        * resource usage: resource usage information provided as an array of
          strings where each string complies with the format <name>=<value>.
          The information provided depends on the DRMS and DRMAA implementation.

    Raises *UnknownObjectError* if the job_id is not valid
    '''
    return self._engine_proxy.job_termination_status(job_id)


  def retrieve_job_stdouterr(self,
                             job_id,
                             stdout_file_path,
                             stderr_file_path = None,
                             buffer_size = 512**2):
    '''
    Copies the job standard output and error to specified file.

    * job_id *job identifier*

    * stdout_file_path *string*
        Path of the file where to copy the standard output.

    * stderr_file_path *string*
        Path of the file where to copy the standard error.

    * buffer_size *int*
        The file is transfered piece by piece of size buffer_size.

    Raises *UnknownObjectError* if the job_id is not valid
    '''
    stdout_file_path = os.path.abspath(stdout_file_path)
    stderr_file_path = os.path.abspath(stderr_file_path)
    (engine_stdout_file, engine_stderr_file) = self._engine_proxy.stdouterr_file_path(job_id)

    self._transfer_stdouterr.transfer_from_remote(engine_stdout_file,
                                                  stdout_file_path)
    self._transfer_stdouterr.transfer_from_remote(engine_stderr_file,
                                                  stderr_file_path)


  ########## FILE TRANSFER MONITORING ###################################

  def transfer_status(self, transfer_id):
    '''
    File transfer status and information related to the transfer progress.

    * transfer_id *transfer identifier*

    * returns: *tuple(transfer_status or None, tuple or None)*
        * Status of the file transfer : see :ref:`file-transfer-status` or the
          constants.FILE_TRANSFER_STATUS list.
        * None if the transfer status in not
          constants.TRANSFERING_FROM_CLIENT_TO_CR or
          constants.TRANSFERING_FROM_CR_TO_CLIENT.
          tuple (file size, size already transfered) if it is a file transfer.
          tuple (cumulated size, sequence of tuple (relative_path, file_size, size already transfered) if it is a directory transfer.

    Raises *UnknownObjectError* if the transfer_id is not valid
    '''

    (transfer_id,
    client_path,
    expiration_date,
    workflow_id,
    client_paths,
    transfer_type,
    status) = self._engine_proxy.transfer_information(transfer_id)
    progression = self._transfer_progression(status,
                                             transfer_type,
                                             client_path,
                                             client_paths,
                                             transfer_id)

    return (status, progression)


  ########## WORKFLOW CONTROL ############################################

  def restart_workflow(self, workflow_id, queue=None):
    '''
    Restarts the jobs of the workflow which failed. The jobs will be submitted
    again.
    The workflow status has to be constants.WORKFLOW_DONE.

    * workflow_id *workflow identifier*

    * queue *string*
        Optional name of the queue where to submit jobs. If it is not specified
        the jobs will be submitted to the default queue.

    * returns: *boolean*
        True if some jobs were restarted.

    Raises *UnknownObjectError* if the workflow_id is not valid
    '''
    return self._engine_proxy.restart_workflow(workflow_id, queue)


  def delete_workflow(self, workflow_id, force=True):
    '''
    Deletes the workflow and all its associated elements (FileTransfers and
    Jobs). The worklfow_id will become invalid and can not be used anymore.
    The workflow jobs which are running will be killed.
    If force is set to True: the client will wait for the workflow to be deleted.
    If it can't be deleted properly workflow will be deleted from
    the database server. However, if some jobs are running they won't be kill
    and will burden the computing resource.

    * workflow_id *workflow_identifier*

    * force *boolean*
      If force is set to True, the call won't return before the workflow is
      deleted. It will wait for the workflow to be deleted.
      If it can't be deleted properly workflow will be deleted from
      the database server. However, if some jobs are running they won't be kill
      and will burden the computing resource (see return value).

    * returns: *boolean*
      If force is True: return True if the running jobs were killed and False
      if some jobs are possibly still running on the computing resource despite
      the workflow doesn't exist.

    Raises *UnknownObjectError* if the workflow_id is not valid
    '''
    #cProfile.runctx("self._engine_proxy.delete_workflow(workflow_id)", globals(), locals(), "/home/soizic/profile/profile_delete_workflow")

    return self._engine_proxy.delete_workflow(workflow_id, force)


  def stop_workflow(self, workflow_id):
    '''
    Stops a workflow.
    The running jobs will be killed.
    The jobs in queues will be removed from queues.
    It will be possible to restart the workflow afterwards.

     * returns: *boolean*

      return True if the running jobs were killed and False
      if some jobs are possibly still running on the computing resource 
      despite the workflow was stopped.
    '''

    return self._engine_proxy.stop_workflow(workflow_id)

  def change_workflow_expiration_date(self, workflow_id, new_expiration_date):
    '''
    Sets a new expiration date for the workflow.

    * workflow_id *workflow identifier*

    * new_expiration_date *datetime.datetime*

    * returns: *boolean*
        True if the expiration date was changed.

    Raises *UnknownObjectError* if the workflow_id is not valid
    '''
    return self._engine_proxy.change_workflow_expiration_date(workflow_id, new_expiration_date)


  ########## JOB CONTROL #################################################

  def wait_job( self, job_ids, timeout = -1):
    '''
    Waits for all the specified jobs to finish.

    * job_ids *sequence of job identifier*
        Jobs to wait for.

    * timeout *int*
        The call to wait_job exits before timeout seconds.
        A negative value means that the method will wait indefinetely.

    Raises *UnknownObjectError* if the job_id is not valid
    '''
    self._engine_proxy.wait_job(job_ids, timeout)

  def kill_job(self, job_id ):
    '''
    Kills a running job. The job will not be deleted from the system (the job
    identifier remains valid).
    Use the restart_job method to restart the job.

    Raises *UnknownObjectError* if the job_id is not valid
    '''
    self._engine_proxy.kill_job(job_id)


  def restart_job(self, job_id):
    '''
    Restarts a job which status is constants.FAILED or constants.WARNING.

    * job_id *job identifier*
    * returns: *boolean*
        True if the job was restarted.

    Raises *UnknownObjectError* if the job_id is not valid
    '''
    self._engine_proxy.restart_job(job_id)


  def delete_job(self, job_id, force=True):
    '''
    Deletes a job which is not part of a workflow.
    The job_id will become invalid and can not be used anymore.
    The job is killed if it is running.

    Raises *UnknownObjectError* if the job_id is not valid

    * returns: *boolean*
      If force is True: return True if the running jobs were killed and False
      if some jobs are possibly still running on the computing resource despite
      the workflow doesn't exist.
    '''

    return self._engine_proxy.delete_job(job_id, force)


  ########## FILE TRANSFER CONTROL #######################################

  def transfer_files(self, transfer_ids, buffer_size = 512**2):
    '''
    Transfer file(s) associated to the transfer_id.
    If the files are only located on the client side (that is the transfer
    status is constants.FILES_ON_CLIENT) the file(s) will be transfered from the
    client to the computing resource.
    If the files are located on the computing resource side (that is the
    transfer status is constants.FILES_ON_CR or
    constants.FILES_ON_CLIENT_AND_CR)
    the files will be transfered from the computing resource to the client.

    * transfer_id *FileTransfer identifier*

    * buffer_size *int*
        Depending on the transfer method, the files can be transfered piece by
        piece. The size of each piece can be tuned using the buffer_size
        argument.

    * returns: *boolean*
        The transfer was done. (TBI right error management)

    Raises *UnknownObjectError* if the transfer_id is not valid
    #Raises *TransferError*
    '''
    if not type(transfer_ids) in types.StringTypes:
        for transfer_id in transfer_ids:
          self._transfer_file(transfer_id, buffer_size)
    else:
      self._transfer_file(transfer_ids, buffer_size)



  def delete_transfer(self, transfer_id):
    '''
    Deletes the FileTransfer and the associated files and directories on the
    computing resource side. The transfer_id will become invalid and can not be
    used anymore. If some jobs reference the FileTransfer as an input or an
    output the FileTransfer will not be deleted immediately but as soon as these
    jobs will be deleted.

    Raises *UnknownObjectError* if the transfer_id is not valid
    '''
    self._engine_proxy.delete_transfer(transfer_id)


  ########## PRIVATE #############################################

  def _initialize_transfer(self, transfer_id):
    '''
    Initializes the transfer and returns the transfer action information.

    * transfer_id *FileTransfer identifier*

    * returns: *tuple*
        transfer_type


        * (file_size, md5_hash) in the case of a file transfer
        * (cumulated_size, dictionary relative path -> (file_size, md5_hash)) in
          case of a directory transfer.

    Raises *UnknownObjectError* if the transfer_id is not valid
    '''
    (transfer_id,
     client_path,
     expiration_date,
     workflow_id,
     client_paths,
     transfer_type,
     status) = self._engine_proxy.transfer_information(transfer_id)

    if status == constants.FILES_ON_CLIENT:
      if not client_paths:
        if os.path.isfile(client_path):
          transfer_type = constants.TR_FILE_C_TO_CR
          self._engine_proxy.set_transfer_status(transfer_id,
                                                 constants.TRANSFERING_FROM_CLIENT_TO_CR)
          self._engine_proxy.set_transfer_type(transfer_id,
                                               transfer_type)


        elif os.path.isdir(client_path):
          transfer_type = constants.TR_DIR_C_TO_CR
          self._engine_proxy.set_transfer_status(transfer_id,
                                                 constants.TRANSFERING_FROM_CLIENT_TO_CR)
          self._engine_proxy.set_transfer_type(transfer_id,
                                               constants.TR_DIR_C_TO_CR)
        else:
          print("WARNING: The file or directory %s doesn't exist "
                "on the client machine." %(client_path))
      else: #client_paths
        for path in client_paths:
          if not os.path.isfile(path) and not os.path.isdir(path):
            print("WARNING: The file or directory %s doesn't exist "
                  "on the client machine." %(path))
        transfer_type = constants.TR_MFF_C_TO_CR

      self._engine_proxy.set_transfer_status(transfer_id,
                                             constants.TRANSFERING_FROM_CLIENT_TO_CR)
      self._engine_proxy.set_transfer_type(transfer_id,
                                           transfer_type)
      return transfer_type

    elif status == constants.FILES_ON_CR or status == constants.FILES_ON_CLIENT_AND_CR:
      #transfer_type = self._engine_proxy.init_transfer_from_cr(transfer_id,
                                               #client_path,
                                               #expiration_date,
                                               #workflow_id,
                                               #client_paths,
                                               #status)
      if not client_paths:
        if self._engine_proxy.is_file(transfer_id):
          transfer_type = constants.TR_FILE_CR_TO_C
        elif self._engine_proxy.is_dir(transfer_id):
          transfer_type = constants.TR_DIR_CR_TO_C
        else:
          print("WARNING: The file or directory %s doesn't exist "
                "on the computing resource side." %(transfer_id))
      else: #client_paths
        for path in client_paths:
          relative_path = os.path.basename(path)
          r_path = posixpath.join(transfer_id, relative_path)
          if not self._engine_proxy.is_file(r_path) and \
             not self._engine_proxy.is_dir(r_path):
            print("WARNING: The file or directory %s doesn't exist "
                  "on the computing resource side." %(r_path))
        transfer_type = constants.TR_MFF_CR_TO_C

      self._engine_proxy.set_transfer_status(transfer_id,
                                        constants.TRANSFERING_FROM_CR_TO_CLIENT)
      self._engine_proxy.set_transfer_type(transfer_id,
                                           transfer_type)

      return transfer_type


  def _transfer_file(self, transfer_id, buffer_size):

    (transfer_id,
     client_path,
     expiration_date,
     workflow_id,
     client_paths,
     transfer_type,
     status) = self._engine_proxy.transfer_information(transfer_id)

    if status == constants.FILES_ON_CLIENT or \
       status == constants.TRANSFERING_FROM_CLIENT_TO_CR:
      # transfer from client to computing resource
      #overwrite = False
      #if not transfer_type or \
         #transfer_type == constants.TR_FILE_CR_TO_C or \
         #transfer_type == constants.TR_DIR_CR_TO_C or \
         #transfer_type == constants.TR_MFF_CR_TO_C:
        ## transfer reset
        #overwrite = True
      transfer_type = self._initialize_transfer(transfer_id)

      remote_path = transfer_id

      if transfer_type == constants.TR_FILE_C_TO_CR or \
         transfer_type == constants.TR_DIR_C_TO_CR:
        self._transfer.transfer_to_remote(client_path,
                                         remote_path)
        self._engine_proxy.set_transfer_status(transfer_id,
                                               constants.FILES_ON_CLIENT_AND_CR)
        self._engine_proxy.signalTransferEnded(transfer_id, workflow_id)
        return True

      if transfer_type == constants.TR_MFF_C_TO_CR:
        for path in client_paths:
          relative_path = os.path.basename(path)
          r_path = posixpath.join(remote_path, relative_path)
          self._transfer.transfer_to_remote(path,
                                            r_path)

        self._engine_proxy.set_transfer_status(transfer_id,
                                               constants.FILES_ON_CLIENT_AND_CR)
        self._engine_proxy.signalTransferEnded(transfer_id, workflow_id)
        return True

    if status == constants.FILES_ON_CR or \
       status == constants.TRANSFERING_FROM_CR_TO_CLIENT or \
       status == constants.FILES_ON_CLIENT_AND_CR:
      # transfer from computing resource to client
      #overwrite = False
      #if not transfer_type or \
         #transfer_type == constants.TR_FILE_C_TO_CR or \
         #transfer_type == constants.TR_DIR_C_TO_CR or \
         #transfer_type == constants.TR_MFF_C_TO_CR :
        ## TBI remove existing files
        #overwrite = True
      transfer_type = self._initialize_transfer(transfer_id)


      remote_path = transfer_id
      if transfer_type == constants.TR_FILE_CR_TO_C or \
         transfer_type == constants.TR_DIR_CR_TO_C:
        # file case
        self._transfer.transfer_from_remote(remote_path,
                                            client_path)
        self._engine_proxy.set_transfer_status(transfer_id,
                                               constants.FILES_ON_CLIENT_AND_CR)
        self._engine_proxy.signalTransferEnded(transfer_id, workflow_id)
        return True

      if transfer_type == constants.TR_MFF_CR_TO_C:
        for path in client_paths:
          relative_path = os.path.basename(path)
          r_path = posixpath.join(remote_path, relative_path)
          self._transfer.transfer_from_remote(r_path,
                                              path)

        self._engine_proxy.set_transfer_status(transfer_id,
                                               constants.FILES_ON_CLIENT_AND_CR)
        self._engine_proxy.signalTransferEnded(transfer_id, workflow_id)
        return True

    return False


  def _transfer_progression(self,
                            status,
                            transfer_type,
                            client_path,
                            client_paths,
                            engine_path):
    if status == constants.TRANSFERING_FROM_CLIENT_TO_CR:
      if transfer_type == constants.TR_MFF_C_TO_CR:
        data_size = 0
        data_transfered = 0
        for path in client_paths:
          relative_path = os.path.basename(path)
          r_path = posixpath.join(engine_path, relative_path)
          (ds,
          dt) = self._transfer_monitoring.transfer_to_remote_progression(path,
                                                                r_path)
          data_size = data_size + ds
          data_transfered = data_transfered + dt
        progression = (data_size, data_transfered)
      else:
        progression = self._transfer_monitoring.transfer_to_remote_progression(
                                                                client_path,
                                                                engine_path)

    elif status == constants.TRANSFERING_FROM_CR_TO_CLIENT:
      if transfer_type == constants.TR_MFF_CR_TO_C:
        data_size = 0
        data_transfered = 0
        for path in client_paths:
          relative_path = os.path.basename(path)
          r_path = posixpath.join(engine_path, relative_path)
          (ds,
          dt) = self._transfer_monitoring.transfer_from_remote_progression(r_path,
                                                                         path)
          data_size = data_size + ds
          data_transfered = data_transfered + dt
        progression = (data_size, data_transfered)
      else:
        progression = self._transfer_monitoring.transfer_from_remote_progression(engine_path,
                                                                  client_path)
    else:
      progression = (100, 100)

    return progression


def _embedded_engine_and_server(config, local_scheduler_config=None):
  '''
  Creates the workflow engine and workflow database server in the client
  process.
  The client process can not finish before the workflows and jobs are done.
  Using serveral client process simultaneously (thus several database server
  with the same database file) can cause error (notably database locked problems)

  * config: *soma.workflow.configuration.Configuration*

  * returns: *WorkflowEngine*
  '''
  import logging

  from soma.workflow.engine import WorkflowEngine, ConfiguredWorkflowEngine
  from soma.workflow.database_server import WorkflowDatabaseServer

  (engine_log_dir,
  engine_log_format,
  engine_log_level) = config.get_engine_log_info()
  if engine_log_dir:
    logfilepath = os.path.join(os.path.abspath(engine_log_dir), "log_light_mode")
    logging.basicConfig(
        filename=logfilepath,
        format=engine_log_format,
        level=eval("logging." + engine_log_level))
    logger = logging.getLogger('engine')
    logger.info(" ")
    logger.info("****************************************************")
    logger.info("****************************************************")

  # database server
  database_server = WorkflowDatabaseServer(config.get_database_file(),
                                           config.get_transfered_file_dir())

  if config.get_scheduler_type() == 'drmaa':
    from soma.workflow.scheduler import Drmaa
    print "scheduler type: drmaa"
    scheduler = Drmaa(config.get_drmaa_implementation(),
                      config.get_parallel_job_config())

  elif config.get_scheduler_type() == 'local_basic':
    from soma.workflow.scheduler import ConfiguredLocalScheduler
    if local_scheduler_config == None:
      local_scheduler_config = LocalSchedulerCfg()
    print "scheduler type: basic, number of cpu: " + repr(local_scheduler_config.get_proc_nb())
    scheduler = ConfiguredLocalScheduler(local_scheduler_config)

  workflow_engine = ConfiguredWorkflowEngine(database_server,
                                             scheduler, 
                                             config)

  return workflow_engine


class Helper(object):

  def __init__(self):
    pass


  @staticmethod
  def wait_workflow(workflow_id,
                    wf_ctrl):
    '''
    Waits for workflow execution to end.

    * workflow_id *workflow identifier*

    * wf_ctrl *client.WorkflowController*
    '''

    element_status = wf_ctrl.workflow_elements_status(workflow_id)
    job_ids = []
    for job_info in element_status[0]:
      job_ids.append(job_info[0])


    wf_ctrl.wait_job(job_ids)



  @staticmethod
  def transfer_input_files(workflow_id,
                           wf_ctrl,
                           buffer_size = 512**2):
    '''
    Transfers all the input files of a workflow.

    * workflow_id *workflow identifier*

    * wf_ctrl *client.WorkflowController*

    * buffer_size *int*
        Depending on the transfer method, the files can be transfered piece by
        piece. The size of each piece can be tuned using the buffer_size
        argument.
    '''
    transfer_info = None
    wf_elements_status = wf_ctrl.workflow_elements_status(workflow_id)

    to_transfer = []
    for transfer_info in wf_elements_status[1]:
      status = transfer_info[1][0]
      if status == constants.FILES_ON_CLIENT:
        engine_path = transfer_info[0]
        to_transfer.append(engine_path)
      if status == constants.TRANSFERING_FROM_CLIENT_TO_CR:
        engine_path = transfer_info[0]
        to_transfer.append(engine_path)

    wf_ctrl.transfer_files(to_transfer, buffer_size)



  @staticmethod
  def transfer_output_files(workflow_id,
                            wf_ctrl,
                            buffer_size=512**2):
    '''
    Transfers all the output files of a workflow which are ready to transfer.

    * workflow_id *workflow identifier*

    * wf_ctrl *client.WorkflowController*

    * buffer_size *int*
        Depending on the transfer method, the files can be transfered piece by
        piece. The size of each piece can be tuned using the buffer_size
        argument.
    '''
    transfer_info = None
    wf_elements_status = wf_ctrl.workflow_elements_status(workflow_id)

    to_transfer = []
    for transfer_info in wf_elements_status[1]:
      status = transfer_info[1][0]
      if status == constants.FILES_ON_CR:
        engine_path = transfer_info[0]
        to_transfer.append(engine_path)
      if status == constants.TRANSFERING_FROM_CR_TO_CLIENT:
        engine_path = transfer_info[0]
        to_transfer.append(engine_path)

    wf_ctrl.transfer_files(to_transfer, buffer_size)



  @staticmethod
  def serialize(file_path, workflow):
    '''
    Saves a workflow to a file => Python Pickle for now.

    * file_path *String*

    * workflow *client.Workflow*

    Raises *SerializationError*
    '''
    try:
      file = open(file_path, "w")
      pickle.dump(workflow, file)
      file.close()
    except Exception, e:
      raise SerializationError("%s: %s" %(type(e), e))

  @staticmethod
  def unserialize(file_path):
    '''
    Loads a workflow from a file 

    * file_path *String*

    * returns: *client.Workflow*

    Raises *SerializationError*
    '''
    try:
      file = open(file_path, "r")
      workflow = pickle.load(file)
      file.close()
    except Exception, e:
      raise SerializationError("%s: %s" %(type(e), e))
    return workflow


  @staticmethod
  def cpu_count():
    """
    Detects the number of CPUs on a system.
    ==> Python >= 2.6: multiprocessing.cpu_count
    """
    if sys.version_info[:2] >= (2,6):
      try:
        import multiprocessing
        return multiprocessing.cpu_count()
      except: # sometimes happens on MacOS... ?
        print >> sys.stderr, \
            'Warning: CPU count detection failed. Using default (2)'
        return 2
    # Linux, Unix and MacOS:
    if hasattr(os, "sysconf"):
        if os.sysconf_names.has_key("SC_NPROCESSORS_ONLN"):
            # Linux & Unix:
            ncpus = os.sysconf("SC_NPROCESSORS_ONLN")
            if isinstance(ncpus, int) and ncpus > 0:
                return ncpus
        else: # OSX:
            return int(subprocess.Popen(["sysctl", "-n", "hw.ncpu"],
                stdout=subprocess.PIPE).stdout.read())
    # Windows:
    if os.environ.has_key("NUMBER_OF_PROCESSORS"):
            ncpus = int(os.environ["NUMBER_OF_PROCESSORS"]);
            if ncpus > 0:
                return ncpus
    return 1 # Default

