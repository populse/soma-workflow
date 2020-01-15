# -*- coding: utf-8 -*-

'''
@author: Soizic Laguitton

@organization: I2BM, Neurospin, Gif-sur-Yvette, France
@organization: CATI, France
@organization: U{IFR 49<http://www.ifr49.org>}

@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''


#-------------------------------------------------------------------------------
# Imports
#-------------------------------------------------------------------------

from __future__ import print_function

import os
import hashlib
import stat
import operator
import random
import pickle
import types
import sys
import posixpath
import logging
import six

if sys.version_info[:2] >= (2, 6):
    import json
# import cProfile
# import traceback

if sys.version_info[0] >= 3:
    basestring = str


import soma_workflow.connection as connection
from soma_workflow.transfer import PortableRemoteTransfer, TransferSCP, TransferRsync, TransferMonitoring, TransferLocal
import soma_workflow.constants as constants
import soma_workflow.configuration as configuration
from soma_workflow.errors import TransferError, SerializationError, SomaWorkflowError

#-------------------------------------------------------------------------------
# Classes and functions
#-------------------------------------------------------------------------

# imports required by the users of soma-workflow API (do not remove):
from soma_workflow.client_types import Job
from soma_workflow.client_types import EngineExecutionJob
from soma_workflow.custom_jobs import BarrierJob
from soma_workflow.custom_jobs import MapJob
from soma_workflow.custom_jobs import ReduceJob
from soma_workflow.custom_jobs import ListCatJob
from soma_workflow.custom_jobs import LeaveOneOutJob
from soma_workflow.custom_jobs import CrossValidationFoldJob
from soma_workflow.client_types import Workflow
from soma_workflow.client_types import Group
from soma_workflow.client_types import FileTransfer
from soma_workflow.client_types import SharedResourcePath
from soma_workflow.client_types import TemporaryPath
from soma_workflow.client_types import SpecialPath
from soma_workflow.client_types import OptionPath
from soma_workflow import scheduler

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

        .. note::
          The login and password are only required for a remote computing
          resource.

        Parameters
        ----------
        resource_id: str
            Identifier of the computing resource to connect to.
            If None, the number of cpu of the current machine is detected and
            the basic scheduler is lauched.

        login: str
            Required if the computing resource is remote.

        password: str
            Required if the computing resource is remote and not RSA key where
            configured to log on the remote machine with ssh.

        config: configuration.Configuration
            Optional configuration.

        rsa_key_pass: str
            Required if the RSA key is protected with a password.
        '''

        if config is None:
            self.config = configuration.Configuration.load_from_file(
                resource_id)
        else:
            self.config = config

        if resource_id is None:
            resource_id \
                = configuration.Configuration.get_local_resource_id(config)

        if password == '':
            password = None

        self.scheduler_config = None

        mode = self.config.get_mode()

        self._resource_id = resource_id

        # LOCAL MODE
        if mode == configuration.LOCAL_MODE:
            print("In local mode")
            # setup logging
            (engine_log_dir,
            engine_log_format,
            engine_log_level) = self.config.get_engine_log_info()
            if engine_log_dir:
                logfilepath = os.path.join(
                    os.path.abspath(engine_log_dir), "log_local_mode")
                log_dir = os.path.dirname(logfilepath)
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                logging.basicConfig(
                    filename=logfilepath,
                    format=engine_log_format,
                    level=eval("logging." + engine_log_level))


            self._connection = connection.LocalConnection(resource_id, "")
            self._engine_proxy = self._connection.get_workflow_engine()
            self.engine_config_proxy = self._connection.get_configuration()
            self._transfer = TransferLocal(self._engine_proxy)
            self._transfer_stdouterr = TransferLocal(self._engine_proxy)

        # REMOTE MODE
        elif mode == configuration.REMOTE_MODE:
            print("In remote mode")
            submitting_machines = self.config.get_submitting_machines()
            sub_machine = submitting_machines[random.randint(
                0, len(submitting_machines) - 1)]
            cluster_address = self.config.get_cluster_address()
            if login is None:
                login = self.config.get_login()
            print('cluster address: %s, submission machine: %s, login: %s'
                  % (cluster_address, sub_machine, login))
            self._connection = connection.RemoteConnection(login,
                                                           password,
                                                           cluster_address,
                                                           sub_machine,
                                                           resource_id,
                                                           "",
                                                           rsa_key_pass,
                                                           self.config)
            print("Remote connection established")
            self._engine_proxy = self._connection.get_workflow_engine()
            self.engine_config_proxy = self._connection.get_configuration()
            self.scheduler_config = self._connection.get_scheduler_config()

            if not password and not rsa_key_pass:
                self._transfer = TransferSCP(self._engine_proxy,
                                             username=login,
                                             hostname=sub_machine)
            else:
                self._transfer = PortableRemoteTransfer(self._engine_proxy)
            self._transfer_stdouterr = PortableRemoteTransfer(
                self._engine_proxy)

        # LIGHT MODE
        elif mode == configuration.LIGHT_MODE:
            print("In light mode")
            local_scdl_cfg_path \
                = configuration.LocalSchedulerCfg.search_config_path()
            if local_scdl_cfg_path == None:
                cpu_count = Helper.cpu_count()
                self.scheduler_config = configuration.LocalSchedulerCfg(
                    proc_nb=cpu_count)
            else:
                self.scheduler_config \
                    = configuration.LocalSchedulerCfg.load_from_file(
                        local_scdl_cfg_path)

            self.config.set_scheduler_config(self.scheduler_config)
            self._engine_proxy = _embedded_engine_and_server(self.config)
            self.engine_config_proxy = self.config
            self._connection = None
            self._transfer = TransferLocal(self._engine_proxy)
            self._transfer_stdouterr = TransferLocal(self._engine_proxy)

        self._transfer_monitoring = TransferMonitoring(self._engine_proxy)
        print("Workflow controller initialised")

    def __del__(self):
        print('del WorkflowController')
        self.disconnect()
        import gc
        gc.collect()

    def disconnect(self):
        '''
        Simulates a disconnection for TEST PURPOSE ONLY.
        !!! The current instance will not be usable anymore after this call !!!!
        '''
        if self._connection:
            self._connection.stop()

    def stop_engine(self):
        self._engine_proxy.stop()
        self.disconnect()

    # SUBMISSION / REGISTRATION ####################################
    def submit_workflow(self,
                        workflow,
                        expiration_date=None,
                        name=None,
                        queue=None):
        '''
        Submits a workflow and returns a workflow identifier.

        Raises *WorkflowError* or *JobError* if the workflow is not correct.

        Parameters
        ----------
        workflow: client.Workflow
            Workflow description.

        expiration_date: *datetime.datetime*
            After this date the workflow will be deleted.

        name: str
            Optional workflow name.

        queue: str
            Optional name of the queue where to submit jobs. If it is not
            specified the jobs will be submitted to the default queue.

        Returns
        -------
        Workflow_identifier: int

        '''

        if self.engine_config_proxy.get_scheduler_type() \
                == configuration.MPI_SCHEDULER:
            raise SomaWorkflowError(
                "The MPI scheduler is configured for this resource. "
                "Use soma_workflow.MPI_workflow_runner to submit a workflow "
                "using the MPI scheduler.")

        # cProfile.runctx("wf_id = self._engine_proxy.submit_workflow(workflow,
        # expiration_date, name, queue)", globals(), locals(),
        # "/home/soizic/profile/profile_submit_workflow")

        wf_id = self._engine_proxy.submit_workflow(workflow,
                                                   expiration_date,
                                                   name,
                                                   queue)
        return wf_id

    def register_transfer(self, file_transfer):
        '''
        Registers a file transfer which is not part of a workflow and returns a
        file transfer identifier.

        Parameters
        ----------
        file_transfer: client.FileTransfer

        Returns
        -------
        transfer: EngineTransfer
        '''

        engine_transfer = self._engine_proxy.register_transfer(file_transfer)

        return engine_transfer

    # WORKFLOWS, JOBS and FILE TRANSFERS RETRIEVAL ###################

    def workflow(self, workflow_id):
        '''
        Raises *UnknownObjectError* if the workflow_id is not valid

        Parameters
        ----------
        workflow_id: workflow_identifier

        Returns
        -------
        Workflow
        '''
        return self._engine_proxy.workflow(workflow_id)

    def workflows(self, workflow_ids=None):
        '''
        Lists the identifiers and general information about all the workflows
        submitted by the user, or about the workflows specified in the
        *workflow_ids* argument.

        Parameters
        ----------
        workflow_ids: sequence of workflow identifiers

        Returns
        -------
        workflows: dictionary: workflow identifier -> tuple(date, string)
            workflow_id -> (workflow_name, expiration_date)
        '''
        return self._engine_proxy.workflows(workflow_ids)

    def jobs(self, job_ids=None):
        '''
        Lists the identifiers and general information about all the jobs
        submitted by the user and which are not part of a workflow, or about
        the jobs specified in the *job_ids* argument.

        Parameters
        ----------
        job_ids: sequence of job identifiers

        Returns
        -------
        jobs: dictionary: job identifiers -> tuple(string, string, date)
            job_id -> (name, command, submission date)
        '''
        return self._engine_proxy.jobs(job_ids)

    def transfers(self, transfer_ids=None):
        '''
        Lists the identifiers and information about all the user's file
        transfers which are not part of a workflow or about the file transfers
        specified in the *transfer_ids* argument.

        Parameters
        ----------
        transfer_ids: sequence of FileTransfer identifiers

        Returns
        -------
        transfers: dictionary: str -> tuple(str, date, None or sequence of str)
            transfer_id -> (
                            * client_path: client file or directory path
                            * expiration_date: after this date the file copied
                              on the computing resource and all the transfer
                              information will be deleted, unless an existing
                              job has declared this file as output or input.
                            * client_paths: sequence of file or directory path
                              or None)
        '''
        return self._engine_proxy.transfers(transfer_ids)

    # WORKFLOW MONITORING #########################################

    def workflow_status(self, workflow_id):
        '''
        Raises *UnknownObjectError* if the workflow_id is not valid

        Parameters
        ----------
        workflow_id: workflow identifier

        Returns
        -------
        status: str or None
            Status of the workflow: see :ref:`workflow-status` or the
            constants.WORKFLOW_STATUS list.
        '''
        return self._engine_proxy.workflow_status(workflow_id)

    def workflow_elements_status(self, workflow_id, with_drms_id=True):
        '''
        Gets back the status of all the workflow elements at once, minimizing
        the communication with the server and request to the database.
        TO DO => make it more user friendly.

        Note: in Soma-Workflow 3.0, the last job info (drmaa_id) has been added
        to job status tuple.

        Parameters
        ----------
        workflow_id: workflow_identifier
        with_drms_id: bool (optional, default=True)
            if True the DRMS id (drmaa_id) is also included in the returned
            tuple for each job. This info has been added in soma_workflow 3.0
            and is thus optional to avoid breaking compatibility with earlier
            versions.

        Returns
        -------
        status: tuple:
            * sequence of tuple
                (job_id, status, queue, exit_info,
                    (submission_date, execution_date, ending_date, drmaa_id),
                    [drms_id]),
            * sequence of tuple
                (transfer_id, (status, progression_info, engine_path,
                 client_path, client_paths)),
            * workflow_status,
            * workflow_queue,
            * sequence of tuple (temp_path_id, engine_path, status)

        Raises *UnknownObjectError* if the workflow_id is not valid
        '''
        wf_status = self._engine_proxy.workflow_elements_status(
            workflow_id, with_drms_id=with_drms_id)
        # special processing for transfer status:
        new_transfer_status = []
        for transfer_id, engine_path, client_path, client_paths, status, \
                transfer_type in wf_status[1]:
            progression = self._transfer_progression(status,
                                                     transfer_type,
                                                     client_path,
                                                     client_paths,
                                                     engine_path)

            new_transfer_status.append((transfer_id,
                                        (status, progression, engine_path,
                                         client_path, client_paths)))

        new_wf_status = (
            wf_status[0], new_transfer_status, wf_status[2], wf_status[3],
            wf_status[4])
        return new_wf_status

    # JOB MONITORING #############################################
    def job_status(self, job_id):
        '''
        Parameters
        ----------
        job_id: job identifier

        Returns
        -------
        status: str
            Status of the job: see :ref:`job-status` or the list
            constants.JOB_STATUS.

        Raises *UnknownObjectError* if the job_id is not valid
        '''
        return self._engine_proxy.job_status(job_id)

    def get_job_command(self, job_id):
        '''
        Get a job commandline from the database
        '''
        return self._engine_proxy.get_job_command(job_id)

    def updated_job_parameters(self, job_id):
        return self._engine_proxy.updated_job_parameters(job_id)

    def get_job_output_params(self, job_id):
        return self._engine_proxy.get_job_output_params(job_id)

    def drms_job_id(self, wf_id, job_id):
        return self._engine_proxy.drms_job_id(wf_id, job_id)

    def job_termination_status(self, job_id):
        '''
        Information related to the end of the job.

        Parameters
        ----------
        job_id: job identifier

        Returns
        -------
        status: tuple(str, int or None, str or None, str) or None
            * exit status: status of the terminated job: see
              :ref:`job-exit-status` or the constants.JOB_EXIT_STATUS list.
            * exit value: operating system exit code of the job if the job
              terminated normally.
            * terminating signal: representation of the signal that caused the
              termination of the job if the job terminated due to the receipt
              of a signal.
            * resource usage: resource usage information provided as an array
              of strings where each string complies with the format
              <name>=<value>.
              The information provided depends on the DRMS and DRMAA
              implementation.

        Raises *UnknownObjectError* if the job_id is not valid
        '''
        return self._engine_proxy.job_termination_status(job_id)

    def retrieve_job_stdouterr(self,
                               job_id,
                               stdout_file_path,
                               stderr_file_path=None,
                               buffer_size=512 ** 2):
        '''
        Copies the job standard output and error to specified file.

        Raises *UnknownObjectError* if the job_id is not valid

        Parameters
        ----------
        job_id: job identifier

        stdout_file_path: str
            Path of the file where to copy the standard output.

        stderr_file_path: str
            Path of the file where to copy the standard error.

        buffer_size: int
            The file is transfered piece by piece of size buffer_size.
        '''
        stdout_file_path = os.path.abspath(stdout_file_path)
        stderr_file_path = os.path.abspath(stderr_file_path)
        (engine_stdout_file,
         engine_stderr_file) = self._engine_proxy.stdouterr_file_path(job_id)

        self._transfer_stdouterr.transfer_from_remote(engine_stdout_file,
                                                      stdout_file_path)
        self._transfer_stdouterr.transfer_from_remote(engine_stderr_file,
                                                      stderr_file_path)

    # FILE TRANSFER MONITORING ###################################
    def transfer_status(self, transfer_id):
        '''
        File transfer status and information related to the transfer progress.

        Parameters
        ----------
        transfer_id: transfer identifier

        Returns
        -------
        status: tuple(transfer_status or None, tuple or None)
            * Status of the file transfer : see :ref:`file-transfer-status` or
              the constants.FILE_TRANSFER_STATUS list.
            * None if the transfer status in not
              constants.TRANSFERING_FROM_CLIENT_TO_CR or
              constants.TRANSFERING_FROM_CR_TO_CLIENT.
              tuple (file size, size already transfered) if it is a file
              transfer.
              tuple (cumulated size, sequence of tuple (relative_path,
              file_size, size already transfered) if it is a directory
              transfer.

        Raises *UnknownObjectError* if the transfer_id is not valid
        '''

        (transfer_id,
         engine_path,
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
                                                 engine_path)

        return (status, progression)

    # WORKFLOW CONTROL ############################################
    def restart_workflow(self, workflow_id, queue=None):
        '''
        Restarts the jobs of the workflow which failed. The jobs will be
        submitted again.
        The workflow status has to be constants.WORKFLOW_DONE.

        Parameters
        ----------
        workflow_id: workflow identifier

        queue: str
            Optional name of the queue where to submit jobs. If it is not
            specified the jobs will be submitted to the default queue.

        Returns
        -------
        success: bool
            True if some jobs were restarted.

        Raises *UnknownObjectError* if the workflow_id is not valid
        '''
        if self.engine_config_proxy.get_scheduler_type() \
                == configuration.MPI_SCHEDULER:
            raise SomaWorkflowError(
                "The MPI scheduler is configured for this resource. "
                "Use soma_workflow.MPI_workflow_runner to restart a workflow "
                "using the MPI scheduler.")

        return self._engine_proxy.restart_workflow(workflow_id, queue)

    def delete_workflow(self, workflow_id, force=True):
        '''
        Deletes the workflow and all its associated elements (FileTransfers and
        Jobs). The worklfow_id will become invalid and can not be used anymore.
        The workflow jobs which are running will be killed.
        If force is set to True: the call will block until the workflow is
        deleted. With force set to True, if the workflow can not be deleted
        properly it is deleted from Soma-workflow database. However, if some
        jobs are still running they are not be killed. In this case the return
        value is False.

        Parameters
        ----------
        workflow_id: workflow_identifier

        force: bool

        Returns
        -------
        success: bool

        Raises *UnknownObjectError* if the workflow_id is not valid
        '''
        # cProfile.runctx("self._engine_proxy.delete_workflow(workflow_id)",
        # globals(), locals(), "/home/soizic/profile/profile_delete_workflow")

        return self._engine_proxy.delete_workflow(workflow_id, force)

    def stop_workflow(self, workflow_id):
        '''
        Stops a workflow.
        The running jobs will be killed.
        The jobs in queues will be removed from queues.
        It will be possible to restart the workflow afterwards.

        Returns
        -------
        success: bool
            returns True if the running jobs were killed and False
            if some jobs are possibly still running on the computing resource
            despite the workflow was stopped.
        '''
        if self.engine_config_proxy.get_scheduler_type() \
                == configuration.MPI_SCHEDULER:
            raise SomaWorkflowError(
                "The MPI scheduler is configured for this resource. "
                "Kill the soma_workflow.MPI_workflow_runner job to stop the "
                "workflow.")

        return self._engine_proxy.stop_workflow(workflow_id)

    def stop_jobs(self, workflow_id, job_ids):
        return self._engine_proxy.stop_jobs(workflow_id, job_ids)

    def restart_jobs(self, workflow_id, job_ids):
        return self._engine_proxy.restart_jobs(workflow_id, job_ids)

    def change_workflow_expiration_date(self, workflow_id,
                                        new_expiration_date):
        '''
        Sets a new expiration date for the workflow.

        Parameters
        ----------
        workflow_id: workflow identifier

        new_expiration_date: datetime.datetime

        Returns
        -------
        success: bool
            True if the expiration date was changed.

        Raises *UnknownObjectError* if the workflow_id is not valid
        '''
        return self._engine_proxy.change_workflow_expiration_date(
            workflow_id, new_expiration_date)

    # JOB CONTROL #################################################
    def wait_job(self, job_ids, timeout=-1):
        '''
        Waits for all the specified jobs to finish.

        Raises *UnknownObjectError* if the job_id is not valid

        Parameters
        ----------
        job_ids: sequence of job identifier
            Jobs to wait for.

        timeout: int
            The call to wait_job exits before timeout seconds.
            A negative value means that the method will wait indefinetely.
        '''
        self._engine_proxy.wait_job(job_ids, timeout)

    def wait_workflow(self, workflow_id, timeout=-1):
        '''
        Waits for the specified workflow to finish.

        Raises *UnknownObjectError* if the job_id is not valid

        Parameters
        ----------
        workflow_id: workflow identifier
            Jobs to wait for.

        timeout: int
            The call to wait_job exits before timeout seconds.
            A negative value means that the method will wait indefinetely.
        '''
        self._engine_proxy.wait_workflow(workflow_id, timeout)

    # FILE TRANSFER CONTROL #######################################
    def transfer_files(self, transfer_ids, buffer_size=512 ** 2):
        '''
        Transfer file(s) associated to the transfer_id.
        If the files are only located on the client side (that is the transfer
        status is constants.FILES_ON_CLIENT) the file(s) will be transfered
        from the client to the computing resource.
        If the files are located on the computing resource side (that is the
        transfer status is constants.FILES_ON_CR or
        constants.FILES_ON_CLIENT_AND_CR)
        the files will be transfered from the computing resource to the client.

        Parameters
        ----------
        transfer_id: FileTransfer identifier

        buffer_size: int
            Depending on the transfer method, the files can be transfered piece
            by piece. The size of each piece can be tuned using the buffer_size
            argument.

        Returns
        -------
        success: bool
            The transfer was done. (TBI right error management)

        Raises *UnknownObjectError* if the transfer_id is not valid
        '''
        #Raises *TransferError*
        if not isinstance(transfer_ids, basestring):
            for transfer_id in transfer_ids:
                self._transfer_file(transfer_id, buffer_size)
        else:
            self._transfer_file(transfer_ids, buffer_size)

    def delete_transfer(self, transfer_id):
        '''
        Deletes the FileTransfer and the associated files and directories on
        the computing resource side. The transfer_id will become invalid and
        can not be used anymore. If some jobs reference the FileTransfer as an
        input or an output the FileTransfer will not be deleted immediately but
        as soon as these jobs will be deleted.

        Raises *UnknownObjectError* if the transfer_id is not valid
        '''
        self._engine_proxy.delete_transfer(transfer_id)

    # PRIVATE #############################################
    def _initialize_transfer(self, transfer_id):
        '''
        Initializes the transfer and returns the transfer action information.

        Parameters
        ----------
        transfer_id: FileTransfer identifier

        Returns
        -------
        transfer: tuple
            transfer_type

            * (file_size, md5_hash) in the case of a file transfer
            * (cumulated_size, dictionary relative path -> (file_size,
              md5_hash)) in case of a directory transfer.

        Raises *UnknownObjectError* if the transfer_id is not valid
        '''
        (transfer_id,
         engine_path,
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
                    self._engine_proxy.set_transfer_status(
                        transfer_id, constants.TRANSFERING_FROM_CLIENT_TO_CR)
                    self._engine_proxy.set_transfer_type(transfer_id,
                                                         transfer_type)

                elif os.path.isdir(client_path):
                    transfer_type = constants.TR_DIR_C_TO_CR
                    self._engine_proxy.set_transfer_status(
                        transfer_id, constants.TRANSFERING_FROM_CLIENT_TO_CR)
                    self._engine_proxy.set_transfer_type(
                        transfer_id, constants.TR_DIR_C_TO_CR)
                else:
                    print("WARNING: The file or directory %s doesn't exist "
                          "on the client machine." % (client_path))
            else:  # client_paths
                for path in client_paths:
                    if not os.path.isfile(path) and not os.path.isdir(path):
                        print("WARNING: The file or directory %s doesn't "
                              "exist on the client machine." % (path))
                transfer_type = constants.TR_MFF_C_TO_CR

            self._engine_proxy.set_transfer_status(
                transfer_id, constants.TRANSFERING_FROM_CLIENT_TO_CR)
            self._engine_proxy.set_transfer_type(transfer_id,
                                                 transfer_type)
            return transfer_type

        elif status == constants.FILES_ON_CR \
                or status == constants.FILES_ON_CLIENT_AND_CR:
            # transfer_type = self._engine_proxy.init_transfer_from_cr(transfer_id,
                                                   # client_path,
                                                   # expiration_date,
                                                   # workflow_id,
                                                   # client_paths,
                                                   # status)
            if not client_paths:
                if self._engine_proxy.is_file(engine_path):
                    transfer_type = constants.TR_FILE_CR_TO_C
                elif self._engine_proxy.is_dir(engine_path):
                    transfer_type = constants.TR_DIR_CR_TO_C
                else:
                    print("WARNING: The file or directory %s doesn't exist "
                          "on the computing resource side." % (engine_path))
            else:  # client_paths
                for path in client_paths:
                    relative_path = os.path.basename(path)
                    r_path = posixpath.join(transfer_id, relative_path)
                    if not self._engine_proxy.is_file(r_path) and \
                       not self._engine_proxy.is_dir(r_path):
                        print("WARNING: The file or directory %s doesn't "
                              "exist on the computing resource side."
                              % (r_path))
                transfer_type = constants.TR_MFF_CR_TO_C

            self._engine_proxy.set_transfer_status(
                transfer_id, constants.TRANSFERING_FROM_CR_TO_CLIENT)
            self._engine_proxy.set_transfer_type(transfer_id,
                                                 transfer_type)

            return transfer_type

    def _transfer_file(self, transfer_id, buffer_size):

        (transfer_id,
         engine_path,
         client_path,
         expiration_date,
         workflow_id,
         client_paths,
         transfer_type,
         status) = self._engine_proxy.transfer_information(transfer_id)

        if status == constants.FILES_ON_CLIENT or \
           status == constants.TRANSFERING_FROM_CLIENT_TO_CR:
            # transfer from client to computing resource
            # overwrite = False
            # if not transfer_type or \
             # transfer_type == constants.TR_FILE_CR_TO_C or \
             # transfer_type == constants.TR_DIR_CR_TO_C or \
             # transfer_type == constants.TR_MFF_CR_TO_C:
                # transfer reset
                # overwrite = True
            transfer_type = self._initialize_transfer(transfer_id)

            remote_path = engine_path

            if transfer_type == constants.TR_FILE_C_TO_CR or \
               transfer_type == constants.TR_DIR_C_TO_CR:
                self._transfer.transfer_to_remote(client_path,
                                                  remote_path)
                self._engine_proxy.set_transfer_status(
                    transfer_id, constants.FILES_ON_CLIENT_AND_CR)
                self._engine_proxy.signalTransferEnded(
                    transfer_id, workflow_id)
                return True

            if transfer_type == constants.TR_MFF_C_TO_CR:
                for path in client_paths:
                    relative_path = os.path.basename(path)
                    r_path = posixpath.join(remote_path, relative_path)
                    self._transfer.transfer_to_remote(path,
                                                      r_path)

                self._engine_proxy.set_transfer_status(
                    transfer_id, constants.FILES_ON_CLIENT_AND_CR)
                self._engine_proxy.signalTransferEnded(
                    transfer_id, workflow_id)
                return True

        if status == constants.FILES_ON_CR or \
           status == constants.TRANSFERING_FROM_CR_TO_CLIENT or \
           status == constants.FILES_ON_CLIENT_AND_CR:
            # transfer from computing resource to client
            # overwrite = False
            # if not transfer_type or \
             # transfer_type == constants.TR_FILE_C_TO_CR or \
             # transfer_type == constants.TR_DIR_C_TO_CR or \
             # transfer_type == constants.TR_MFF_C_TO_CR :
                # TBI remove existing files
                # overwrite = True
            transfer_type = self._initialize_transfer(transfer_id)

            remote_path = engine_path
            if transfer_type == constants.TR_FILE_CR_TO_C or \
               transfer_type == constants.TR_DIR_CR_TO_C:
                # file case
                self._transfer.transfer_from_remote(remote_path,
                                                    client_path)
                self._engine_proxy.set_transfer_status(
                    transfer_id, constants.FILES_ON_CLIENT_AND_CR)
                self._engine_proxy.signalTransferEnded(
                    transfer_id, workflow_id)
                return True

            if transfer_type == constants.TR_MFF_CR_TO_C:
                for path in client_paths:
                    relative_path = os.path.basename(path)
                    r_path = posixpath.join(remote_path, relative_path)
                    self._transfer.transfer_from_remote(r_path,
                                                        path)

                self._engine_proxy.set_transfer_status(
                    transfer_id, constants.FILES_ON_CLIENT_AND_CR)
                self._engine_proxy.signalTransferEnded(
                    transfer_id, workflow_id)
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
                    (ds, dt) \
                        = self._transfer_monitoring.transfer_to_remote_progression(
                            path, r_path)
                    data_size = data_size + ds
                    data_transfered = data_transfered + dt
                progression = (data_size, data_transfered)
            else:
                progression \
                    = self._transfer_monitoring.transfer_to_remote_progression(
                        client_path, engine_path)

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
                progression = self._transfer_monitoring.transfer_from_remote_progression(
                    engine_path,
                    client_path)
        else:
            progression = (100, 100)

        return progression


def _embedded_engine_and_server(config):
    '''
    Creates the workflow engine and workflow database server in the client
    process.
    The client process can not finish before the workflows and jobs are done.
    Using serveral client process simultaneously (thus several database server
    with the same database file) can cause error (notably database locked
    problems)

    Parameters
    ----------
    config: configuration.Configuration

    Returns
    -------
    engine: WorkflowEngine
    '''
    import logging

    from soma_workflow.engine import WorkflowEngine, ConfiguredWorkflowEngine
    from soma_workflow.database_server import WorkflowDatabaseServer

    # configure logging
    log_config = {'version': 1}

    (engine_log_dir,
     engine_log_format,
     engine_log_level) = config.get_engine_log_info()
    if engine_log_dir:
        logfilepath = os.path.join(
            os.path.abspath(engine_log_dir), "log_light_mode")
        log_config['loggers'] = {
            'engine': {
                'level': eval("logging." + engine_log_level),
                'handlers': ['engine'],
                'propagate': False,
            }
        }
        log_config['handlers'] = {
            'engine': {
                'class': 'logging.FileHandler',
                'filename': logfilepath,
                'level': eval("logging." + engine_log_level),
                'formatter': 'engine',
            }
        }
        log_config['formatters'] = {
            'engine': {
                'format': engine_log_format,
            }
        }

    (server_log_file,
     server_log_format,
     server_log_level) = config.get_server_log_info()
    if server_log_file:
        log_config.setdefault('loggers', {})['jobServer'] = {
            'level': eval("logging." + server_log_level),
            'handlers': ['jobServer'],
            'propagate': False,
        }
        log_config.setdefault('handlers', {})['jobServer'] = {
            'class': 'logging.FileHandler',
            'filename': server_log_file,
            'level': eval("logging." + server_log_level),
            'formatter': 'jobServer',
        }
        log_config.setdefault('formatters', {})['jobServer'] = {
            'format': server_log_format,
        }

        if not os.path.exists(os.path.dirname(server_log_file)):
            os.makedirs(os.path.dirname(server_log_file))

    if sys.version_info >= (2, 7):
        import logging.config
        logging.config.dictConfig(log_config)
    elif engine_log_dir:
        logging.basicConfig(
            filename=logfilepath,
            format=engine_log_format,
            level=eval("logging." + engine_log_level))

    if engine_log_dir:
        logger = logging.getLogger('engine')
        logger.info(" ")
        logger.info("****************************************************")
        logger.info("****************************************************")
    if server_log_file:
        logger = logging.getLogger('jobServer')
        logger.info(" ")
        logger.info("****************************************************")
        logger.info("****************************************************")

    # database server
    database_server = WorkflowDatabaseServer(config.get_database_file(),
                                             config.get_transfered_file_dir(),
                                             remove_orphan_files = config.get_remove_orphan_files())

    sch = scheduler.build_scheduler(config.get_scheduler_type(), config)
    workflow_engine = ConfiguredWorkflowEngine(database_server,
                                               sch,
                                               config)

    return workflow_engine


class Helper(object):

    def __init__(self):
        pass

    @staticmethod
    def list_failed_jobs(workflow_id,
                         wf_ctrl,
                         include_aborted_jobs=False,
                         include_user_killed_jobs=False):
        '''
        To spot the problematic jobs in a workflow.

        Parameters
        ----------
        workflow_id: workflow identifier

        include_aborted_jobs: bool
            Include the jobs which exit status is constants.EXIT_ABORTED
            and constants.EXIT_NOTRUN

        include_user_killed_jobs: bool
            Include the jobs which exit status is constants.USER_KILLED

        Returns
        -------
        jobs: list of job identifier
            Returns the list of id of job which status is constants.FAILED
            or which exit value is not 0.
        '''
        (jobs_info,
         transfers_info,
         workflow_status,
         workflow_queue,
         transfers_temp_info) = wf_ctrl.workflow_elements_status(
            workflow_id, with_drms_id=True)
        failed_job_ids = []
        for (job_id, status, queue, exit_info, dates, drmaa_id) in jobs_info:
            if(status == constants.DONE and exit_info[1] != 0) or \
              (status == constants.FAILED and
               (include_aborted_jobs
                or exit_info[0] not in (constants.EXIT_ABORTED,
                                        constants.EXIT_NOTRUN))
               and (include_user_killed_jobs
                    or exit_info[0] != constants.USER_KILLED)):
                failed_job_ids.append(job_id)
        return failed_job_ids

    @staticmethod
    def delete_all_workflows(wf_ctrl, force=True):
        '''
        Delete all the workflows.
        If force is set to True: the call will block until the workflows are
        deleted. With force set to True, if a workflow can not be deleted
        properly it is deleted from Soma-workflow database. However, if some
        jobs are still running they will not be killed. In this case the return
        value is False.

        Parameters
        ----------
        wf_ctrl: client.WorkflowController

        force: bool

        Returns
        -------
        success: bool
        '''

        deleted_properly = True
        while wf_ctrl.workflows():
            wf_id = next(iter(wf_ctrl.workflows().keys()))
            deleted_properly = deleted_properly and wf_ctrl.delete_workflow(
                wf_id, force)
        return deleted_properly

    @staticmethod
    def wait_workflow(workflow_id,
                      wf_ctrl):
        '''
        Waits for workflow execution to end.

        Parameters
        ----------
        workflow_id: workflow identifier

        wf_ctrl: client.WorkflowController
        '''

        wf_ctrl.wait_workflow(workflow_id)

    @staticmethod
    def transfer_input_files(workflow_id,
                             wf_ctrl,
                             buffer_size=512 ** 2):
        '''
        Transfers all the input files of a workflow.

        Parameters
        ----------
        workflow_id: workflow identifier

        wf_ctrl: client.WorkflowController

        buffer_size: int
            Depending on the transfer method, the files can be transfered piece
            by piece. The size of each piece can be tuned using the buffer_size
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
                              buffer_size=512 ** 2):
        '''
        Transfers all the output files of a workflow which are ready to
        transfer.

        Parameters
        ----------
        workflow_id: workflow identifier

        wf_ctrl: client.WorkflowController

        buffer_size: int
            Depending on the transfer method, the files can be transfered piece
            by piece. The size of each piece can be tuned using the buffer_size
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
        Saves a workflow to a file.
        Uses JSON format if Python >= 2.6, Python pickle otherwise.

        Raises *SerializationError* in case of failure

        Parameters
        ----------
        file_path: str

        workflow: client.Workflow

        '''
        from soma_workflow import utils

        if sys.version_info[:2] >= (2, 6):
            try:
                file = open(file_path, "w")
                workflow_dict = workflow.to_dict()
                json.dump(utils.to_json(workflow_dict), file, indent=4)
                file.close()
            except Exception as e:
                six.reraise(SerializationError,
                            SerializationError("%s: %s" % (type(e), e)),
                            sys.exc_info()[2])
        else:
            try:
                file = open(file_path, "w")
                pickle.dump(workflow, file)
                file.close()
            except Exception as e:
                raise SerializationError("%s: %s" % (type(e), e))

    @staticmethod
    def unserialize(file_path):
        '''
        Loads a workflow from a file.
        Opens JSON format or pickle if Python >= 2.6, only Python pickle
        otherwise (see the method: Helper.convert_wf_file_for_p2_5).

        Parameters
        ----------
        file_path: str

        Returns
        -------
        workflow: client.Workflow

        Raises *SerializationError* in case of failure
        '''

        from soma_workflow import utils

        if sys.version_info[:2] >= (2, 6):
            try:
                file = open(file_path, "r")
            except Exception as e:
                raise SerializationError("%s: %s" % (type(e), e))

            workflow = None
            try:
                dict_from_json = utils.from_json(json.load(file))
            except ValueError as e:
                pass
            else:
                workflow = Workflow.from_dict(dict_from_json)

            if not workflow:
                file.close()
                file = open(file_path, "r")
                try:
                    workflow = pickle.load(file)
                except Exception as e:
                    raise SerializationError("%s: %s" % (type(e), e))

            try:
                file.close()
            except Exception as e:
                raise SerializationError("%s: %s" % (type(e), e))

        else:
            try:
                file = open(file_path, "r")
            except Exception as e:
                raise SerializationError("%s: %s" % (type(e), e))
            try:
                workflow = pickle.load(file)
            except Exception as e:
                raise SerializationError(
                  "Error %s: %s \n\n"
                  "The workflow file may have been created "
                  "using Python >= 2.6 using the JSON format.\n"
                  "Use the converter: \n"
                  "soma_workflow.client.Helper.convert_wf_file_for_p2_5 "
                  " " % (type(e), e))
            try:
                file.close()
            except Exception as e:
                raise SerializationError("%s: %s" % (type(e), e))

        # compatibility with version 2.2 and previous
        for job in workflow.jobs:
            if not hasattr(job, "native_specification"):
                job.native_specification = None

        return workflow

    @staticmethod
    def convert_wf_file_for_p2_5(origin_file_path, target_file_path):
        '''
        This method requires Python >= 2.6.
        It converts a workflow file created using Python >= 2.6 to workflow
        file usable in Python 2.5.
        '''
        from soma_workflow import utils

        if sys.version_info[:2] < (2, 6):
            raise Exception("convert_wf_file_for_p2_5 requires Python >= 2.6.")

        try:
            o_file = open(origin_file_path, "r")
            dict_from_json = utils.from_json(json.load(o_file))
            workflow = Workflow.from_dict(dict_from_json)
            o_file.close()
            t_file = open(target_file_path, "w")
            pickle.dump(workflow, t_file)
            t_file.close()
        except Exception as e:
            SerializationError("%s: %s" % (type(e), e))

    @staticmethod
    def cpu_count():
        """
        Detects the number of CPUs on a system.
        ==> Python >= 2.6: multiprocessing.cpu_count
        """
        return configuration.cpu_count()

