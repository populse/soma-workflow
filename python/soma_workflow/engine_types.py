# -*- coding: utf-8 -*-

from __future__ import print_function
from __future__ import absolute_import
from six.moves import range

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

import io
import os
import logging
import tempfile
import weakref
import six
import time
import datetime
from soma_workflow import subprocess
from soma_workflow import utils
import json

from soma_workflow.errors import JobError, WorkflowError
import soma_workflow.constants as constants
from soma_workflow.client import Job, EngineExecutionJob, SpecialPath, \
    FileTransfer, Workflow, SharedResourcePath, TemporaryPath, OptionPath, \
    Group
import sys


class EngineJob(Job):

    '''
    This object represents a Job, i.e. an individual processing task, on the
    server side. It is able to build server representations of SpecialPath
    objets, i.e. files which path is server-dependant (transfered files,
    temporary files, etc.). Its role is then to generate the
    command actually run on server side.

    There should be only one Engine (i.e. server side) version of
    TemporaryPath and FileTransfer objects, these engine representations are
    stored in the transfer_mapping dictionary and shared between jobs of a
    same workflow.

    Parameters
    ----------
    client_job : Job
        Client representation of the job.
    queue : str
        Name of the queue to which the job was submitted.
    workflow_id
        Identifier of the workflow to which the job belong.
    path_translation : dict
        Dictionary mapping namespaces and SharedResourcePath's uuid to the
        server translation of the path.
    transfer_mapping : dict
        Mapping between client side objects (:obj:`FileTransfer` and
        :obj:`TemporaryPath`) and their server side equivalent
        (:obj:`EngineTransfer` and :obj:`EngineTemporaryPath`).
    container_command: list or None
        container (docker / singularity) command prefix to be prepended to
        the commandline
    '''

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

    # mapping between SpecialPath objects and their server (engine) version
    path_mapping = None

    # mapping between CommonPath and actual concatenated path, which should
    # be the same
    # dictionary: OptionPath -> string (path)
    com_mapping = None

    path_translation = None

    logger = None

    # docker / singularity prefix command, to be prepended to all jobs
    # commandlines
    container_command = None

    # job class
    job_class = None

    def __init__(self,
                 client_job,
                 queue,
                 workflow_id=-1,
                 path_translation=None,
                 transfer_mapping=None,
                 container_command=None,
                 wf_env=None):

        super(EngineJob, self).__init__(
            client_job.command,
            client_job.referenced_input_files,
            client_job.referenced_output_files,
            client_job.stdin,
            client_job.join_stderrout,
            client_job.disposal_timeout,
            client_job.name,
            client_job.stdout_file,
            client_job.stderr_file,
            client_job.working_directory,
            client_job.parallel_job_info,
            client_job.priority,
            client_job.native_specification,
            env=client_job.env,
            param_dict=client_job.param_dict,
            use_input_params_file=client_job.use_input_params_file,
            has_outputs=client_job.has_outputs,
            input_params_file=client_job.input_params_file,
            output_params_file=client_job.output_params_file,
            configuration=client_job.configuration)

        self.job_id = -1

        self.drmaa_id = None
        self.status = constants.NOT_SUBMITTED
        self.exit_status = None
        self.exit_value = None
        self.terminating_signal = None

        self.workflow_id = workflow_id
        self.queue = queue

        self.path_translation = path_translation
        self.container_command = container_command

        if not transfer_mapping:
            self.transfer_mapping = {}
        else:
            self.transfer_mapping = transfer_mapping
        self.path_mapping = {}
        if isinstance(client_job, EngineExecutionJob):
            self.is_engine_execution = True
        else:
            self.is_engine_execution = False

        if wf_env:
            # use workflow env, then update with self.env
            env = dict(wf_env)
            if self.env:
                env.update(self.env)
            self.env = env

        self.job_class = type(client_job)
        if hasattr(client_job, 'uuid'):
            self.uuid = client_job.uuid

        self._map()

    def _map(self):
        '''
        Fill the transfer_mapping and srp_mapping attributes.
        + check the types of the Job arguments.
        '''
        if not self.command and not self.is_engine_execution:
            raise JobError("The command attribute is the only required "
                           "attribute of Job.")

        # if self.parallel_job_info:
            # parallel_config_name = self.parallel_job_info.get('config_name')
            # nodes_number = self.parallel_job_info.get('nodes_number', 1)
            # cpu_per_node = self.parallel_job_info.get('cpu_per_node', 1)
            # if not parallel_job_submission_info:
                # raise JobError(
                    #"No parallel information was registered for the "
                    #" current resource. A parallel job can not be submitted")
            # if parallel_config_name not in parallel_job_submission_info:
                # raise JobError(
                    #"The parallel job can not be submitted because the "
                    #"parallel configuration %s is missing."
                    #% (parallel_config_name))
                # potential bug should configuration_name be
                # parallel_config_name

        def map_and_register(file, mode=None, addTo=[]):
            '''
            Helper function to register SpecialPath objects and map them
            to their engine representation.

            Args:
                file (:obj:`list` or :obj:`tuple` or :obj:`str` or
                    :obj:`SpecialPath`): File, command or command elements
                    that should be analyzed and registered.
                mode (str): If "Command", checks that FileTransfer and
                    TemporaryPath objects were referenced in
                    self.referenced_input_files or
                    self.referenced_output_files.
                            If "File", and `file` is a string, convert it to
                    its abspath value.
                addTo (:obj:`list` of :obj:`str`): If `addTo` contains "Input",
                    and `file` is a SpecialPath, it will be added to
                    self.referenced_input_files. If `addTo` contains "Output",
                    it will be added to self.referenced_output_files.
            '''
            if isinstance(file, tuple) or isinstance(file, list):
                for f in file:
                    map_and_register(f)
                return
            if file:
                if isinstance(file, OptionPath):
                    if not file in self.path_mapping:
                        self.path_mapping[file] = EngineOptionPath(
                            file, self.transfer_mapping, self.path_translation)
                        true_file = file.parent_path
                    else:
                        return
                else:
                    true_file = file
                if isinstance(true_file, TemporaryPath) \
                        or isinstance(true_file, FileTransfer):
                    if not true_file in self.transfer_mapping:
                        if mode == "Command" and \
                           not true_file in self.referenced_input_files and \
                           not true_file in self.referenced_output_files:
                            raise JobError("FileTransfer and TemporaryPath objets used in the "
                                           "command must be declared in the Job "
                                           "attributes: referenced_input_files "
                                           "and referenced_output_files.")
                        engine = None
                        if isinstance(true_file, EngineTransfer) or isinstance(true_file, EngineTemporaryPath):
                            engine = true_file
                        elif isinstance(true_file, FileTransfer):
                            engine = EngineTransfer(true_file)
                        elif isinstance(true_file, TemporaryPath):
                            engine = get_EngineTemporaryPath(true_file)
                        self.transfer_mapping[true_file] = engine
                    if "Input" in addTo and not true_file in self.referenced_input_files:
                        self.referenced_input_files.append(true_file)
                    if "Output" in addTo and not true_file in self.referenced_output_files:
                        self.referenced_output_files.append(true_file)
                    if not true_file in self.path_mapping:
                        self.path_mapping[
                            true_file] = self.transfer_mapping[true_file]
                elif isinstance(true_file, SharedResourcePath) \
                        and not true_file in self.path_mapping:
                    self.path_mapping[true_file] \
                        = EngineSharedResourcePath(
                            true_file, path_translation=self.path_translation)
                else:
                    if six.PY3 and isinstance(true_file, bytes):
                        true_file = true_file.decode('utf-8')
                    if not isinstance(true_file, six.string_types):
                        return
                        # raise JobError(
                            #"Wrong argument type in job %s: %s\ncommand:\n%s"
                            #% (self.name, repr(true_file), repr(self.command)))
                    if mode == "File":
                        true_file = os.path.abspath(true_file)
                # if not isinstance(file, OptionPath):
                    # file = true_file

        map_and_register(self.stdin, mode="File", addTo=["Input"])
        map_and_register(
            self.working_directory, mode="File", addTo=["Input", "Output"])
        map_and_register(self.stdout_file, mode="File", addTo=["Output"])
        map_and_register(self.stderr_file, mode="File", addTo=["Output"])
        if self.use_input_params_file:
            map_and_register(self.input_params_file, mode="File",
                             addTo=["Input"])
        if self.has_outputs:
            map_and_register(self.output_params_file, mode="File",
                             addTo=["Output"])
        for ft in self.referenced_input_files:
            map_and_register(ft)
        for ft in self.referenced_output_files:
            map_and_register(ft)
        map_and_register(self.command, mode="Command")
        for param, item in self.param_dict.items():
            map_and_register(item)

    def generate_command(self, command, mode=None, level=0):
        '''
        This function should only be added after all SpecialPath were
        registered. It generates an adequate server representation of
        the provided command or file.

        Parameters
        ----------
        command : :obj:`list` or :obj:`tuple` or :obj:`str` or:obj:`SpecialPath`
            File, command or command elements that should be converted to an
            adequate string representation.
        mode : str
            * if "Command": return the command as a list of strings
            * If "Tuple", only the path to the directory is returned. Indeed,
            tuples are used to provide a FileTransfer directory and a filename.
            The two must then be concatenated.
            * if "PathOnly", only special path replacements are done, the
            remaining of parameters are left as they are (lists, tuples etc).
            * if None, ``command`` it will be converted
            to a string representation (i.e., the output list will be quoted).
        '''
        if command is None:
            new_command = command
        elif isinstance(command, tuple) and len(command) == 2 \
                and isinstance(command[0], SpecialPath) \
                and isinstance(command[1], (six.string_types, bytes)):
            # If the entry si a tuple, we use 'first' to recover the directory
            # and 'second' to get the filename
            c1 = command[1]
            if six.PY3 and isinstance(c1, bytes):
                c1 = c1.decode()
            new_command = os.path.join(
                self.generate_command(command[0], mode="Tuple"), c1)
        elif isinstance(command, (list, tuple)):
            # If the entry is a list, we convert all its elements. If the
            # parent call was done on the full command (mode=="Command"),
            # all children lists should be converted to string representations
            new_command = []
            for c in command:
                item = self.generate_command(c, mode=mode, level=level + 1)
                if level == 0 and isinstance(item, (list, tuple)) \
                        and mode == 'Command':
                    # commandline elements should be strings
                    item = repr(item)
                new_command.append(item)
            if len(new_command) != 0 and new_command[0] == '<join>':
                new_command = ''.join(new_command[1:])
            if isinstance(command, tuple):
                new_command = tuple(new_command)
            elif isinstance(command, set):
                new_command = set(new_command)
            if mode is None:
                new_command = repr(new_command).replace("'", "\"")
                if six.PY2:
                    new_command = new_command.encode('utf-8')
        elif isinstance(command, SpecialPath):
            # If the entry is a SpecialPath, it is converted into the
            # corresponding path representation. If the parent call cas
            # done on a tuple (mode=="Tuple"), we only recover the directory path
            # (get_engine_path), else we get the path to the main file
            # (get_engine_main_path)
            if mode == "Tuple":
                new_command = (
                    command.pattern
                    % self.path_mapping[command].get_engine_path())
                new_command = six.ensure_str(new_command, 'utf-8')
            else:
                new_command = (
                    command.pattern
                    % self.path_mapping[command].get_engine_main_path())
                new_command = six.ensure_str(new_command, 'utf-8')
        else:
            # If the entry is anything else, we return its string
            # representation
            if mode not in ('Command', 'PathOnly'):
                new_command = six.text_type(command)
            else:
                new_command = command
        return new_command

    @staticmethod
    def escape_quotes(line):
        return line.replace('"', '\\"')

    def plain_command(self):
        '''
        Compute the actual job command (sequence of string) from the command
        holding FileTransfer and SharedResourcePath objects.

        returns: sequence of string
        '''
        if self.container_command is not None:
            replaced = [i for i in range(len(self.container_command))
                        if '{#command}' in self.container_command[i]]
            if len(replaced) == 0:
                command = self.container_command + self.command
            else:
                user_command = self.generate_command(self.command,
                                                     mode="Command")
                for i, item in enumerate(user_command):
                    if isinstance(item, list):
                        user_command[i] = ''.join(item)
                user_command = [self.escape_quotes(item)
                                for item in user_command]
                user_command = '"' + '" "'.join(user_command) + '"'
                command = list(self.container_command)
                for i in replaced:
                    command[i] \
                        = self.container_command[i].replace('{#command}',
                                                            user_command)
                return command  # no need to replace again
        else:
            command = self.command
        repl_command = self.commandline_repl(
            self.generate_command(command, mode="Command"))
        # re-go through generate_command since repl_command leaves SpecialPath
        # instances
        res_command = [self.generate_command(x, mode='Command')
                       for x in repl_command]
        return res_command

    def plain_stdin(self):
        return self.generate_command(self.stdin)

    def plain_stdout(self):
        return self.generate_command(self.stdout_file)

    def plain_stderr(self):
        return self.generate_command(self.stderr_file)

    def plain_input_params_file(self):
        return self.generate_command(self.input_params_file)

    def plain_output_params_file(self):
        return self.generate_command(self.output_params_file)

    def plain_working_directory(self):
        return self.generate_command(self.working_directory)

    def write_input_params_file(self):
        if self.use_input_params_file and self.input_params_file:
            params = {}  # dict(self.param_dict)
            param_dict = {'parameters': params}
            for param, value in six.iteritems(self.param_dict):
                params[param] = self.generate_command(value, mode='PathOnly')
            # include config
            if self.configuration:
                param_dict['configuration_dict'] = self.configuration
            with open(self.input_params_file, 'w') as f:
                json.dump(utils.to_json(param_dict), f)

    def is_running(self):
        running = self.status != constants.NOT_SUBMITTED and \
            self.status != constants.FAILED and \
            self.status != constants.DONE
        return running

    def is_done(self):
        done = self.status == constants.DONE or self.status == constants.FAILED
        return done

    def failed(self):
        failed = (self.is_done() and
                  ((self.exit_value != 0 and self.exit_value != None) or
                   self.exit_status != constants.FINISHED_REGULARLY or
                   self.terminating_signal != None))
        return failed

    def ended_with_success(self):
        success = self.is_done() and \
            self.exit_value == 0 and \
            self.exit_status == constants.FINISHED_REGULARLY and \
            self.terminating_signal == None
        return success

    def engine_execution(self):
        func = getattr(self.job_class, 'engine_execution', None)
        if func:
            return func(self.job_class, self)


class EngineWorkflow(Workflow):

    '''
    Server side representation of a :obj:`Workflow`, i.e. a list of jobs
    with groups and dependencies.
    '''

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

    # Once registered on the database server each
    # TemporaryPath has an id.
    # dictonary: tr_id -> EngineTemporaryPath
    registered_tmp = None

    # docker / singularity prefix command, to be prepended to all jobs
    # commandlines
    container_command = None

    # for each job: list of all the jobs which have to end before a job can start
    # dictionary: job_id -> list of job id
    _dependency_dict = None

    # A workflow object. For serialisation purposes with serpent
    _client_workflow = None

    logger = None

    def to_dict(self):
        wf_dict = super(EngineWorkflow, self).to_dict()

        # path_translation
        # queue
        # expiration_date
        # container_command
        wf_dict["container_command"] = self.container_command

        return wf_dict

    @classmethod
    def from_dict(cls, d):
        client_workflow = Workflow.from_dict(d)

        # path_translation
        path_translation = {}

        # queue
        queue = d.get('queue', None)

        # expiration_date
        expiration_date = d.get('expiration_date')
        if expiration_date is not None:
            expiration_date = datetime.datetime(*expiration_date)

        # name
        name = client_workflow.name

        # container_command
        container_command = d.get('container_command')

        return cls(client_workflow, path_translation, queue, expiration_date,
                   name, container_command=container_command)

    class WorkflowCache(object):

        def __init__(self):
            self.waiting_jobs = set()
            self.to_run = set()
            self.dependencies = {}
            self.done = set()
            self.running = set()
            self.to_abort = set()
            self.has_new_failed_jobs = False

    def __init__(self,
                 client_workflow,
                 path_translation,
                 queue,
                 expiration_date,
                 name,
                 container_command=None):
        logging.debug("Within Engine workflow constructor")

        super(EngineWorkflow, self).__init__(
            client_workflow.jobs,
            client_workflow.dependencies,
            client_workflow.root_group,
            env=client_workflow.env,
            env_builder_code=client_workflow.env_builder_code,
            param_links=client_workflow.param_links)
            # STRANGE: does not match Workflow constructor,
            # ,client_workflow.groups)
        self.wf_id = -1

        logging.debug("After call to parent constructor, if we change the "
                      "prototype of the constructor suppressing the "
                      "last parametre nothing seem to happen, see comment above")
        self.status = constants.WORKFLOW_NOT_STARTED
        self._path_translation = path_translation
        self.queue = queue
        self.expiration_date = expiration_date
        self.name = name

        self.user_storage = client_workflow.user_storage
        if hasattr(client_workflow, 'uuid'):
            self.uuid = client_workflow.uuid

        self._dependency_dict = {}
        self._jobs_with_downstream = set()
        # reshape dependencies as a dict dependent_job: [upstream_job, ...]
        # and set of jobs whith downstream dependencies
        for dep in self.dependencies:
            self._dependency_dict.setdefault(dep[1], []).append(dep[0])
            self._jobs_with_downstream.add(dep[0])

        self.job_mapping = {}
        self.transfer_mapping = {}
        self.container_command = container_command
        self._map()

        self.registered_tr = {}
        self.registered_tmp = {}
        self.registered_jobs = {}

        self.cache = None
        # begin without cache because it also has an overhead
        self.use_cache = False

    def get_environ(self):
        ''' Get environment variables dict for the workflow. This environment
        is applied to all engine jobs (and can be specialized on a per-job
        basis if jobs also have an env variable).

        Env variables are built from the env variable of the workflow, and by
        the result of execution of the env_builder_code source code.
        '''
        env = {}
        if self.env_builder_code:
            t = tempfile.mkstemp(prefix='swf_', suffix='.py')
            try:
                os.close(t[0])
                with io.open(t[1], 'w', encoding='utf-8') as f:
                    f.write(six.ensure_text(self.env_builder_code))
                    f.write(u'\n')
                try:
                    env_json = subprocess.check_output([sys.executable,
                                                        t[1]]).decode('utf-8')
                    env = json.loads(env_json)
                except Exception as e:
                    logging.error(
                        'workflow env_builder_code could not be executed:\n' + repr(e) + '\ncode:\n' + self.env_builder_code)
            finally:
                os.unlink(t[1])
        if self.env:
            env.update(self.env)
        return env

    def _map(self):
        '''
        Fill the job_mapping attributes.
        + type checking
        '''
        # get workflow environment variables
        env = self.get_environ()

        # jobs
        for job in self.jobs:
            if not isinstance(job, Job):
                raise WorkflowError("%s: Wrong type in the jobs attribute. "
                                    " An object of type Job is required." % (repr(job)))
            if job not in self.job_mapping:
                ejob = EngineJob(client_job=job,
                                 queue=self.queue,
                                 path_translation=self._path_translation,
                                 transfer_mapping=self.transfer_mapping,
                                 container_command=self.container_command,
                                 wf_env=env)
                self.transfer_mapping.update(ejob.transfer_mapping)
                self.job_mapping[job] = ejob

        # dependencies
        for dependency in self.dependencies:
            if not isinstance(dependency[0], Job) or \
               not isinstance(dependency[1], Job):
                raise WorkflowError("%s, %s: Wrong type in the workflow dependencies."
                                    " An object of type Job is required." %
                                    (repr(dependency[0]), repr(dependency[1])))

            if dependency[0] not in self.job_mapping:
                self.jobs.append(dependency[0])
                ejob = EngineJob(client_job=dependency[0],
                                 queue=self.queue,
                                 path_translation=self._path_translation,
                                 transfer_mapping=self.transfer_mapping,
                                 container_command=self.container_command,
                                 wf_env=env)
                self.transfer_mapping.update(ejob.transfer_mapping)
                self.job_mapping[dependency[0]] = ejob

            if dependency[1] not in self.job_mapping:
                self.jobs.append(dependency[1])
                ejob = EngineJob(client_job=dependency[1],
                                 queue=self.queue,
                                 path_translation=self._path_translation,
                                 transfer_mapping=self.transfer_mapping,
                                 container_command=self.container_command,
                                 wf_env=env)
                self.transfer_mapping.update(ejob.transfer_mapping)
                self.job_mapping[dependency[1]] = ejob

        # groups
        groups = self.groups
        for group in self.groups:
            for elem in group.elements:
                if isinstance(elem, Job):
                    if elem not in self.job_mapping:
                        self.jobs.append(elem)
                        ejob = EngineJob(
                            client_job=elem,
                            queue=self.queue,
                            path_translation=self._path_translation,
                            transfer_mapping=self.transfer_mapping,
                            container_command=self.container_command,
                            wf_env=env)
                        self.transfer_mapping.update(ejob.transfer_mapping)
                        self.job_mapping[elem] = ejob
                elif not isinstance(elem, Group):
                    raise WorkflowError("%s: Wrong type in the workflow "
                                        "groups. Objects of type Job or "
                                        "Group are required. Got type: %s"
                                        % (repr(elem), type(elem).__name__))

        # root group
        for elem in self.root_group:
            if isinstance(elem, Job):
                if elem not in self.job_mapping:
                    self.jobs.append(elem)
                    ejob = EngineJob(client_job=elem,
                                     queue=self.queue,
                                     path_translation=self._path_translation,
                                     transfer_mapping=self.transfer_mapping,
                                     container_command=self.container_command,
                                     wf_env=env)
                    self.transfer_mapping.update(ejob.transfer_mapping)
                    self.job_mapping[elem] = ejob
            elif not isinstance(elem, Group):
                raise WorkflowError(
                    "%s: Wrong type in the workflow root_group."
                      " Objects of type Job or Group are required." %
                      (repr(elem)))

        for client_job, job in self.job_mapping.items():
            job.signal_end = (client_job in self._jobs_with_downstream)

    def find_out_independant_jobs(self):
        independant_jobs = []
        for job in self.jobs:
            to_run = True
            for ft in job.referenced_input_files:
                if not self.transfer_mapping[ft].files_exist_on_server():
                    if self.transfer_mapping[ft].status \
                            == constants.TRANSFERING_FROM_CR_TO_CLIENT:
                        # TBI stop the transfer
                        pass
                    to_run = False
                    break
            if to_run:
                deps = self._dependency_dict.get(job)
                if deps is not None and len(deps) != 0:
                    to_run = False
            if to_run:
                independant_jobs.append(self.job_mapping[job])
        if independant_jobs:
            status = constants.WORKFLOW_IN_PROGRESS
        elif len(self.jobs) != 0:
            status = self.status
        else:
            status = constants.WORKFLOW_DONE
        return (independant_jobs, status)

    def find_out_jobs_to_process(self):
        '''
        Workflow exploration to find out new node to process.

        @rtype: tuple (sequence of EngineJob,
                       sequence of EngineJob,
                       constants.WORKFLOW_STATUS)
        @return: (jobs to run,
                  ended jobs
                  workflow status)
        '''

        if not self.use_cache:
            return self.find_out_jobs_to_process_nocache()

        self.logger = logging.getLogger('engine.EngineWorkflow')
        self.logger.debug("self.jobs=" + repr(self.jobs))
        if self.cache is None:
            self.cache = EngineWorkflow.WorkflowCache()
            self.cache.waiting_jobs = set(self.jobs)
            self.cache.dependencies = dict(
                (k, set(v)) for k, v in six.iteritems(self._dependency_dict))
            self.cache.has_new_failed_jobs = False
        cache = self.cache
        to_run = set()
        to_abort = set()
        done = set()
        running = set()
        rmap = {}
        # jcount = 0
        # dcount = 0
        # fcount = 0
        has_failed_jobs = self.cache.has_new_failed_jobs
        self.cache.has_new_failed_jobs = False
        # import time
        # t0 = time.perf_counter()
        for client_job in cache.waiting_jobs:
            self.logger.debug("client_job=" + repr(client_job))
            job = self.job_mapping[client_job]
            # jcount += 1
            if job.is_done():
                done.add(job)
                rmap[job] = client_job
                if job.failed():
                    has_failed_jobs = True
                    self.has_new_failed_jobs = True
            elif job.is_running():
                running.add(job)
                rmap[job] = client_job
            elif job.status == constants.NOT_SUBMITTED:
                job_to_run = True
                job_to_abort = False
                for ft in job.referenced_input_files:
                    # fcount += 1
                    eft = job.transfer_mapping[ft]
                    if not eft.files_exist_on_server():
                        if eft.status == constants.TRANSFERING_FROM_CR_TO_CLIENT:
                        # TBI stop the transfer
                            pass
                        job_to_run = False
                        break
                deps = cache.dependencies.get(client_job)
                if deps is not None:
                    remove_deps = []
                    for dep_client_job in deps:
                        # dcount += 1
                        dep_job = self.job_mapping[dep_client_job]
                        if not dep_job.ended_with_success():
                            job_to_run = False
                            if dep_job.failed():
                                job_to_abort = True
                                has_failed_jobs = True
                                self.has_new_failed_jobs = True
                                break
                            if not has_failed_jobs:
                                # if no new failed jobs have been encountered,
                                # no need to scan every dep to propagate
                                # abort.
                                break
                        else:
                            remove_deps.append(dep_client_job)
                        # TO DO to abort
                    for dep_client_job in remove_deps:
                        deps.remove(dep_client_job)
                if job_to_run:
                    to_run.add(job)
                if job_to_abort:
                    to_abort.add(job)
                if job_to_run or job_to_abort:
                    rmap[job] = client_job

        # update status of former cache
        to_remove = []
        for job in cache.to_run:
            if job.is_done():
                done.add(job)
                to_remove.append(job)
            elif job.is_running():
                running.add(job)
                to_remove.append(job)
        cache.to_run.difference_update(to_remove)
        to_remove = []
        for job in cache.running:
            if job.is_done():
                done.add(job)
                to_remove.append(job)
        cache.running.difference_update(to_remove)
        to_remove = []
        for job in cache.to_abort:
            if job.is_done():
                done.add(job)
                to_remove.append(job)
        cache.to_abort.difference_update(to_remove)

        cache.waiting_jobs.difference_update(rmap.values())

        cache.to_run.update(to_run)
        cache.running.update(running)
        cache.to_abort.update(to_abort)
        to_run = cache.to_run
        running = cache.running

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
                job.exit_status = constants.EXIT_NOTRUN
                # remove job from to_run and running sets otherwise the
                # workflow status could be wrong
                to_run.discard(job)
                running.discard(job)

        # counts
        to_run.difference_update(done)
        to_abort.difference_update(done)
        running.difference_update(done)
        cache.done.update(done)
        done = cache.done

        if len(running) + len(to_run) > 0:
            status = constants.WORKFLOW_IN_PROGRESS
        elif len(done) + len(to_abort) == len(self.jobs):
            status = constants.WORKFLOW_DONE
        elif len(done) > 0:
            # set it to DONE to avoid hangout
            status = constants.WORKFLOW_DONE
            # !!!! the workflow may be stuck !!!!
            # TBI
            self.logger.error("!!!! The workflow status is not clear. "
                              "Stopping it !!!!")
            self.logger.error(
                "total jobs: %d, done/aborted: %d, to abort: %d, running: %d, "
                "to run: %d"
                % (len(self.jobs), len(done), len(to_abort), len(running),
                   len(to_run)))
        else:
            status = constants.WORKFLOW_NOT_STARTED

        # t1 = time.perf_counter()
        # print('jcount:', jcount, ', dcount:', dcount, ', fcount:', fcount, ',
        # time:', t1 - t0, ', to_run:', len(to_run), ', ended:',
        # len(ended_jobs), ', done:', len(done), ', running:', len(running))

        return (list(to_run), ended_jobs, status)

    def find_out_jobs_to_process_nocache(self):
        '''
        Workflow exploration to find out new node to process.

        @rtype: tuple (sequence of EngineJob,
                       sequence of EngineJob,
                       constants.WORKFLOW_STATUS)
        @return: (jobs to run,
                  ended jobs
                  workflow status)
        '''

        self.logger = logging.getLogger('engine.EngineWorkflow')
        self.logger.debug("self.jobs=" + repr(self.jobs))
        to_run = set()
        to_abort = set()
        done = []
        running = set()
        # jcount = 0
        # dcount = 0
        # fcount = 0
        j_to_discard = 0
        # d_to_discard = 0
        # f_to_discard = 0
        # has_failed_jobs = getattr(self, 'has_new_failed_jobs', False)
        # self.has_new_failed_jobs = False
        # t0 = time.perf_counter()
        for client_job in self.jobs:
            # jcount += 1
            self.logger.debug("client_job=" + repr(client_job))
            job = self.job_mapping[client_job]
            if job.is_done():
                done.append(job)
                j_to_discard += 1
            elif job.is_running():
                running.add(job)
                j_to_discard += 1
            elif job.status == constants.NOT_SUBMITTED:
                job_to_run = True
                job_to_abort = False
                self.logger.debug(
                    "job not submitted, referenced_input_files: " + repr(job.referenced_input_files))
                for ft in job.referenced_input_files:
                    # fcount += 1
                    eft = job.transfer_mapping[ft]
                    if not eft.files_exist_on_server():
                        if eft.status \
                                == constants.TRANSFERING_FROM_CR_TO_CLIENT:
                            # TBI stop the transfer
                            pass
                        self.logger.debug("Transfer not complete: %s / %s"
                                          % (eft,  eft.engine_path)
                                          + ', status: ' + repr(eft.status))
                        job_to_run = False
                        break
                self.logger.debug("job_to_run: " + repr(job_to_run))
                if client_job in self._dependency_dict:
                    for dep_client_job in self._dependency_dict[client_job]:
                        # dcount += 1
                        dep_job = self.job_mapping[dep_client_job]
                        if not dep_job.ended_with_success():
                            job_to_run = False
                            if dep_job.failed():
                                job_to_abort = True
                                break
                        # else: d_to_discard += 1
                        # TO DO to abort
                if job_to_run:
                    to_run.add(job)
                    j_to_discard += 1
                if job_to_abort:
                    j_to_discard += 1
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
                    j_to_discard += 1
                    break

        # stop the whole branch
        ended_jobs = {}
        for job in to_abort:
            if job.job_id and job.status != constants.FAILED:
                self.logger.debug("  ---- Failure: job to abort " + job.name)
                assert(job.status == constants.NOT_SUBMITTED)
                ended_jobs[job.job_id] = job
                job.status = constants.FAILED
                job.exit_status = constants.EXIT_NOTRUN
                # remove job from to_run and running sets otherwise the
                # workflow status could be wrong
                to_run.discard(job)
                running.discard(job)

        if len(running) + len(to_run) > 0:
            status = constants.WORKFLOW_IN_PROGRESS
        elif len(done) + len(to_abort) == len(self.jobs):
            status = constants.WORKFLOW_DONE
        elif len(done) > 0:
            # set it to DONE to avoid hangout
            status = constants.WORKFLOW_DONE
            # !!!! the workflow may be stuck !!!!
            # TBI
            self.logger.error("!!!! The workflow status is not clear. "
                              "Stopping it !!!!")
            self.logger.error(
                "total jobs: %d, done/aborted: %d, to abort: %d, running: %d, "
                "to run: %d"
                % (len(self.jobs), len(done), len(to_abort), len(running),
                   len(to_run)))
        else:
            status = constants.WORKFLOW_NOT_STARTED

        # t1 = time.perf_counter()
        # print('jcount:', jcount, ', dcount:', dcount, ', time:', t1 - t0, ',
        # to_run:', len(to_run), ', done:', len(done), ', running:',
        # len(running), 'j_to_discard:', j_to_discard, ', d_to_discard:',
        # d_to_discard)
        if j_to_discard >= 2000:
            self.use_cache = True
            self.logger.debug('enabling cache.')

        return (list(to_run), ended_jobs, status)

    def _update_state_from_database_server(self, database_server):
        wf_status = database_server.get_detailed_workflow_status(
            self.wf_id, with_drms_id=True)

        for job_info in wf_status[0]:
            job_id, status, queue, exit_info, date_info, drmaa_id = job_info
            self.registered_jobs[job_id].status = status
            exit_status, exit_value, term_signal, resource_usage = exit_info
            self.registered_jobs[job_id].exit_status = exit_status
            self.registered_jobs[job_id].exit_value = exit_value
            self.registered_jobs[job_id].str_rusage = resource_usage
            self.registered_jobs[job_id].terminating_signal = term_signal
            self.registered_jobs[job_id].drmaa_id = drmaa_id

        for ft_info in wf_status[1]:
            (transfer_id,
             engine_path,
             client_path,
             client_paths,
             status,
             transfer_type) = ft_info
            engine_transfer = self.registered_tr[transfer_id]
            engine_transfer.status = status
            engine_transfer.engine_path = engine_path

        self.queue = wf_status[3]

        for ft_info in wf_status[4]:
            (temp_path_id,
             engine_path,
             status) = ft_info
            engine_temp = self.registered_tmp[temp_path_id]
            engine_temp.status = status

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
                # what's this for ?
                #stdout = open(job.stdout_file, "w")
                #stdout.close()
                #stderr = open(job.stderr_file, "w")
                #stderr.close()
                new_status[job.job_id] = constants.FAILED
                new_exit_info[job.job_id] = job

        database_server.set_jobs_status(new_status)
        database_server.set_jobs_exit_info(new_exit_info)
        database_server.set_workflow_status(self.wf_id, self.status)

    def restart(self, database_server, queue):

        self._update_state_from_database_server(database_server)

        self.queue = queue
        undone_jobs = []
        sub_info_to_resert = {}
        new_status = {}
        jobs_queue_changed = []
        self.cache = None
        for client_job in self.jobs:
            job = self.job_mapping[client_job]
            undone = False
            if job.failed():
                # clear all the information related to the previous job
                # submission
                job.status = constants.NOT_SUBMITTED
                job.exit_status = None
                job.exit_value = None
                job.terminating_signal = None
                job.drmaa_id = None
                job.queue = self.queue
                jobs_queue_changed.append(job.job_id)
                os.makedirs(os.path.dirname(job.stdout_file), exist_ok=True)
                stdout = open(job.stdout_file, "w")
                stdout.close()
                os.makedirs(os.path.dirname(job.stderr_file), exist_ok=True)
                stderr = open(job.stderr_file, "w")
                stderr.close()

                sub_info_to_resert[job.job_id] = None
                new_status[job.job_id] = constants.NOT_SUBMITTED
                undone = True

            if undone or (self.status != constants.WORKFLOW_IN_PROGRESS
                          and not job.ended_with_success()):
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
                job_to_run = True  # a node is run when all its dependencies succeed
                for ft in job.referenced_input_files:
                    eft = self.transfer_mapping[ft]
                    if not eft.files_exist_on_server():
                        if eft.status == constants.TRANSFERING_FROM_CR_TO_CLIENT:
                            # TBI stop the transfer
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

        if to_run or self.status == constants.WORKFLOW_IN_PROGRESS:
            status = constants.WORKFLOW_IN_PROGRESS
        else:
            status = constants.WORKFLOW_DONE

        return (to_run, status)

    def job_ids_which_can_rerun(self, job_ids):
        ''' Check jobs depending on jobs from the job_ids list, and include
        them in an extended list if they should re-run if job_ids are restarted.
        '''
        ext_job_ids = set(job_ids)
        to_test = []
        jobsdeps_f = {}
        jobsdeps_t = {}
        for dep in self.dependencies:
            j1 = self.job_mapping[dep[0]]  # get engine jobs
            j2 = self.job_mapping[dep[1]]
            # if j2.status == constants.FAILED \
                    # and j2.job_id not in ext_job_ids:
            if j2.job_id not in ext_job_ids:
                # dep[1] may change state
                jobsdeps_f.setdefault(j2, set()).add(j1)
                if j1.job_id in ext_job_ids \
                        and j2.job_id not in ext_job_ids:
                    to_test.append(j2)
            # if j1.status == constants.FAILED \
                    # and j1.job_id not in ext_job_ids:
            if j1.job_id not in ext_job_ids:
                jobsdeps_t.setdefault(j1, set()).add(j2)
        while to_test:
            job = to_test.pop(0)
            if job.job_id in ext_job_ids:
                continue
            if all([j.status != constants.FAILED or j.job_id in ext_job_ids
                    for j in jobsdeps_f[job]]):
                ext_job_ids.add(job.job_id)
                to_test += [j for j in jobsdeps_t.get(job, set())
                            if j not in to_test]
        return ext_job_ids

    def restart_jobs(self, database_server, job_ids, check_deps=True):
        ''' Restart jobs in a running workflow. Jobs should have been stopped
        previously, otherwise they will probably be duplicated and run
        concurrently. Dependent jobs can also be restarted.

        Parameters
        ----------
        database_server:
            database server object
        job_ids: list of int
            list of job_ids to be restarted
        check_deps: bool
            if True, dependent jobs are looked for and also reset to be
            restarted. They also should have been stopped previously.

        Returns
        -------
        jobs_to_run: list of EngineJob
            jobs which can re-run immediately. These jobs can be submitted
            directly in the engine.
        '''
        self._update_state_from_database_server(database_server)

        if check_deps:
            extended_job_ids = self.job_ids_which_can_rerun(job_ids)
        else:
            extended_job_ids = job_ids
        print('restart_jobs:', extended_job_ids)
        sub_info_to_resert = {}
        new_status = {}
        jobs_to_run = set()
        for client_job in self.jobs:
            job = self.job_mapping[client_job]
            job_id = job.job_id
            if job_id not in extended_job_ids:
                continue
            if job.status not in (constants.DONE, constants.FAILED):
                print('job', job_id, 'is not ready for restart:', job.status)
                continue

            jobs_to_run.add(job)
            # clear all the information related to the previous job
            # submission
            job.status = constants.NOT_SUBMITTED
            job.exit_status = None
            job.exit_value = None
            job.terminating_signal = None
            job.drmaa_id = None
            job.queue = self.queue
            stdout = open(job.stdout_file, "w")
            stdout.close()
            stderr = open(job.stderr_file, "w")
            stderr.close()

            sub_info_to_resert[job.job_id] = None
            new_status[job_id] = constants.NOT_SUBMITTED

        database_server.set_submission_information(sub_info_to_resert, None)
        database_server.set_jobs_status(new_status)

        extended_jobs = set(jobs_to_run)
        # look for jobs which can restart immediately (all deps met)
        for dep in self.dependencies:
            j1 = self.job_mapping[dep[0]]  # get engine jobs
            j2 = self.job_mapping[dep[1]]
            if j2 in jobs_to_run and j1.status != constants.DONE:
                jobs_to_run.remove(j2)

        return jobs_to_run


class EngineTransfer(FileTransfer):

    engine_path = None

    status = None

    disposal_timeout = None

    workflow_id = None

    transfer_id = None

    def __init__(self, client_file_transfer):

        exist_on_client = \
            client_file_transfer.initial_status == constants.FILES_ON_CLIENT
        super(EngineTransfer, self).__init__(exist_on_client,
                                             client_file_transfer.client_path,
                                             client_file_transfer.disposal_timeout,
                                             client_file_transfer.name,
                                             client_file_transfer.client_paths)

        self.status = self.initial_status
        self.pattern = client_file_transfer.pattern

        workflow_id = -1

    def files_exist_on_server(self):

        exist = self.status == constants.FILES_ON_CR or \
            self.status == constants.FILES_ON_CLIENT_AND_CR or \
            self.status == constants.TRANSFERING_FROM_CR_TO_CLIENT
        return exist

    def get_engine_path(self):
        return self.engine_path

    def set_engine_path(self, path, client_path=None, client_paths=None):
        '''
        Set engine path (when it is output by a job), and client path if they
        are given.
        '''
        self.engine_path = path
        if client_path is None:
            client_path = os.path.join(os.path.dirname(self.client_path),
                                       os.path.basename(path))
        self.client_path = client_path
        if client_paths is not None:
            self.client_paths = client_paths

    def map_client_path_to_engine(self, engine_path, param_dict):
        '''
        Try to generate a client path[s] from an output engine path in a
        consistent way with other job named parameters.
        '''
        eng_dir, rel_eng_path = os.path.split(engine_path)
        out_dir = param_dict.get('output_directory')
        if out_dir and isinstance(out_dir, FileTransfer):
            eng_dir = out_dir.engine_path
            out_dir = out_dir.client_path
            rel_eng_path = os.path.relpath(engine_path, eng_dir)

        if not out_dir:
            trans = None
            path = None
            for param, value in six.iteritems(param_dict):
                if not isinstance(value, FileTransfer) \
                        or not os.path.isabs(value.client_path):
                    continue
                if isinstance(value, FileTransfer):
                    if value._initial_status == constants.FILES_DO_NOT_EXIST:
                        # value is an output; use its client directory
                        trans = value
                        break
                    # value is not an output. Keep it if we don't find a better
                    # match
                    if trans is None:
                        trans = value
                elif path is None:
                    # at last take any path dirname
                    path = value
            if trans is not None:
                out_dir = os.path.dirname(trans.client_path)
            elif path is not None:
                out_dir = os.path.dirname(path)

        if out_dir:
            rdir = os.path.dirname(rel_eng_path)
            if rdir:
                out_dir = os.path.join(out_dir, rdir)
            client_path = os.path.join(out_dir, os.path.basename(rel_eng_path))
            if self.client_paths:
                client_paths = [os.path.join(out_dir, os.path.basename(p))
                                for p in self.client_paths]
            else:
                client_paths = None
            return (client_path, client_paths)
        return (None, None)

    def get_engine_main_path(self):
        ''' main file (translated client_path) name '''
        if self.client_paths:
            return os.path.join(self.get_engine_path(),
                                os.path.basename(self.client_path))
        else:
            return self.get_engine_path()

    def get_id(self):
        return self.transfer_id


class EngineTemporaryPath(TemporaryPath):

    engine_path = None

    disposal_timeout = None

    workflow_id = None

    temp_path_id = None

    # suffix = None

    # temporary_directory should be set according to configuration.
    # It will be set by engine.ConfiguredWorkflowEngine, which has access to
    # the config.
    temporary_directory = None

    def __init__(self, client_temporary_path):
        super(EngineTemporaryPath, self).__init__(
            is_directory=client_temporary_path.is_directory,
            disposal_timeout=client_temporary_path.disposal_timeout,
            name=client_temporary_path.name,
            suffix=client_temporary_path.suffix)
        self.status = constants.FILES_DO_NOT_EXIST
        self.pattern = client_temporary_path.pattern

    def files_exist_on_server(self):
        exist = self.status == constants.FILES_ON_CR
        return exist

    def mktemp(self):
        dir = self.temporary_directory
        if self.is_directory:
            path = tempfile.mkdtemp(dir=dir, suffix=self.suffix)
        else:
            tmp = tempfile.mkstemp(dir=dir, suffix=self.suffix)
            path = tmp[1]
            os.close(tmp[0])  # should we close the fd ?
        self.engine_path = path
        self.status = constants.FILES_ON_CR

    def get_engine_path(self):
        if self.engine_path is None:
            self.mktemp()
        return self.engine_path

    def get_engine_main_path(self):
        return self.get_engine_path()

    def get_id(self):
        return self.temp_path_id


_engine_temp_paths = weakref.WeakKeyDictionary()


def get_EngineTemporaryPath(client_temporary_path, insert=True):
    '''
    Create an EngineTemporaryPath from a client TemporaryPath, or return an existing one if it has already been created.
    '''
    global _engine_temp_paths
    etp = _engine_temp_paths.get(client_temporary_path)
    if etp is None and insert:
        etp = EngineTemporaryPath(client_temporary_path)
        _engine_temp_paths[client_temporary_path] = etp
    return etp


class EngineOptionPath(OptionPath):

    engine_parent_path = None

    def __init__(self, client_option_path, transfer_mapping=None, path_translation=None):
        super(EngineOptionPath, self).__init__(
            parent_path=client_option_path.parent_path,
            uri=client_option_path.uri,
            name=client_option_path.name)
        if transfer_mapping is None:
            transfer_mapping = {}
        if isinstance(client_option_path.parent_path, FileTransfer):
            if not client_option_path.parent_path in transfer_mapping:
                transfer_mapping[client_option_path.parent_path] = \
                    EngineTransfer(client_option_path.parent_path)
            self.engine_parent_path = transfer_mapping[
                client_option_path.parent_path]
            self.status = self.engine_parent_path.status
        elif isinstance(client_option_path.parent_path, TemporaryPath):
            if not client_option_path.parent_path in transfer_mapping:
                transfer_mapping[client_option_path.parent_path] = \
                    get_EngineTemporaryPath(client_option_path.parent_path)
            self.engine_parent_path = transfer_mapping[
                client_option_path.parent_path]
            self.status = self.engine_parent_path.status
        elif isinstance(client_option_path, SharedResourcePath):
            self.engine_parent_path = EngineSharedResourcePath(
                client_option_path.parent_path, path_translation)
            self.status = self.engine_parent_path.status
        else:
            self.engine_parent_path = client_option_path.parent_path
            self.status = constants.FILES_ON_CLIENT

    def files_exist_on_server(self):
        exist = self.status == constants.FILES_ON_CR
        return exist

    def get_engine_path(self):
        if isinstance(self.engine_parent_path, EngineTransfer):
            return str(self.engine_parent_path.get_engine_main_path()) + str(self.uri)
        elif isinstance(self.engine_parent_path, SpecialPath):
            return str(self.engine_parent_path.get_engine_path()) + str(self.uri)
        else:
            return str(self.engine_parent_path) + str(self.uri)

    def get_engine_main_path(self):
        return self.get_engine_path()

    def get_id(self):
        return self.get_engine_path()


class EngineSharedResourcePath(SharedResourcePath):

    translated_path = None
    path_translation = None

    def __init__(self, client_shared_path, path_translation):
        super(EngineSharedResourcePath, self).__init__(
            relative_path=client_shared_path.relative_path,
            namespace=client_shared_path.namespace,
            uuid=client_shared_path.uuid,
            disposal_timeout=client_shared_path.disposal_timeout)
        self.path_translation = path_translation
        self.translated_path = self._translate()

    def _translate(self):
        if not self.path_translation:
            raise JobError("Could not submit workflows or jobs with shared resource "
                           "path: no translation found in the configuration file.")
        if not self.namespace in self.path_translation.keys():
            raise JobError("Could not translate shared resource path. The "
                           "namespace %s is not configured." % (self.namespace))
        if not self.uuid in self.path_translation[self.namespace]:
            raise JobError("Could not translate shared resource path. The uuid %s "
                           "does not exist for the namespace %s." %
                           (self.uuid, self.namespace))
        translated_path = os.path.join(
            self.path_translation[self.namespace][self.uuid],
            self.relative_path)
        return translated_path

    def get_engine_path(self):
        if self.translated_path is None:
            translated_path = self._translate()
        return self.translated_path

    def get_engine_main_path(self):
        return self.get_engine_path()

    def get_id(self):
        return self.get_engine_path()
