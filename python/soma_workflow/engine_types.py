
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

import os
import logging
import tempfile
import weakref
import six
import time

from soma_workflow.errors import JobError, WorkflowError
import soma_workflow.constants as constants
from soma_workflow.client import Job, BarrierJob, SpecialPath, FileTransfer, \
    Workflow, SharedResourcePath, TemporaryPath, OptionPath, Group

# python 2/3 compatibility
import sys
if sys.version_info[0] >= 3:
    basestring = str
    unicode = str


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
    parallel_job_submission_info
        Configuration of the prallel submission.
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

    def __init__(self,
                 client_job,
                 queue,
                 workflow_id=-1,
                 path_translation=None,
                 transfer_mapping=None,
                 parallel_job_submission_info=None,
                 container_command=None):

        super(EngineJob, self).__init__(client_job.command,
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
                                        env = client_job.env)

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
        if isinstance(client_job, BarrierJob):
            self.is_barrier = True
        else:
            self.is_barrier = False

        self._map(parallel_job_submission_info)

    def _map(self, parallel_job_submission_info):
        '''
        Fill the transfer_mapping and srp_mapping attributes.
        + check the types of the Job arguments.
        '''
        if not self.command and not self.is_barrier:
            raise JobError("The command attribute is the only required "
                           "attribute of Job.")

        if self.parallel_job_info:
            parallel_config_name, max_node_number = self.parallel_job_info
            if not parallel_job_submission_info:
                raise JobError("No parallel information was registered for the "
                               " current resource. A parallel job can not be submitted")
            if parallel_config_name not in parallel_job_submission_info:
                raise JobError("The parallel job can not be submitted because the "
                               "parallel configuration %s is missing." % (configuration_name))

        def map_and_register(file, mode=None, addTo=[]):
            '''
            Helper function to register SpecialPath objects and map them
            to teir engine representation.

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
                        self.path_mapping[file] = EngineOptionPath(file, self.transfer_mapping, self.path_translation)
                        true_file = file.parent_path
                    else:
                        return
                else:
                    true_file = file
                if isinstance(true_file, TemporaryPath) or isinstance(true_file, FileTransfer):
                    if not true_file in self.transfer_mapping:
                        if mode=="Command" and \
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
                        self.path_mapping[true_file] = self.transfer_mapping[true_file]
                elif isinstance(true_file, SharedResourcePath) and not true_file in self.path_mapping:
                    self.path_mapping[true_file] = EngineSharedResourcePath(true_file, path_translation=self.path_translation)
                else:
                    if not isinstance(true_file, basestring):
                        raise JobError("Wrong type: %s" % (repr(true_file)))
                    if mode == "File":
                        true_file = os.path.abspath(true_file)
                if not isinstance(file, OptionPath):
                    file = true_file

        map_and_register(self.stdin, mode="File", addTo=["Input"])
        map_and_register(self.working_directory, mode="File", addTo=["Input", "Output"])
        map_and_register(self.stdout_file, mode="File", addTo=["Output"])
        map_and_register(self.stderr_file, mode="File", addTo=["Output"])
        for ft in self.referenced_input_files:
            map_and_register(ft)
        for ft in self.referenced_output_files:
            map_and_register(ft)
        map_and_register(self.command, mode="Command")


    def generate_command(self, command, mode=None):
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
            * If not "Command", and `command` is a list, it will be converted
            to a string representation (i.e., the output list will be quoted).
            * If "Tuple", only the path to the directory is returned. Indeed,
            tuples are used to provide a FileTransfer directory and a filename.
            The two must then be concatenated.
        '''
        if command is None:
            new_command = command
        elif isinstance(command, tuple):
            # If the entry si a tuple, we use 'first' to recover the directory
            # and 'second' to get the filename
            new_command = os.path.join(self.generate_command(command[0], mode="Tuple"), command[1])
        elif isinstance(command, list):
            # If the entry is a list, we convert all its elements. If the
            # parent call was done on the full command (mode=="Command"),
            # all children lists should be converted to string representations
            new_command = []
            for c in command:
                new_command += [self.generate_command(c)]
            if mode != "Command":
                new_command = str(repr(new_command)).replace("'", "\"")
        elif isinstance(command, SpecialPath):
            # If the entry is a SpecialPath, it is converted into the
            # corresponding path representation. If the parent call cas
            # done on a tuple (mode=="Tuple"), we only recover the directory path
            # (get_engine_path), else we get the path to the main file
            # (get_engine_main_path)
            if mode == "Tuple":
                new_command = command.pattern % self.path_mapping[command].get_engine_path()
            else:
                new_command = command.pattern % self.path_mapping[command].get_engine_main_path()
        else:
            # If the entry is anything else, we return its string representation
            new_command = str(command)
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
                user_command = [self.escape_quotes(item)
                                for item in user_command]
                user_command = '"' + '" "'.join(user_command) + '"'
                command = list(self.container_command)
                for i in replaced:
                    command[i] \
                        = self.container_command[i].replace('{#command}',
                                                            user_command)
                return command # no need to replace again
        else:
            command = self.command
        return self.generate_command(command, mode="Command")

    def plain_stdin(self):
        return self.generate_command(self.stdin)

    def plain_stdout(self):
        return self.generate_command(self.stdout_file)

    def plain_stderr(self):
        return self.generate_command(self.stderr_file)

    def plain_working_directory(self):
        return self.generate_command(self.working_directory)

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

    # docker / singularity prefix command, to be prepended to all jobs
    # commandlines
    container_command = None

    # for each job: list of all the jobs which have to end before a job can start
    # dictionary: job_id -> list of job id
    _dependency_dict = None

    logger = None

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
        self.container_command = container_command
        self._map()

        self.registered_tr = {}
        self.registered_jobs = {}

        self._dependency_dict = {}
        for dep in self.dependencies:
            if dep[1] in self._dependency_dict:
                self._dependency_dict[dep[1]].append(dep[0])
            else:
                self._dependency_dict[dep[1]] = [dep[0]]
        self.cache = None
        # begin without cache because it also has an overhead
        self.use_cache = False

    def _map(self):
        '''
        Fill the job_mapping attributes.
        + type checking
        '''
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
                                 container_command=self.container_command)
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
                                 container_command=self.container_command)
                self.transfer_mapping.update(ejob.transfer_mapping)
                self.job_mapping[dependency[0]] = ejob

            if dependency[1] not in self.job_mapping:
                self.jobs.append(dependency[1])
                ejob = EngineJob(client_job=dependency[1],
                                 queue=self.queue,
                                 path_translation=self._path_translation,
                                 transfer_mapping=self.transfer_mapping,
                                 container_command=self.container_command)
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
                            container_command=self.container_command)
                        self.transfer_mapping.update(ejob.transfer_mapping)
                        self.job_mapping[elem] = ejob
                elif not isinstance(elem, Group):
                    raise WorkflowError("%s: Wrong type in the workflow "
                                        "groups. Objects of type Job or "
                                        "Group are required." % (repr(elem)))

        # root group
        for elem in self.root_group:
            if isinstance(elem, Job):
                if elem not in self.job_mapping:
                    self.jobs.append(elem)
                    ejob = EngineJob(client_job=elem,
                                     queue=self.queue,
                                     path_translation=self._path_translation,
                                     transfer_mapping=self.transfer_mapping,
                                     container_command=self.container_command)
                    self.transfer_mapping.update(ejob.transfer_mapping)
                    self.job_mapping[elem] = ejob
            elif not isinstance(elem, Group):
                raise WorkflowError(
                      "%s: Wrong type in the workflow root_group."
                      " Objects of type Job or Group are required." %
                      (repr(elem)))

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
                       constanst.WORKFLOW_STATUS)
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
                (k, set(v)) for k, v in  six.iteritems(self._dependency_dict))
            self.cache.has_new_failed_jobs = False
        cache = self.cache
        to_run = set()
        to_abort = set()
        done = set()
        running = set()
        rmap = {}
        #jcount = 0
        #dcount = 0
        #fcount = 0
        has_failed_jobs = self.cache.has_new_failed_jobs
        self.cache.has_new_failed_jobs = False
        #import time
        #t0 = time.clock()
        for client_job in cache.waiting_jobs:
            self.logger.debug("client_job=" + repr(client_job))
            job = self.job_mapping[client_job]
            #jcount += 1
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
                    #fcount += 1
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
                        #dcount += 1
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

        #t1 = time.clock()
        #print('jcount:', jcount, ', dcount:', dcount, ', fcount:', fcount, ', time:', t1 - t0, ', to_run:', len(to_run), ', ended:', len(ended_jobs), ', done:', len(done), ', running:', len(running))

        return (list(to_run), ended_jobs, status)

    def find_out_jobs_to_process_nocache(self):
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
        self.logger.debug("self.jobs=" + repr(self.jobs))
        to_run = set()
        to_abort = set()
        done = []
        running = set()
        #jcount = 0
        #dcount = 0
        #fcount = 0
        j_to_discard = 0
        #d_to_discard = 0
        #f_to_discard = 0
        #has_failed_jobs = getattr(self, 'has_new_failed_jobs', False)
        #self.has_new_failed_jobs = False
        t0 = time.clock()
        for client_job in self.jobs:
            #jcount += 1
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
                for ft in job.referenced_input_files:
                    #fcount += 1
                    eft = job.transfer_mapping[ft]
                    if not eft.files_exist_on_server():
                        if eft.status \
                                == constants.TRANSFERING_FROM_CR_TO_CLIENT:
                            # TBI stop the transfer
                            pass
                        job_to_run = False
                        break
                if client_job in self._dependency_dict:
                    for dep_client_job in self._dependency_dict[client_job]:
                        #dcount += 1
                        dep_job = self.job_mapping[dep_client_job]
                        if not dep_job.ended_with_success():
                            job_to_run = False
                            if dep_job.failed():
                                job_to_abort = True
                                break
                        #else: d_to_discard += 1
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

        #t1 = time.clock()
        #print('jcount:', jcount, ', dcount:', dcount, ', time:', t1 - t0, ', to_run:', len(to_run), ', done:', len(done), ', running:', len(running), 'j_to_discard:', j_to_discard, ', d_to_discard:', d_to_discard)
        if j_to_discard >= 2000:
            self.use_cache = True
            self.logger.debug('enabling cache.')

        return (list(to_run), ended_jobs, status)

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
        self.cache = None
        for client_job in self.jobs:
            job = self.job_mapping[client_job]
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

    def get_engine_main_path(self):
        ''' main file (translated client_path) name '''
        if self.client_paths:
            return os.path.join(self.get_engine_path(),
                                os.path.basename(self.client_path))
        else:
            return self.get_engine_path()

    def get_id(self):
        return self.engine_path


class EngineTemporaryPath(TemporaryPath):

    engine_path = None

    disposal_timeout = None

    workflow_id = None

    temp_path_id = None

    #suffix = None

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


def get_EngineTemporaryPath(client_temporary_path):
    '''
    Create an EngineTemporaryPath from a client TemporaryPath, or return an existing one if it has already been created.
    '''
    global _engine_temp_paths
    etp = _engine_temp_paths.get(client_temporary_path)
    if etp is None:
        etp = EngineTemporaryPath(client_temporary_path)
        _engine_temp_paths[client_temporary_path] = etp
    return etp


class EngineOptionPath(OptionPath):

    engine_parent_path = None

    def __init__(self, client_option_path, transfer_mapping = None, path_translation = None ):
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
            self.engine_parent_path = transfer_mapping[client_option_path.parent_path]
            self.status = self.engine_parent_path.status
        elif isinstance(client_option_path.parent_path, TemporaryPath):
            if not client_option_path.parent_path in transfer_mapping:
                transfer_mapping[client_option_path.parent_path] = \
                    get_EngineTemporaryPath(client_option_path.parent_path)
            self.engine_parent_path = transfer_mapping[client_option_path.parent_path]
            self.status = self.engine_parent_path.status
        elif isinstance(client_option_path, SharedResourcePath):
            self.engine_parent_path = EngineSharedResourcePath(client_option_path.parent_path, path_translation)
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
