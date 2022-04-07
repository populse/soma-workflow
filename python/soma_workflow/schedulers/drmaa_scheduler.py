# -*- coding: utf-8 -*-

from __future__ import with_statement, print_function
from __future__ import absolute_import

'''
author: Soizic Laguitton

organization: I2BM, Neurospin, Gif-sur-Yvette, France
organization: CATI, France
organization: IFR 49

license:CeCILL version 2, http://www.cecill.info/licences/Licence_CeCILL_V2-en.html
'''

from ..scheduler import Scheduler
import sys
import logging
import os
import socket
import six
import soma_workflow.constants as constants
from soma_workflow.errors import DRMError
from soma_workflow.configuration import Configuration
from soma_workflow.utils import DetectFindLib


_drmaa_lib_env_name = 'DRMAA_LIBRARY_PATH'

try:
    (DRMAA_LIB_FOUND, _lib) = DetectFindLib(_drmaa_lib_env_name, 'drmaa')
except Exception:
    # an exception occurs when drmaa lib is detected but cannot be loaded
    # because of a failed dependency (torque, grid engine etc)
    print("detection of the DRMAA library failed")
    DRMAA_LIB_FOUND = False


if DRMAA_LIB_FOUND:
    from somadrmaa.errors import *
    from somadrmaa.const import JobControlAction

    class DrmaaScheduler(Scheduler):

        '''
        Scheduling using a Drmaa session.
        Contains possible patch depending on the DRMAA impementation.
        '''

        # DRMAA session. DrmaaJobs
        _drmaa = None
        # string
        _drmaa_implementation = None
        # DRMAA doesn't provide an unified way of submitting
        # parallel jobs. The value of parallel_job_submission is cluster dependant.
        # The keys are:
        #      -Drmaa job template attributes
        #      -parallel configuration name as defined in soma_workflow.constants
        # dict
        parallel_job_submission_info = None

        logger = None

        _configured_native_spec = None

        tmp_file_path = None

        is_sleeping = False
        FAKE_JOB = -167

        def __init__(self,
                     drmaa_implementation,
                     parallel_job_submission_info,
                     tmp_file_path=None,
                     configured_native_spec=None):

            super(DrmaaScheduler, self).__init__()

            import somadrmaa

            self.logger = logging.getLogger('ljp.drmaajs')

            self.wake()

            # self.hostname = socket.gethostname()
            # use full qualified hostname, because of a probable bug on our
            # cluster.
            self.hostname = socket.getfqdn()

            self._drmaa_implementation = drmaa_implementation

            self.parallel_job_submission_info = parallel_job_submission_info

            self._configured_native_spec = configured_native_spec

            self.logger.debug("Parallel job submission info: %s",
                              repr(parallel_job_submission_info))

            if tmp_file_path == None:
                self.tmp_file_path = os.path.abspath("tmp")
            else:
                self.tmp_file_path = os.path.abspath(tmp_file_path)

        def clean(self):
            if self._drmaa_implementation == "PBS":
                tmp_out = os.path.join(
                    self.tmp_file_path, "soma-workflow-empty-job-patch-torque.o")
                tmp_err = os.path.join(
                    self.tmp_file_path, "soma-workflow-empty-job-patch-torque.e")

                # print("tmp_out="+tmp_out)
                # print("tmp_err="+tmp_err)

                if os.path.isfile(tmp_out):
                    os.remove(tmp_out)
                if os.path.isfile(tmp_err):
                    os.remove(tmp_err)

        def close_drmaa_session(self):
            if self._drmaa:
                self._drmaa.exit()
                self._drmaa = None

        def __del__(self):
            self.clean()
            self.close_drmaa_session()

        def sleep(self):
            '''
            Some Drmaa sessions expire if they idle too long.
            '''
            self.close_drmaa_session()
            self.is_sleeping = True

        def wake(self):
            '''
            Creates a fresh Drmaa session.
            '''
            import somadrmaa

            self.is_sleeping = False

            if not self._drmaa:
                self._drmaa = somadrmaa.Session()
                self._drmaa.initialize()

        def submit_simple_test_job(self, outstr, out_o_file, out_e_file):
            import somadrmaa
            # patch for the PBS-torque DRMAA implementation
            if self._drmaa_implementation == "PBS":

                '''
                Create a job to test
                '''
                jobTemplateId = self._drmaa.createJobTemplate()
                jobTemplateId.remoteCommand = 'echo'
                jobTemplateId.args = ["%s" % (outstr)]
                jobTemplateId.outputPath = "%s:%s" % (
                    self.hostname, os.path.join(self.tmp_file_path, "%s" % (out_o_file)))
                jobTemplateId.errorPath = "%s:%s" % (
                    self.hostname, os.path.join(self.tmp_file_path, "%s" % (out_e_file)))

                # print("jobTemplateId="+repr(jobTemplateId))
                # print("jobTemplateId.remoteCommand="+repr(jobTemplateId.remoteCommand))
                # print("jobTemplateId.args="+repr(jobTemplateId.args))
                # print("jobTemplateId.outputPath="+repr(jobTemplateId.outputPath))
                # print(
                # "jobTemplateId.errorPath="+repr(jobTemplateId.errorPath))

                jobid = self._drmaa.runJob(jobTemplateId)
                # print("jobid="+jobid)
                retval = self._drmaa.wait(
                    jobid, drmaa.Session.TIMEOUT_WAIT_FOREVER)
                # print("retval="+repr(retval))
                self._drmaa.deleteJobTemplate(jobTemplateId)

        def _setDrmaaParallelJob(self,
                                 drmaa_job_template,
                                 parallel_job_info):
            '''
            Set the DRMAA job template information for a parallel job submission.
            The configuration file must provide the parallel job submission
            information specific to the cluster in use.

            Parameters
            ----------
            drmaa_job_template: JobTemplate
                id of drmaa job template
            parallel_job_info: dict
                parallel_job_info: (configuration_name, nodes_number,
                cpu_per_node)
            '''
            if self.is_sleeping:
                self.wake()

            self.logger.debug(">> _setDrmaaParallelJob")
            configuration_name = parallel_job_info.et('config_name', 'native')
            cluster_specific_cfg_name = self.parallel_job_submission_info[
                configuration_name]

            for drmaa_attribute in configuration.PARALLEL_DRMAA_ATTRIBUTES:
                value = self.parallel_job_submission_info.get(drmaa_attribute)
                if value:
                    value = value.replace(
                        "{config_name}", cluster_specific_cfg_name)
                    value = value.replace("{nodes_number}",
                                          repr(parallel_job_info.get(
                                              'nodes_number', 1)))

                    setattr(drmaa_job_template, drmaa_attribute, value)

                    self.logger.debug(
                        "Parallel job, drmaa attribute = %s, value = %s ",
                        drmaa_attribute, value)

            if parallel_job_info:
                native_spec = drmaa_job_template.nativeSpecification
                if native_spec is None:
                    native_spec = []
                elif isinstance(native_spec, str):
                    native_spec = [native_spec]
                native_spec.append(
                    '-l nodes=%(nodes_number)s:ppn=%(cpu_per_node)s'
                    % parallel_job_info)
                drmaa_job_template.nativeSpecification = native_spec

            job_env = []
            for parallel_env_v in configuration.PARALLEL_JOB_ENV:
                value = self.parallel_job_submission_info.get(parallel_env_v)
                if value:
                    job_env.append((parallel_env_v, value.rstrip()))

            drmaa_job_template.jobEnvironment = dict(job_env)

            self.logger.debug("Parallel job environment : " + repr(job_env))
            self.logger.debug("<< _setDrmaaParallelJob")

            return drmaa_job_template

        def job_submission(self, jobs):
            '''
            @type  job: soma_workflow.engine_types.EngineJob
            @param job: job to be submitted
            @rtype: string
            @return: drmaa job ids
            '''
            drmaa_ids = []
            for job in jobs:
                try:
                    drmaa_id = self.submit_one_job(job)
                except:
                    drmaa_id = None
                drmaa_ids.append(drmaa_id)
            return drmaa_ids

        def submit_one_job(self, jobs):
            '''
            @type  job: soma_workflow.engine_types.EngineJob
            @param job: job to be submitted
            @rtype: string
            @return: drmaa job id
            '''
            if self.is_sleeping:
                self.wake()
            # patch for the PBS-torque DRMAA implementation
            command = []
            if job.is_engine_execution:
                # barrier jobs don't actually go through DRMAA.
                self.logger.debug('job_submission, DRMAA - barrier job.')
                job.status = constants.DONE
                return self.FAKE_JOB

            job_command = job.plain_command()

            # This is only for the old drmaa version
            # Now it is not necessary anymore
            # if self._drmaa_implementation == "PBS":
            if False:
                if job_command[0] == 'python':
                    job_command[0] = sys.executable
                for command_el in job_command:
                    command_el = command_el.replace('"', '\\\"')
                    command.append("\"" + command_el + "\"")
                self.logger.debug("PBS case, new command:" + repr(command))
            else:
                command = job_command

            self.logger.info("command: " + repr(command))
            self.logger.info("job.name=" + repr(job.name))

            stdout_file = job.plain_stdout()
            stderr_file = job.plain_stderr()
            stdin = job.plain_stdin()

            try:
                jobTemplateId = self._drmaa.createJobTemplate()
                jobTemplateId.remoteCommand = command[0]
                # ensure all args are strings
                jobTemplateId.args = [str(c) for c in command[1:]]
                jobTemplateId.jobName = job.name

                self.logger.info("jobTemplateId=" + repr(jobTemplateId) + " command[0]=" + repr(
                    command[0]) + " command[1:]=" + repr(command[1:]))
                self.logger.info(
                    "hostname and stdout_file= [%s]:%s" % (self.hostname, stdout_file))
                # ensure there is a directory for stdout
                if not os.path.exists(os.path.dirname(stdout_file)):
                    os.makedirs(os.path.dirname(stdout_file))

                jobTemplateId.outputPath = "%s:%s" % (
                    self.hostname, stdout_file)

                if job.join_stderrout:
                    jobTemplateId.joinFiles = "y"
                else:
                    if stderr_file:
                        jobTemplateId.errorPath = "%s:%s" % (
                            self.hostname, stderr_file)
                        # ensure there is a directory for stderr
                        if not os.path.exists(os.path.dirname(stderr_file)):
                            os.makedirs(os.path.dirname(stderr_file))

                if job.stdin:
                    # self.logger.debug("stdin: " + repr(stdin))
                    # self._drmaa.setAttribute(drmaaJobId,
                    #                        "drmaa_input_path",
                    #                        "%s:%s" %(self.hostname, stdin))
                    self.logger.debug("stdin: " + repr(stdin))
                    jobTemplateId.inputPath = stdin

                working_directory = job.plain_working_directory()
                if working_directory:
                    jobTemplateId.workingDirectory = working_directory

                self.logger.debug(
                    "JOB NATIVE_SPEC " + repr(job.native_specification))
                self.logger.debug(
                    "CONFIGURED NATIVE SPEC " + repr(self._configured_native_spec))
                native_spec = None

                if job.native_specification:
                    native_spec = job.native_specification
                elif self._configured_native_spec:
                    native_spec = self._configured_native_spec

                if job.queue and native_spec:
                    jobTemplateId.nativeSpecification = "-q " + \
                        str(job.queue) + " " + str(native_spec)
                    self.logger.debug(
                        "NATIVE specification " + "-q " + str(job.queue) + " " + str(native_spec))
                elif job.queue:
                    jobTemplateId.nativeSpecification = "-q " + str(job.queue)
                    self.logger.debug(
                        "NATIVE specification " + "-q " + str(job.queue))
                elif native_spec:
                    jobTemplateId.nativeSpecification = str(native_spec)
                    self.logger.debug(
                        "NATIVE specification " + str(native_spec))

                jobTemplateId.jobEnvironment = {}

                if job.parallel_job_info:
                    jobTemplateId = self._setDrmaaParallelJob(
                        jobTemplateId, job.parallel_job_info)

                if self._drmaa_implementation == "PBS":
                    job_env = []
                    for var_name in os.environ.keys():
                        job_env.append((var_name, os.environ[var_name]))
                    jobTemplateId.jobEnvironment = dict(job_env)
                if isinstance(job.env, dict):
                    jobTemplateId.jobEnvironment.update(job.env)

                self.logger.debug("before submit command: " + repr(command))
                self.logger.debug("before submit job.name=" + repr(job.name))
                self.logger.debug("before submit job.env=" + repr(job.env))
                drmaaSubmittedJobId = self._drmaa.runJob(jobTemplateId)
                self._drmaa.deleteJobTemplate(jobTemplateId)

            except Exception as e:
                try:
                    f = open(stderr_file, "wa")
                    f.write("Error in job submission: %s" % (e))
                    f.close()
                except IOError as ioe:
                    pass
                self.logger.error("Error in job submission: %s" % (e))
                raise DRMError("Job submission error: %s" % (e))

            return drmaaSubmittedJobId

        def kill_job(self, scheduler_job_id):
            if self.is_sleeping:
                self.wake()
            if scheduler_job_id == self.FAKE_JOB:
                return  # barriers are not run, thus cannot be killed.
            try:
                self._drmaa.control(
                    scheduler_job_id, JobControlAction.TERMINATE)
            except DrmaaException as e:
                self.logger.critical("%s" % e)
                raise e

        def get_job_status(self, scheduler_job_id):
            if self.is_sleeping:
                self.wake()
            if scheduler_job_id == self.FAKE_JOB:
                # a barrier job is done as soon as it is started.
                return constants.DONE
            try:
                status = self._drmaa.jobStatus(scheduler_job_id)
            except DrmaaException as e:
                self.logger.error("%s" % (e))
                raise DRMError("%s" % (e))
            return status

        def get_job_exit_info(self, scheduler_job_id):
            if self.is_sleeping:
                self.wake()

            if scheduler_job_id == self.FAKE_JOB:
                res_resourceUsage = ''
                res_status = constants.FINISHED_REGULARLY
                res_exitValue = 0
                res_termSignal = None
                return (res_status, res_exitValue, res_termSignal,
                        res_resourceUsage)

            res_resourceUsage = []
            res_status = constants.EXIT_UNDETERMINED
            res_exitValue = 0
            res_termSignal = None

            try:
                self.logger.debug(
                    "  ==> Start to find info of job %s" % (scheduler_job_id))
                jid_out, exit_value, signaled, term_sig, coredumped, aborted, exit_status, resource_usage = self._drmaa.wait(
                    scheduler_job_id, self._drmaa.TIMEOUT_NO_WAIT)

                self.logger.debug("  ==> jid_out=" + repr(jid_out))
                self.logger.debug("  ==> exit_value=" + repr(exit_value))
                self.logger.debug("  ==> signaled=" + repr(signaled))
                self.logger.debug("  ==> term_sig=" + repr(term_sig))
                self.logger.debug("  ==> coredumped=" + repr(coredumped))
                self.logger.debug("  ==> aborted=" + repr(aborted))
                self.logger.debug("  ==> exit_status=" + repr(exit_status))
                self.logger.debug(
                    "  ==> resource_usage=" + repr(resource_usage))

                if aborted:
                    res_status = constants.EXIT_ABORTED
                else:
                    if exit_value:
                        res_status = constants.FINISHED_REGULARLY
                        res_exitValue = exit_status
                    else:
                        if signaled:
                            res_status = constants.FINISHED_TERM_SIG
                            res_termSignal = term_sig
                        else:
                            res_status = constants.FINISHED_UNCLEAR_CONDITIONS

                self.logger.debug("  ==> res_status=" + repr(res_status))
                res_resourceUsage = b''
                for k, v in six.iteritems(resource_usage):
                    res_resourceUsage = res_resourceUsage + k + b'=' + v + b' '

            except ExitTimeoutException:
                res_status = constants.EXIT_UNDETERMINED
                self.logger.debug("  ==> self._drmaa.wait time out")

            # DRMAA may leave files in ~/.drmaa
            self.cleanup_drmaa_files(scheduler_job_id)

            return (res_status, res_exitValue, res_termSignal, res_resourceUsage)

        def cleanup_drmaa_files(self, scheduler_job_id):
            filename = os.path.join(Configuration.get_home_dir(),
                                    '.drmaa', str(scheduler_job_id))
            startfile = '%s.started' % filename
            endfile = '%s.exitcode' % filename
            for f in (startfile, endfile):
                if os.path.exists(f):
                    os.unlink(f)

        @classmethod
        def build_scheduler(cls, config):
            if not DRMAA_LIB_FOUND:
                raise NoDrmaaLibError
            sch = DrmaaScheduler(
                config.get_drmaa_implementation(),
                config.get_parallel_job_config(),
                os.path.expanduser("~"),
                configured_native_spec=config.get_native_specification())
            return sch


else:  # DRMAA_LIB_FOUND is False

    DrmaaScheduler = None

# for backward compatibility
DrmaaCTypes = DrmaaScheduler
