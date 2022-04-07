# -*- coding: utf-8 -*-

from __future__ import with_statement, print_function
from __future__ import absolute_import

'''
organization: I2BM, Neurospin, Gif-sur-Yvette, France

license:CeCILL version 2, http://www.cecill.info/licences/Licence_CeCILL_V2-en.html
'''

from ..scheduler import Scheduler
import sys
import logging
import os
import socket
import six
import json
import soma_workflow.constants as constants
from soma_workflow.errors import DRMError, ExitTimeoutException
from soma_workflow import configuration
from soma_workflow.configuration import Configuration
import tempfile
from soma_workflow import subprocess
import time
import distutils.spawn


class JobTemplate(object):

    def __init__(self, remoteCommand, outputPath=None, errorPath=None, **kwargs):
        self.remoteCommand = remoteCommand
        self.outputPath = outputPath
        self.errorPath = errorPath
        self.inputPath = None
        self.jobName = ''
        self.queue = None
        self.env = None
        self.nativeSpecification = None
        self.joinFiles = False
        self.workingDirectory = None
        self.__dict__.update(kwargs)

    def build_pbs_script(self, script_file=None):
        if not script_file:
            script_file_ = tempfile.mkstemp()
            script_file = script_file_[1]
            os.close(script_file_[0])
        with open(script_file, 'w') as f:
            f.write('#!/bin/sh\n')
            # PBS options
            if self.jobName:
                special_chars = " ()&:"
                job_name = self.jobName
                for special_char in special_chars:
                    job_name = job_name.replace(special_char, '_')
                f.write('#PBS -N %s\n' % job_name)
            if self.queue:
                f.write('#PBS -q %s\n' % self.queue)
            # if self.env:
                # var = ','.join(['"%s=%s"' % (k, v.replace('"', '\\"'))
                                # for k, v in six.iteritems(self.env)])
                # f.write('#PBS -v %s\n' % var)
            if self.nativeSpecification:
                native_spec = self.nativeSpecification
                if not isinstance(native_spec, list):
                    native_spec = [native_spec]
                for spec in native_spec:
                    f.write('#PBS %s\n' % spec)
            if self.joinFiles:
                f.write('#PBS -j oe\n')

            # env variables
            if self.env:
                for var, value in self.env.items():
                    f.write('export %s=%s\n' % (var, value))

            # working directory
            if self.workingDirectory:
                f.write('cd "%s"\n' % self.workingDirectory)

            # commandline, ensure all args are strings
            escaped_command = [str(x).replace('"', '\\"')
                               for x in self.remoteCommand]
            redirect = ''
            if self.inputPath:
                # handle stdin redirection. Use bash redirection, I don't
                # know  another way.
                redirect = ' < "%s"' % self.inputPath

            f.write('"' + '" "'.join(escaped_command) + '"' + redirect + '\n')
        logger = logging.getLogger('ljp.pbspro_scheduler')
        logger.debug('build_pbs_script: %s' % script_file)
        return script_file

    def qsub_command(self, script_file):
        cmd = PBSProScheduler.qsub_command()
        if script_file:
            if self.outputPath:
                cmd += ['-o', self.outputPath]
            if self.errorPath:
                cmd += ['-e', self.errorPath]
            cmd.append(script_file)
        return cmd

    def run_job(self, script_file=None, keep_script_file=False):
        rm_script = False
        if not script_file:
            rm_script = True
            script_file = self.build_pbs_script()
        try:
            cmd = self.qsub_command(script_file)
            logger = logging.getLogger('ljp.pbspro_scheduler')
            logger.debug('run job: %s' % repr(cmd))
            job_id = subprocess.check_output(
                cmd, stderr=subprocess.STDOUT).decode('utf-8').strip()
            logger.debug('job_id: ' + repr(job_id))
            return job_id
        finally:
            if rm_script and not keep_script_file:
                os.unlink(script_file)
            elif rm_script:
                logger.info('job %s script file: %s' %
                            (self.jobName, script_file))


class PBSProScheduler(Scheduler):

    '''
    Scheduling using a PBS Pro session.
    '''

    # dict
    parallel_job_submission_info = None

    logger = None

    _configured_native_spec = None

    tmp_file_path = None

    is_sleeping = False
    FAKE_JOB = -167
    _out_of_container_command = None

    def __init__(self,
                 parallel_job_submission_info,
                 tmp_file_path=None,
                 configured_native_spec=None):

        super(PBSProScheduler, self).__init__()

        self.logger = logging.getLogger('ljp.pbspro_scheduler')

        self.wake()

        # self.hostname = socket.gethostname()
        # use full qualified hostname, because of a probable bug on our
        # cluster.
        self.hostname = socket.getfqdn()

        self.parallel_job_submission_info = parallel_job_submission_info

        self._configured_native_spec = configured_native_spec

        self.logger.debug("Parallel job submission info: %s",
                          repr(parallel_job_submission_info))

        if tmp_file_path == None:
            self.tmp_file_path = os.path.abspath("tmp")
        else:
            self.tmp_file_path = os.path.abspath(tmp_file_path)
        self.setup_pbs_variant()

    def __del__(self):
        self.clean()

    @staticmethod
    def out_of_container_command():
        """
        In case this server is running inside a contaner, qsat/qsub commands
        are not available inside the container but are on the host. We need to
        get out of the container in such a situation.
        """
        out_container = getattr(PBSProScheduler, '_out_of_container_command',
                                None)
        if out_container is not None:
            return out_container  # cached
        if distutils.spawn.find_executable('qstat') \
                or 'CASA_HOST_DIR' not in os.environ:
            out_container = []
        else:
            out_container = ['ssh', 'localhost']
        PBSProScheduler._out_of_container_command = out_container
        return out_container

    @staticmethod
    def qstat_command():
        return PBSProScheduler.out_of_container_command() + ['qstat']

    @staticmethod
    def qsub_command():
        return PBSProScheduler.out_of_container_command() + ['qsub']

    def submit_simple_test_job(self, outstr, out_o_file, out_e_file):
        '''
        Create a job to test
        '''
        outputPath = "%s:%s" % (self.hostname,
                                os.path.join(self.tmp_file_path, "%s" % (out_o_file)))
        errorPath = "%s:%s" % (self.hostname,
                               os.path.join(self.tmp_file_path, "%s" % (out_e_file)))
        jobTemplate = JobTemplate(remoteCommand=['echo', outstr],
                                  outputPath=outputPath, errorPath=errorPath)

        # print("jobTemplate="+repr(jobTemplate))
        # print("jobTemplate.remoteCommand="+repr(jobTemplate.remoteCommand))
        # print("jobTemplate.outputPath="+repr(jobTemplate.outputPath))
        # print("jobTemplate.errorPath="+repr(jobTemplate.errorPath))

        jobid = jobTemplate.run_job()
        # print("jobid="+jobid)
        retval = self.wait_job(jobid)
        # print("retval="+repr(retval))

    def get_pbs_version(self):
        '''
        get PBS implementation and version. May be PBS Pro or Torque/PBS

        Returns
        -------
        pbs_version: tuple (2 strings)
            (implementation, version)
        '''
        cmd = self.qstat_command() + ['--version']
        verstr = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode(
            'utf-8')
        if verstr.startswith('pbs_version ='):
            impl = 'pbspro'
            ver = verstr.split('=')[1].strip()
        else:
            impl = 'torque'
            ver = verstr.split(':')[1].strip()
        return (impl, ver)

    def setup_pbs_variant(self):
        '''
        determine PBS implementation / version and set internal behaviour accordingly
        '''
        impl, ver = self.get_pbs_version()
        self._pbs_impl = impl
        # strip off suffixes like in '20.0.1~ubuntu_16.04'
        if '~' in ver:
            ver = ver[:ver.index('~')]
        self._pbs_version = [int(x) for x in ver.split('.')]
        self.logger.info('PBS implementation: %s' % self._pbs_impl)
        self.logger.info('PBS version: %s' % repr(self._pbs_version))

    def _setParallelJob(self,
                        job_template,
                        parallel_job_info):
        '''
        Set the job template information for a parallel job submission.
        The configuration file must provide the parallel job submission
        information specific to the cluster in use.

        Parameters
        ----------
        job_template: JobTemplate
            job template instance
        parallel_job_info: dict
            parallel info dict, containing:
            nodes_number: int
                maximum node number the job requests (on a unique machine or
                separated machine depending on the parallel configuration)
            cpu_per_node: int
                nomber of CPUs or cores per node
        '''
        if self.is_sleeping:
            self.wake()

        self.logger.debug(">> _setParallelJob")
        configuration_name = parallel_job_info.get('config_name', 'native')
        cluster_specific_cfg_name = self.parallel_job_submission_info.get(
            configuration_name)

        if cluster_specific_cfg_name:
            for attribute in configuration.PARALLEL_DRMAA_ATTRIBUTES:
                value = self.parallel_job_submission_info.get(attribute)
                if value:
                    value = value.replace(
                        "{config_name}", cluster_specific_cfg_name)
                    value = value.replace("{nodes_number}",
                                          repr(parallel_job_info.get(
                                              'nodes_number', 1)))

                    setattr(job_template, attribute, value)

                    self.logger.debug(
                        "Parallel job, drmaa attribute = %s, value = %s ",
                        attribute, value)

        if parallel_job_info:
            native_spec = job_template.nativeSpecification
            if native_spec is None:
                native_spec = []
            elif isinstance(native_spec, str):
                native_spec = [native_spec]
            native_spec.append(
                '-l nodes=%(nodes_number)s:ppn=%(cpu_per_node)s'
                % parallel_job_info)
            job_template.nativeSpecification = native_spec

        job_env = {}
        for parallel_env_v in configuration.PARALLEL_JOB_ENV:
            value = self.parallel_job_submission_info.get(parallel_env_v)
            if value:
                job_env[parallel_env_v] = value.rstrip()

        if job_template.env is None:
            job_template.env = {}
        job_template.env.update(job_env)

        self.logger.debug("Parallel job environment : " + repr(job_env))
        self.logger.debug("<< _setParallelJob")

        return job_template

    def job_submission(self, jobs, signal_end=True):
        '''
        Parameters
        ----------
        jobs: soma_workflow.client.Job
            job to be submitted
        signal_end: bool

        Returns
        -------
        job_id: string
            job id
        '''
        drmaa_ids = []
        for job in jobs:
            try:
                drmaa_id = self.submit_one_job(job)
            except:
                drmaa_id = None
            drmaa_ids.append(drmaa_id)
        return drmaa_ids

    def submit_one_job(self, job):
        if self.is_sleeping:
            self.wake()
        if job.is_engine_execution:
            # barrier jobs don't actually go through job submission
            self.logger.debug('job_submission, PBS - barrier job.')
            job.status = constants.DONE
            return self.FAKE_JOB

        command = job.plain_command()

        self.logger.info("command: " + repr(command))
        self.logger.info("job.name=" + repr(job.name))

        stdout_file = job.plain_stdout()
        stderr_file = job.plain_stderr()
        stdin = job.plain_stdin()

        try:
            outputPath = "%s:%s" % (self.hostname, stdout_file)
            if stderr_file:
                errorPath = "%s:%s" % (self.hostname, stderr_file)
            else:
                errorPath = None
            jobTemplate = JobTemplate(remoteCommand=command,
                                      outputPath=outputPath,
                                      errorPath=errorPath)
            jobTemplate.jobName = job.name

            self.logger.info("jobTemplate=" + repr(jobTemplate)
                             + " command[0]=" + repr(command[0])
                             + " command[1:]=" + repr(command[1:]))
            self.logger.info(
                "hostname and stdout_file= [%s]:%s" % (self.hostname, stdout_file))
            # ensure there is a directory for stdout
            if not os.path.exists(os.path.dirname(stdout_file)):
                os.makedirs(os.path.dirname(stdout_file))

            if not job.join_stderrout:
                # ensure there is a directory for stderr
                if not os.path.exists(os.path.dirname(stderr_file)):
                    os.makedirs(os.path.dirname(stderr_file))

            if job.stdin:
                self.logger.debug("stdin: " + repr(stdin))
                jobTemplate.inputPath = stdin

            working_directory = job.plain_working_directory()
            if working_directory:
                jobTemplate.workingDirectory = working_directory

            self.logger.debug(
                "JOB NATIVE_SPEC " + repr(job.native_specification))
            self.logger.debug(
                "CONFIGURED NATIVE SPEC " + repr(self._configured_native_spec))

            if job.queue:
                jobTemplate.queue = job.queue
                self.logger.debug("queue: " + str(job.queue))

            native_spec = None
            if job.native_specification:
                native_spec = job.native_specification
            elif self._configured_native_spec:
                native_spec = self._configured_native_spec

            if native_spec:
                jobTemplate.nativeSpecification = str(native_spec)
                self.logger.debug(
                    "NATIVE specification " + str(native_spec))

            if job.parallel_job_info:
                jobTemplate = self._setParallelJob(jobTemplate,
                                                   job.parallel_job_info)

            if job.env:
                jobTemplate.env = dict(job.env)

            self.logger.debug("before submit command: " + repr(command))
            self.logger.debug("before submit job.name=" + repr(job.name))
            job_id = jobTemplate.run_job()

        except subprocess.CalledProcessError as e:
            # try:  # this will be done in the engine
            #     f = open(stderr_file, "a")
            #     f.write("Error in job submission: %s: %s, output: %s"
            #             % (type(e), e, e.output))
            #     f.close()
            # except Exception:
            #     pass
            self.logger.error("Error in job submission: %s: %s\nOutput:\n%s"
                              % (type(e), e, e.output.decode()),
                              exc_info=sys.exc_info())
            raise DRMError("Job submission error: %s:\n%s\nOutput:\n%s"
                           % (type(e), e, e.output.decode()))

        except Exception as e:
            self.logger.info('exception in PBS job submission:' + repr(e))
            try:
                f = open(stderr_file, "wa")
                f.write("Error in job submission: %s: %s" % (type(e), e))
                f.close()
            except Exception:
                pass
            self.logger.error("Error in job submission: %s: %s" % (type(e), e),
                              exc_info=sys.exc_info())
            raise DRMError("Job submission error: %s: %s" % (type(e), e))

        return job_id

    def kill_job(self, scheduler_job_id):
        '''
        Parameters
        ----------
        scheduler_job_id: string
            Job id for the scheduling system (DRMAA for example)
        '''
        if self.is_sleeping:
            self.wake()
        if scheduler_job_id == self.FAKE_JOB:
            return  # barriers are not run, thus cannot be killed.
        self.logger.debug('kill job: %s' % scheduler_job_id)
        try:
            status = self.get_job_status(scheduler_job_id)
            if status not in (constants.DONE, constants.FAILED):
                cmd = ['qdel', scheduler_job_id]
                subprocess.check_call(cmd)
        except Exception as e:
            self.logger.critical("%s: %s" % (type(e), e))
            # raise

    def get_job_extended_status(self, scheduler_job_id):
        '''
        Get job full status in a dictionary (from qstat in json format)
        '''
        if self.is_sleeping:
            self.wake()
        if scheduler_job_id == self.FAKE_JOB:
            # a barrier job is done as soon as it is started.
            return constants.DONE
        try:
            if self._pbs_impl == 'pbspro':
                cmd = self.qstat_command() + ['-x', '-f', '-F', 'json',
                                              scheduler_job_id]
                json_str = subprocess.check_output(cmd).decode('utf-8')
                super_status = json.loads(json_str)

            else:  # torque/pbs
                import xml.etree.cElementTree as ET
                cmd = self.qstat_command() + ['-x', scheduler_job_id]
                xml_str = subprocess.check_output(cmd).decode('utf-8')
                super_status = {}
                s_xml = ET.fromstring(xml_str)
                xjob = s_xml[0]
                job = {}
                parsing = [(xjob, job)]
                while parsing:
                    element, parent = parsing.pop(0)
                    for child in element:
                        tag = child.tag
                        if tag == 'Job_Id':
                            super_status['Jobs'] = {child.text: job}
                        else:
                            if len(child) != 0:
                                current = {}
                                parsing.append((child, current))
                            else:
                                current = child.text
                            parent[tag] = current
                    current = None
        except Exception as e:
            self.logger.critical("%s: %s" % (type(e), e))
            raise
        status = super_status['Jobs'][scheduler_job_id]
        self.logger.debug('get_job_extended_status: ' + repr(scheduler_job_id)
                          + ': ' + repr(status))
        return status

    def get_pbs_status_codes(self):
        if self._pbs_impl == 'pbspro':
            class codes(object):
                ARRAY_STARTED = 'B'
                EXITING = 'E'
                FINISHED = 'F'
                HELD = 'H'
                MOVED = 'M'
                QUEUED = 'Q'
                RUNNING = 'R'
                SUSPENDED = 'S'
                MOVING = 'T'
                SUSPEND_KB = 'U'
                WAITING = 'W'
                SUBJOB_COMPLETE = 'X'
        else:  # torque/pbs
            class codes(object):
                ARRAY_STARTED = 'B'  # unused
                EXITING = 'E'
                FINISHED = 'C'
                HELD = 'H'
                MOVED = 'M'  # unused
                QUEUED = 'Q'
                RUNNING = 'R'
                SUSPENDED = 'S'
                MOVING = 'T'
                SUSPEND_KB = 'U'  # unused
                WAITING = 'W'
                SUBJOB_COMPLETE = 'X'  # unused
        return codes

    def get_job_status(self, scheduler_job_id):
        '''
        Parameters
        ----------
        scheduler_job_id: string
            Job id for the scheduling system (DRMAA for example)

        Returns
        -------
        status: string
            Job status as defined in constants.JOB_STATUS
        '''
        if scheduler_job_id == self.FAKE_JOB:
            # it's a barrier job, doesn't exist in DRMS, and it's always done.
            return constants.DONE
        codes = self.get_pbs_status_codes()
        try:
            status = self.get_job_extended_status(scheduler_job_id)
            state = status['job_state']
            self.logger.debug(
                'get_job_status for: ' + repr(scheduler_job_id) + ': ', repr(state))
        except Exception:
            return constants.UNDETERMINED
        if state == codes.ARRAY_STARTED:
            return constants.RUNNING
        elif state == codes.EXITING:
            return constants.RUNNING  # FIXME not exactly that
        elif state == codes.FINISHED:
            exit_value = status.get('Exit_status', -1)
            if exit_value != 0:
                return constants.FAILED
            return constants.DONE
        elif state == codes.HELD:
            return constants.USER_SYSTEM_ON_HOLD  # FIXME or USER_ON_HOLD ?
        elif state == codes.MOVED:
            return constants.USER_SYSTEM_SUSPENDED  # FIXME not exactly that
        elif state == codes.QUEUED:
            return constants.QUEUED_ACTIVE
        elif state == codes.RUNNING:
            return constants.RUNNING
        elif state == codes.SUSPENDED:
            return constants.USER_SYSTEM_SUSPENDED  # FIXME or SYSTEM_SUSPENDED ?
        elif state == codes.MOVING:
            return constants.USER_SUSPENDED  # FIXME not exactly that
        elif state == codes.SUSPEND_KB:
            return constants.USER_SYSTEM_SUSPENDED  # is it that ?
        elif state == codes.WAITING:
            return constants.QUEUED_ACTIVE  # FIXME not exactly that
        elif state == codes.SUBJOB_COMPLETE:
            return constants.DELETE_PENDING  # is it that ?
        else:
            return constants.UNDETERMINED

    def get_job_exit_info(self, scheduler_job_id):
        '''
        The exit info consists of 4 values returned in a tuple:
        **exit_status**: string
            one of the constants.JOB_EXIT_STATUS values
        **exit_value**: int
            exit code of the command (normally 0 in case of success)
        **term_sig**: int
            termination signal, 0 IF ok
        **resource_usage**: unicode
            string in the shape
            ``'cpupercent=60 mem=13530kb cput=00:00:12'`` etc. Items may include:

            * cpupercent
            * cput
            * mem
            * vmem
            * ncpus
            * walltime

        Parameters
        ----------
        scheduler_job_id: string
            Job id for the scheduling system (DRMAA for example)

        Returns
        -------
        exit_info: tuple
            exit_status, exit_value, term_sig, resource_usage
        '''
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
            status = self.get_job_extended_status(scheduler_job_id)
            jid_out = status['Output_Path']
            exit_value = status.get('Exit_status', -1)
            signaled = False
            term_sig = 0
            if exit_value >= 256:
                signaled = True
                term_sig = exit_value % 256
            coredumped = (term_sig == 2)
            aborted = (exit_value <= -4 and exit_value >= -6)
            exit_status = exit_value
            resource_usage = status.get('resources_used', {})
            # jid_out, exit_value, signaled, term_sig, coredumped, aborted, exit_status, resource_usage = self._drmaa.wait(
            #     scheduler_job_id, self._drmaa.TIMEOUT_NO_WAIT)

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
                if exit_value == 0:
                    res_status = constants.FINISHED_REGULARLY
                    res_exitValue = exit_status
                else:
                    res_exitValue = exit_status
                    if signaled:
                        res_status = constants.FINISHED_TERM_SIG
                        res_termSignal = term_sig
                    else:
                        # in soma-workflow a job with a non-zero exit code
                        # has still finished regularly (ran without being
                        # interrupted)
                        # res_status = constants.EXIT_ABORTED
                        res_status = constants.FINISHED_REGULARLY

            self.logger.info("  ==> res_status=" + repr(res_status))
            res_resourceUsage = u''
            for k, v in six.iteritems(resource_usage):
                res_resourceUsage = res_resourceUsage + \
                    k + u'=' + str(v) + u' '

        except ExitTimeoutException:
            res_status = constants.EXIT_UNDETERMINED
            self.logger.debug("  ==> wait time out")

        return (res_status, res_exitValue, res_termSignal, res_resourceUsage)

    def wait_job(self, job_id, timeout=0, poll_interval=0.3):
        '''
        Wait for a specific job to be terminated.
        '''
        status = self.get_job_extended_status(job_id)
        stime = time.time()
        while status['job_state'] not in ('F', ):
            if timeout > 0 and time() - stime > timeout:
                raise ExitTimeoutException('wait_job timed out')
            time.sleep(poll_interval)
            status = self.get_job_extended_status(job_id)
        return status

    @classmethod
    def build_scheduler(cls, config):
        '''
        Build an instance of PBSProScheduler
        '''
        sch = PBSProScheduler(
            config.get_parallel_job_config(),
            os.path.expanduser("~"),
            configured_native_spec=config.get_native_specification())
        return sch
