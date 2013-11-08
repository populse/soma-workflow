# -*- coding: utf-8 -*-
"""
Created on Mon Oct 28 11:29:54 2013

@author: laure.hugo@cea.fr
"""
import os

from soma_workflow.errors import ConfigurationError
from soma_workflow.client import Job, FileTransfer
import soma_workflow.configuration as configuration


class JobExamples(object):
    '''
    Job submission example.
    Each method submits 1 job and return the tuple (job_id,
    local_ouput_files, std_out_err)
    => pipeline of 4 jobs with file transfer: submitJob1, submitJob2,
    submitJob3, and submitJob4 methods.
    => job raising an exception with file transfer: submitExceptionJob
    => local job using user's files only (even for stdout and stderr):
    localCustomSubmission
    => local job regular submission: localSubmission
    '''

    def __init__(self, wf_ctrl, python,
                 transfer_timeout=-24, jobs_timeout=1):
        # Define example directories
        import soma_workflow
        self.examples_dir = os.path.join(soma_workflow.__path__[0],
                                         "..", "..", "test", "jobExamples")
        self.output_dir = os.path.join(soma_workflow.__path__[0],
                                       "..", "..", "test", "out")
        if not os.path.isdir(self.output_dir):
            os.mkdir(self.output_dir)
        if (not os.path.isdir(self.examples_dir) or
                not os.path.isdir(self.output_dir)):
            raise ConfigurationError("%s or %s does not exist." % (
                                     self.examples_dir,
                                     self.output_dir))

        self.wf_ctrl = wf_ctrl
        self.tr_timeout = transfer_timeout
        self.jobs_timeout = jobs_timeout
        self.python = python

        self.complete_dir = os.path.join(self.examples_dir, "complete")
        self.simple_dir = os.path.join(self.examples_dir, "simple")
        self.complete_models_dir = os.path.join(self.complete_dir,
                                                "outputModels")
        self.simple_models_dir = os.path.join(self.simple_dir,
                                              "outputModels")
        self.job1_output_file_models = [
            os.path.join(self.complete_models_dir, "file11"),
            os.path.join(self.complete_models_dir, "file12")]
        self.job2_output_file_models = [
            os.path.join(self.complete_models_dir, "file2")]
        self.job3_output_file_models = [
            os.path.join(self.complete_models_dir, "file3")]
        self.job4_output_file_models = [
            os.path.join(self.complete_models_dir, "file4")]

        self.job1_stdouterr_models = [
            os.path.join(self.complete_models_dir, "stdoutjob1"),
            os.path.join(self.complete_models_dir, "stderrjob1")]
        self.job2_stdouterr_models = [
            os.path.join(self.complete_models_dir, "stdoutjob2"),
            os.path.join(self.complete_models_dir, "stderrjob2")]
        self.job3_stdouterr_models = [
            os.path.join(self.complete_models_dir, "stdoutjob3"),
            os.path.join(self.complete_models_dir, "stderrjob3")]
        self.job4_stdouterr_models = [
            os.path.join(self.complete_models_dir, "stdoutjob4"),
            os.path.join(self.complete_models_dir, "stderrjob4")]
        self.exceptionjobstdouterr = [
            os.path.join(self.simple_models_dir, "stdout_exception_job"),
            os.path.join(self.simple_models_dir, "stderr_exception_job")]

    def set_new_connection(self, wf_ctrl):
        '''
        For the disconnection test
        '''
        self.wf_ctrl = wf_ctrl

    def submit_job1(self, time=2):
        self.file11_tr = self.wf_ctrl.register_transfer(
            FileTransfer(is_input=False,
                         client_path=os.path.join(self.output_dir, "file11"),
                         disposal_timeout=self.tr_timeout))
        self.file12_tr = self.wf_ctrl.register_transfer(
            FileTransfer(is_input=False,
                         client_path=os.path.join(self.output_dir, "file12"),
                         disposal_timeout=self.tr_timeout))
        self.file0_tr = self.wf_ctrl.register_transfer(
            FileTransfer(True,
                         os.path.join(self.complete_dir, "file0"),
                         self.tr_timeout))
        script1_tr = self.wf_ctrl.register_transfer(
            FileTransfer(True,
                         os.path.join(self.complete_dir, "job1.py"),
                         self.tr_timeout))
        stdin1_tr = self.wf_ctrl.register_transfer(
            FileTransfer(True,
                         os.path.join(self.complete_dir, "stdin1"),
                         self.tr_timeout))
        self.wf_ctrl.transfer_files(self.file0_tr.engine_path)
        self.wf_ctrl.transfer_files(script1_tr.engine_path)
        self.wf_ctrl.transfer_files(stdin1_tr.engine_path)
        print "files transfered "
        job1_id = self.wf_ctrl.submit_job(Job(
            command=[self.python, script1_tr, self.file0_tr,
                     self.file11_tr, self.file12_tr, repr(time)],
            referenced_input_files=[self.file0_tr, script1_tr,
                                    stdin1_tr],
            referenced_output_files=[self.file11_tr, self.file12_tr],
            stdin=stdin1_tr,
            join_stderrout=False,
            disposal_timeout=self.jobs_timeout,
            name="job1 with transfers"))

        return ((job1_id,
                 [self.file11_tr.engine_path,
                  self.file12_tr.engine_path],
                 None))

    def submit_job2(self, time=2):
        self.file2_tr = self.wf_ctrl.register_transfer(
            FileTransfer(False,
                         os.path.join(self.output_dir, "file2"),
                         self.tr_timeout))
        script2_tr = self.wf_ctrl.register_transfer(
            FileTransfer(True,
                         os.path.join(self.complete_dir, "job2.py"),
                         self.tr_timeout))
        stdin2_tr = self.wf_ctrl.register_transfer(
            FileTransfer(True,
                         os.path.join(self.complete_dir, "stdin2"),
                         self.tr_timeout))
        self.wf_ctrl.transfer_files(script2_tr.engine_path)
        self.wf_ctrl.transfer_files(stdin2_tr.engine_path)
        job2_id = self.wf_ctrl.submit_job(Job(
            command=[self.python, script2_tr, self.file11_tr,
                     self.file0_tr, self.file2_tr, repr(time)],
            referenced_input_files=[self.file0_tr, self.file11_tr,
                                    script2_tr, stdin2_tr],
            referenced_output_files=[self.file2_tr],
            stdin=stdin2_tr,
            join_stderrout=False,
            disposal_timeout=self.jobs_timeout,
            name="job2 with transfers"))

        return (job2_id, [self.file2_tr.engine_path], None)

    def submit_job3(self, time=2):
        self.file3_tr = self.wf_ctrl.register_transfer(
            FileTransfer(False,
                         os.path.join(self.output_dir, "file3"),
                         self.tr_timeout))
        script3_tr = self.wf_ctrl.register_transfer(
            FileTransfer(True,
                         os.path.join(self.complete_dir, "job3.py"),
                         self.tr_timeout))
        stdin3_tr = self.wf_ctrl.register_transfer(
            FileTransfer(True,
                         os.path.join(self.complete_dir, "stdin3"),
                         self.tr_timeout))
        self.wf_ctrl.transfer_files(script3_tr.engine_path)
        self.wf_ctrl.transfer_files(stdin3_tr.engine_path)
        job3_id = self.wf_ctrl.submit_job(Job(
            command=[self.python, script3_tr, self.file12_tr,
                     self.file3_tr, repr(time)],
            referenced_input_files=[self.file12_tr, script3_tr,
                                    stdin3_tr],
            referenced_output_files=[self.file3_tr],
            stdin=stdin3_tr,
            join_stderrout=False,
            disposal_timeout=self.jobs_timeout,
            name="job3 with transfers"))

        return (job3_id, [self.file3_tr.engine_path], None)

    def submit_job4(self, time=10):
        self.file4_tr = self.wf_ctrl.register_transfer(
            FileTransfer(False,
                         os.path.join(self.output_dir, "file4"),
                         self.tr_timeout))
        script4_tr = self.wf_ctrl.register_transfer(
            FileTransfer(True,
                         os.path.join(self.complete_dir, "job4.py"),
                         self.tr_timeout))
        stdin4_tr = self.wf_ctrl.register_transfer(
            FileTransfer(True,
                         os.path.join(self.complete_dir, "stdin4"),
                         self.tr_timeout))
        self.wf_ctrl.transfer_files(script4_tr.engine_path)
        self.wf_ctrl.transfer_files(stdin4_tr.engine_path)
        job4_id = self.wf_ctrl.submit_job(Job(
            command=[self.python, script4_tr, self.file2_tr,
                     self.file3_tr, self.file4_tr, repr(time)],
            referenced_input_files=[self.file2_tr, self.file3_tr,
                                    script4_tr, stdin4_tr],
            referenced_output_files=[self.file4_tr],
            stdin=stdin4_tr,
            join_stderrout=False,
            disposal_timeout=self.jobs_timeout,
            name="job4 with transfers"))

        return (job4_id, [self.file4_tr.engine_path], None)

    def submit_exception_job(self):
        script_tr = self.wf_ctrl.register_transfer(
            FileTransfer(True,
                         os.path.join(self.simple_dir, "exceptionJob.py"),
                         self.tr_timeout))
        self.wf_ctrl.transfer_files(script_tr.engine_path)
        job_id = self.wf_ctrl.submit_job(Job(
            command=[self.python, script_tr],
            referenced_input_files=[script_tr],
            referenced_output_files=[],
            stdin=None,
            join_stderrout=False,
            disposal_timeout=self.jobs_timeout,
            name="job with exception"))

        return (job_id, None, None)

    def local_custom_submission(self):
        stdout = os.path.join(self.output_dir,
                              "stdout_local_custom_submission")
        stderr = os.path.join(self.output_dir,
                              "stderr_local_custom_submission")
        file11 = os.path.join(self.output_dir, "file11")
        file12 = os.path.join(self.output_dir, "file12")
        job_id = self.wf_ctrl.submit_job(Job(
            command=[self.python,
                     os.path.join(self.complete_dir, "job1.py"),
                     os.path.join(self.complete_dir, "file0"),
                     file11, file12, "2"],
            stdin=os.path.join(self.complete_dir, "stdin1"),
            join_stderrout=False,
            disposal_timeout=self.jobs_timeout,
            name="job1 local custom submission",
            stdout_file=stdout,
            stderr_file=stderr,
            working_directory=self.output_dir))

        return (job_id, [file11, file12], [stdout, stderr])

    def local_submission(self):
        file11 = os.path.join(self.output_dir, "file11")
        file12 = os.path.join(self.output_dir, "file12")
        job_id = self.wf_ctrl.submit_job(Job(
            command=[self.python,
                     os.path.join(self.complete_dir, "job1.py"),
                     os.path.join(self.complete_dir, "file0"),
                     file11, file12, "2"],
            stdin=os.path.join(self.complete_dir, "stdin1"),
            join_stderrout=False,
            disposal_timeout=self.jobs_timeout,
            name="job1 local submission"))

        return (job_id, [file11, file12], None)

    def mpi_job_submission(self, node_num):
        #compilation
        source_tr = self.wf_ctrl.register_transfer(FileTransfer(
            True,
            self.examples_dir + "mpi/simple_mpi.c",
            self.tr_timeout))

        self.wf_ctrl.transfer_files(source_tr.engine_path)

        object_tr = self.wf_ctrl.register_transfer(FileTransfer(
            False,
            self.output_dir + "simple_mpi.o",
            self.tr_timeout))
            #/volatile/laguitton/sge6-2u5/mpich/mpich-1.2.7/bin/
            #/opt/mpich/gnu/bin/

        mpibin = self.wf_ctrl.config._config_parser.get(
            self.wf_ctrl._resource_id,
            configuration.OCFG_PARALLEL_ENV_MPI_BIN)
        print "mpibin = " + mpibin

        print "source_tr.engine_path = " + source_tr.engine_path
        print "object_tr.engine_path = " + object_tr.engine_path
        compil1job_id = self.wf_ctrl.submit_job(Job(
            command=[mpibin+"/mpicc", "-c", source_tr, "-o", object_tr],
            referenced_input_files=[source_tr],
            referenced_output_files=[object_tr],
            join_stderrout=False,
            disposal_timeout=self.jobs_timeout,
            name="job compil1 mpi"))

        self.wf_ctrl.wait_job([compil1job_id])

        bin_tr = self.wf_ctrl.register_transfer(FileTransfer(
            True,
            self.output_dir + "simple_mpi",
            self.tr_timeout))
        print "bin_tr.engine_path= " + bin_tr.engine_path

        compil2job_id = self.wf_ctrl.submit_job(Job(
            command=[mpibin + "/mpicc", "-o", bin_tr, object_tr],
            referenced_input_files=[object_tr],
            referenced_output_files=[bin_tr],
            join_stderrout=False,
            disposal_timeout=self.jobs_timeout,
            name="job compil2 mpi"))

        self.wf_ctrl.wait_job([compil2job_id])
        self.wf_ctrl.delete_transfer(object_tr.engine_path)

        # mpi job submission
        script = self.wf_ctrl.register_transfer(FileTransfer(
            True,
            self.examples_dir + "mpi/simple_mpi.sh",
            self.tr_timeout))

        self.wf_ctrl.transfer_files(script.engine_path)

        job_id = self.wf_ctrl.submit_job(Job(
            command=[script, repr(node_num), bin_tr],
            referenced_input_files=[script, bin_tr],
            join_stderrout=False,
            disposal_timeout=self.jobs_timeout,
            name="parallel job mpi",
            parallel_job_info=(configuration.OCFG_PARALLEL_PC_MPI, node_num)))

        self.wf_ctrl.delete_job(compil1job_id)
        self.wf_ctrl.delete_job(compil2job_id)

        return (job_id, [source_tr.engine_path], None)
