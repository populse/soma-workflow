from __future__ import with_statement
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 25 14:03:52 2013

@author: laure

Workflow test of multiple examples:
* Test constitued of 3 workflows : cf test_exception1, test_exception2
                                   and test_simple
* Allowed configurations : Light mode - Local path
                           Local mode - Local path
                           Remote mode - File Transfer
                           Remote mode - Shared Resource Path (SRP)
                           Remote mode - File Transfer and SRP
* Expected comportment : for each workflow, cf corresponding test
* Outcome independant of the configuration
"""

from soma_workflow.client import Helper
from soma_workflow.configuration import LIGHT_MODE
from soma_workflow.configuration import REMOTE_MODE
from soma_workflow.configuration import LOCAL_MODE
import soma_workflow.constants as constants
from soma_workflow.test.utils import identicalFiles
from soma_workflow.test.examples.workflow_test import WorkflowTest


class MultipleTest(WorkflowTest):

    allowed_config = [(LIGHT_MODE, WorkflowTest.LOCAL_PATH),
                      (LOCAL_MODE, WorkflowTest.LOCAL_PATH),
                      (REMOTE_MODE, WorkflowTest.FILE_TRANSFER),
                      (REMOTE_MODE, WorkflowTest.SHARED_RESOURCE_PATH),
                      (REMOTE_MODE, WorkflowTest.SHARED_TRANSFER)]

    def test_result(self):
        workflow = MultipleTest.wf_examples.example_multiple()
        self.wf_id = MultipleTest.wf_ctrl.submit_workflow(
            workflow=workflow,
            name=self.__class__.__name__)

        # Transfer input files if file transfer
        if self.path_management == MultipleTest.FILE_TRANSFER or \
                self.path_management == MultipleTest.SHARED_TRANSFER:
            Helper.transfer_input_files(self.wf_id, MultipleTest.wf_ctrl)

        # Wait for the workflow to finish
        Helper.wait_workflow(self.wf_id, MultipleTest.wf_ctrl)

        # Transfer output files if file transfer
        if self.path_management == MultipleTest.FILE_TRANSFER or \
                self.path_management == MultipleTest.SHARED_TRANSFER:
            Helper.transfer_output_files(self.wf_id, MultipleTest.wf_ctrl)

        status = self.wf_ctrl.workflow_status(self.wf_id)
        self.assertTrue(status == constants.WORKFLOW_DONE)
        self.assertTrue(len(Helper.list_failed_jobs(
                        self.wf_id,
                        MultipleTest.wf_ctrl)) == 2)
        self.assertTrue(len(Helper.list_failed_jobs(
                        self.wf_id,
                        MultipleTest.wf_ctrl,
                        include_aborted_jobs=True)) == 6)
#        fail_jobs = Helper.list_failed_jobs(self.wf_id, MultipleTest.wf_ctrl)

#    num_list_fail_jobs=len(fail_jobs)
#    print "num_list_fail_jobs=" + repr(num_list_fail_jobs)
#    for fail_job_id in fail_jobs:
#        print "fail job id :" +repr(fail_job_id)+"\n"

        (jobs_info, transfers_info, workflow_status, workflow_queue) = \
            MultipleTest.wf_ctrl.workflow_elements_status(self.wf_id)
#    print "len(jobs_info)=" + repr(len(jobs_info)) + "\n"

        # TODO: check the stdout and stderrr
        for (job_id, tmp_status, queue, exit_info, dates) in jobs_info:
#        print "job_id="         +repr(job_id)+"\n"
            job_list = self.wf_ctrl.jobs([job_id])
            # print 'len(job_list)='+repr(len(job_list))+"\n"
            job_name, job_command, job_submission_date = job_list[job_id]
#        print "name="			+repr(job_name)+"\n"
#        print "command="        +repr(job_command)+"\n"
#        print "submission="     +repr(job_submission_date)+"\n"
#        print "tmp_status="     +repr(tmp_status)+"\n"
#        print "exit_info="		+repr(exit_info)+"\n"
#        print "dates="			+repr(dates)+"\n"

            if exit_info[0] == constants.FINISHED_REGULARLY:
                # To check job standard out
                # print "Verify "+repr(job_name)+" \n"
                job_stdout_file = "/tmp/job_soma_out_log_" + repr(job_id)
                job_stderr_file = "/tmp/job_soma_outerr_log_" + repr(job_id)
                self.wf_ctrl.retrieve_job_stdouterr(job_id,
                                                    job_stdout_file,
                                                    job_stderr_file)
                if job_name == 'job1':
                    isSame, msg = identicalFiles(
                        job_stdout_file,
                        MultipleTest.wf_examples.lo_stdout[1])
                    self.assertTrue(isSame)
                    if self.path_management == MultipleTest.LOCAL_PATH:
                        isSame, msg = identicalFiles(
                            MultipleTest.wf_examples.lo_out_model_file[11],
                            MultipleTest.wf_examples.lo_file[11])
                        self.assertTrue(isSame)
                        isSame, msg = identicalFiles(
                            MultipleTest.wf_examples.lo_out_model_file[12],
                            MultipleTest.wf_examples.lo_file[12])
                        self.assertTrue(isSame)
                        isSame, msg = identicalFiles(
                            job_stderr_file,
                            MultipleTest.wf_examples.lo_stderr[1])
                        self.assertTrue(isSame)
                    if self.path_management == MultipleTest.FILE_TRANSFER:
                        isSame, msg = identicalFiles(
                            MultipleTest.wf_examples.lo_out_model_file[11],
                            MultipleTest.wf_examples.tr_file[11].client_path)
                        self.assertTrue(isSame)
                        isSame, msg = identicalFiles(
                            MultipleTest.wf_examples.lo_out_model_file[12],
                            MultipleTest.wf_examples.tr_file[12].client_path)
                        self.assertTrue(isSame)
                        # For unknown reason, it raises some errors
                        # http://stackoverflow.com/questions/10496758/unexpected-end-of-file-and-error-importing-function-definition-error-running
                        #isSame,	msg	= identicalFiles(job_stderr_file,MultipleTest.wf_examples.lo_stderr[1])
                        #self.failUnless(isSame == True)

                if job_name in ['job2', 'job3', 'job4']:
                    job_nb = int(job_name[3])
                    isSame, msg = identicalFiles(
                        job_stdout_file,
                        MultipleTest.wf_examples.lo_stdout[job_nb])
                    self.assertTrue(isSame)
                    if self.path_management == MultipleTest.LOCAL_PATH:
                        isSame, msg = identicalFiles(
                            MultipleTest.wf_examples.lo_out_model_file[job_nb],
                            MultipleTest.wf_examples.lo_file[job_nb])
                        self.assertTrue(isSame)
                        isSame, msg = identicalFiles(
                            job_stderr_file,
                            MultipleTest.wf_examples.lo_stderr[job_nb])
                        self.assertTrue(isSame)
                    if self.path_management == MultipleTest.FILE_TRANSFER:
                        isSame, msg = identicalFiles(
                            MultipleTest.wf_examples.lo_out_model_file[job_nb],
                            MultipleTest.wf_examples.tr_file[job_nb].client_path)
                        self.assertTrue(isSame)

                if job_name in ['job1 with exception',
                                'job3 with exception']:
                    isSame, msg = identicalFiles(
                        job_stdout_file,
                        MultipleTest.wf_examples.lo_stdout1_exception_model)
                    self.assertTrue(isSame)


if __name__ == '__main__':
    MultipleTest.run_test(debug=False)
