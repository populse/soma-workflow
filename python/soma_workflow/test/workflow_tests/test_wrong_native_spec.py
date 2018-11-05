from __future__ import with_statement
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 25 09:19:23 2013

@author: laure.hugo@cea.fr
@author: Soizic Laguitton
@organization: U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}

Workflow test of a wrong native spec of the PBS:

* Workflow constitued of 3 jobs : job1, with walltime and pmem options (and
                                    a syntax error in the options)
                                  job2, with walltime option
                                  job3, no options
* Dependencies : no dependencies
* Allowed configurations : Light mode - Local path
                           Local mode - Local path
                           Remote mode - File Transfer
                           Remote mode - Shared Resource Path (SRP)
                           Remote mode - File Transfer and SRP
* Expected comportment on remote mode : job1 is aborted
                                        job2, job3 succeed
* /!\ The jobs are all successful in local and light mode, because the PBS
    native specs are ignored in those modes
* Tests : final status of the workflow
          number of failed jobs (excluding aborted)
          number of failed jobs (including aborted)
"""

import sys
from soma_workflow.client import Helper
from soma_workflow.configuration import LIGHT_MODE
from soma_workflow.configuration import REMOTE_MODE
from soma_workflow.configuration import LOCAL_MODE
import soma_workflow.constants as constants
from soma_workflow.test.workflow_tests import WorkflowTest


class WrongNativeSpecPbsTest(WorkflowTest):

    allowed_config = [(LIGHT_MODE, WorkflowTest.LOCAL_PATH),
                      (LOCAL_MODE, WorkflowTest.LOCAL_PATH),
                      (REMOTE_MODE, WorkflowTest.FILE_TRANSFER),
                      (REMOTE_MODE, WorkflowTest.SHARED_RESOURCE_PATH),
                      (REMOTE_MODE, WorkflowTest.SHARED_TRANSFER)]

    def test_result(self):
        workflow = self.wf_examples.example_wrong_native_spec_pbs()
        self.wf_id = self.wf_ctrl.submit_workflow(
            workflow=workflow,
            name=self.__class__.__name__)
        # Transfer input files if file transfer
        if self.path_management == self.FILE_TRANSFER or \
                self.path_management == self.SHARED_TRANSFER:
            Helper.transfer_input_files(self.wf_id,
                                        self.wf_ctrl)
        # Wait for the workflow to finish
        Helper.wait_workflow(self.wf_id, self.wf_ctrl)
        # Transfer output files if file transfer
        if self.path_management == self.FILE_TRANSFER or \
                self.path_management == self.SHARED_TRANSFER:
            Helper.transfer_output_files(self.wf_id,
                                         self.wf_ctrl)

        status = self.wf_ctrl.workflow_status(self.wf_id)
        self.assertTrue(status == constants.WORKFLOW_DONE,
                        "workflow status : %s. Expected : %s" %
                        (status, constants.WORKFLOW_DONE))
        nb_failed_jobs = len(Helper.list_failed_jobs(
            self.wf_id,
            self.wf_ctrl))
        self.assertTrue(nb_failed_jobs == 0,
                        "nb failed jobs : %i. Expected : %i" %
                        (nb_failed_jobs, 0))
        nb_failed_aborted_jobs = len(Helper.list_failed_jobs(
            self.wf_id,
            self.wf_ctrl,
            include_aborted_jobs=True))
        if self.path_management == self.LOCAL_PATH:
            self.assertTrue(nb_failed_aborted_jobs == 0,
                            "nb failed jobs including aborted : %i. "
                            "Expected : %i" % (nb_failed_aborted_jobs, 0))
        else:
            self.assertTrue(nb_failed_aborted_jobs == 1,
                            "nb failed jobs including aborted : %i. "
                            "Expected : %i" % (nb_failed_aborted_jobs, 1))


def test():
    return WrongNativeSpecPbsTest.run_test_function(
        **WorkflowTest.parse_args(sys.argv))

if __name__ == '__main__':
    WrongNativeSpecPbsTest.run_test(**WorkflowTest.parse_args(sys.argv))
