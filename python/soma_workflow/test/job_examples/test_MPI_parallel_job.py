# -*- coding: utf-8 -*-
"""
Created on Mon Oct 28 15:12:43 2013

@author: laure
"""
import sys

import soma_workflow.constants as constants
from soma_workflow.test.job_examples.jobs_test import JobsTest
from soma_workflow.configuration import LIGHT_MODE
from soma_workflow.configuration import LOCAL_MODE
from soma_workflow.configuration import REMOTE_MODE


class MPIParallelJobTest(JobsTest):
    '''
    Submission of a parallel job (MPI)
    '''
    allowed_resources = [LIGHT_MODE, LOCAL_MODE, REMOTE_MODE]

    def setUp(self):
        self.my_jobs = []
        self.my_transfers = []
        self.node_num = 4
        info = JobsTest.job_examples.mpi_job_submission(node_num=self.node_num)
        self.my_jobs.append(info[0])
        self.output_files = info[1]

    def tearDown(self):
        JobsTest.tearDown(self)
        # for file in self.output_files:
            #if os.path.isfile(file): os.remove(file)

    def test_result(self):
        jobid = self.my_jobs[0]
        JobsTest.wf_ctrl.wait_job(self.my_jobs)

        status = JobsTest.wf_ctrl.job_status(jobid)
        self.failUnless(status == constants.DONE,
                        'Job %s status after wait: %s' % (jobid, status))
        job_termination_status = JobsTest.wf_ctrl.job_termination_status(jobid)
        exit_status = job_termination_status[0]
        self.failUnless(exit_status == constants.FINISHED_REGULARLY,
                        'Job %s exit status: %s' % (jobid, exit_status))
        exit_value = job_termination_status[1]
        self.failUnless(exit_value == 0,
                        'Job exit value: %d' % exit_value)

        print "stdout: "
        line = JobsTest.wf_ctrl.stdoutReadLine(jobid)
        process_num = 1
        while line:
            splitted_line = line.split()
            if splitted_line[0] == "Grettings":
                self.failUnless(line.rstrip() == "Grettings from process %d!" %
                                (process_num),
                                "stdout line:  %sinstead of  : "
                                "'Grettings from process %d!'" %
                                (line, process_num))
                process_num = process_num + 1
            line = JobsTest.wf_ctrl.stdoutReadLine(jobid)

        self.failUnless(process_num == self.node_num,
                        "%d process(es) run instead of %d." %
                        (process_num - 1, self.node_num))


if __name__ == '__main__':
    MPIParallelJobTest.run_test(debug=False)
    sys.exit(0)
