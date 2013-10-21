from __future__ import with_statement

# -*- coding: utf-8 -*-
"""
Created on Fri Oct 18 13:38:06 2013

@author: laure
"""

from soma_workflow.client import Job
from soma_workflow.client import SharedResourcePath
from soma_workflow.test.workflow_examples import WorkflowExamples


class WorkflowExamplesShared(WorkflowExamples):

    def __init__(self,):
        '''
        The files are read and written on the computing resource.
        '''
        super(WorkflowExamplesShared, self).__init__()

        self.sh_file = {}
        self.sh_script = {}
        self.sh_stdin = {}

        # Shared resource path
        self.sh_file[0] = SharedResourcePath("complete/file0",
                                             "example", "job_dir", 168)
        self.sh_in_dir = SharedResourcePath("", "example", "job_dir", 168)
        self.sh_img_file = SharedResourcePath("special_transfers/example.img",
                                              "example", "job_dir", 168)
        self.sh_exceptionJobScript = SharedResourcePath(
            "simple/exceptionJob.py", "example",
            "job_dir", 168)
        self.sh_sleep_script = SharedResourcePath("simple/sleep_job.py",
                                                  "example", "job_dir", 168)
        self.sh_cmd_check_script = SharedResourcePath(
            "command/argument_check.py", "example",
            "job_dir", 168)
        self.sh_dir_contents_script = SharedResourcePath(
            "special_transfers/dir_contents.py", "example",
            "job_dir", 168)
        self.sh_mff_script = SharedResourcePath(
            "special_transfers/multiple_file_format.py",
            "example", "job_dir", 168)
        self.sh_out_dir = SharedResourcePath("transfered_dir",
                                             "example", "output_dir", 168)
        self.sh_img_out_file = SharedResourcePath("example.img", "example",
                                                  "output_dir", 168)

        for i in range(1, 5):
            self.sh_script[i] = SharedResourcePath(
                "complete/job" + str(i) + ".py",
                "example", "job_dir", 168)
            self.sh_stdin[i] = SharedResourcePath(
                "complete/stdin" + str(i),
                "example", "job_dir", 168)

        for i in [11, 12, 2, 3, 4]:
            self.sh_file[i] = SharedResourcePath("file" + str(i), "example",
                                                 "output_dir", 168)

    def job1(self, option=None):
        time_to_wait = 2
        job_name = "job1"
        job1 = Job(["python",
                    self.sh_script[1], self.sh_file[0],
                    self.sh_file[11], self.sh_file[12],
                    repr(time_to_wait)],
                   None, None,
                   self.sh_stdin[1], False, 168, job_name,
                   native_specification=option)
        return job1

    def job2(self):
        time_to_wait = 2
        job_name = "job2"
        job2 = Job(["python",
                    self.sh_script[2], self.sh_file[11],
                    self.sh_file[0], self.sh_file[2],
                    repr(time_to_wait)],
                   None, None,
                   self.sh_stdin[2], False, 168, job_name)
        return job2

    def job3(self):
        time_to_wait = 2
        job_name = "job3"
        job3 = Job(["python",
                    self.sh_script[3], self.sh_file[12],
                    self.sh_file[3], repr(time_to_wait)],
                   None, None,
                   self.sh_stdin[3], False, 168, job_name)
        return job3

    def job4(self):
        time_to_wait = 10
        job_name = "job4"
        job4 = Job(["python",
                    self.sh_script[4], self.sh_file[2],
                    self.sh_file[3], self.sh_file[4],
                    repr(time_to_wait)],
                   None, None,
                   self.sh_stdin[4], False, 168, job_name)
        return job4

    def job_test_command_1(self):
        test_command = Job(["python", self.sh_cmd_check_script,
                            [self.sh_script[1], self.sh_script[2],
                             self.sh_script[3]],
                            "[13.5, 14.5, 15.0]", '[13.5, 14.5, 15.0]',
                            "['un', 'deux', 'trois']",
                            '["un", "deux", "trois"]'],
                           None, None,
                           None, False, 168, "test_command_1")
        return test_command

    def job_test_dir_contents(self):
        job = Job(["python", self.sh_dir_contents_script,
                  self.sh_in_dir, self.sh_out_dir],
                  None, None,
                  None, False, 168, "dir_contents")
        return job

    def job_test_multi_file_format(self):
        job = Job(["python", self.sh_mff_script,
                   self.sh_img_file, self.sh_img_out_file],
                  None, None,
                  None, False, 168, "multi file format test")
        return job

    def job_sleep(self, period):
        job = Job(["python", self.sh_sleep_script, repr(period)],
                  None, None,
                  None, False, 168, "sleep " + repr(period) + " s")
        return job

    def job1_exception(self):
        job = Job(["python", self.sh_exceptionJobScript],
                  None,
                  None,
                  self.sh_stdin[1], False, 168, "job1 with exception")
        return job

    def job3_exception(self):
        job = Job(["python", self.sh_exceptionJobScript],
                  None,
                  None,
                  self.sh_stdin[3], False, 168, "job3 with exception")
        return job
