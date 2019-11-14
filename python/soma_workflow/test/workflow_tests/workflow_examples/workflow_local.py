from __future__ import with_statement

# -*- coding: utf-8 -*-
"""
Created on Fri Oct 18 12:19:20 2013

@author: laure.hugo@cea.fr
@author: Soizic Laguitton
@organization: U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
"""
#-----------------------------------------------------------------------------
# Imports
#-----------------------------------------------------------------------------

import os
import sys

from soma_workflow.client import Job
from soma_workflow.test.workflow_tests import WorkflowExamples


#-----------------------------------------------------------------------------
# Classes and functions
#-----------------------------------------------------------------------------

class WorkflowExamplesLocal(WorkflowExamples):

    def __init__(self):
        super(WorkflowExamplesLocal, self).__init__()

# Initialize the dictionaries
        self.lo_file = {}
        self.lo_script = {}
        self.lo_stdin = {}
        self.lo_stdout = {}
        self.lo_out_model_file = {}

        # Complete path
        self.complete_path = os.path.join(self.examples_dir, "complete")
        self.lo_file[0] = os.path.join(self.complete_path, "file0")
        self.lo_file[1] = os.path.join(self.complete_path, "file1")
        self.lo_exceptionJobScript = os.path.join(self.complete_path,
                                                  "exception_job.py")
        self.lo_sleep_script = os.path.join(self.complete_path,
                                            "sleep_job.py")
        self.lo_cmd_check_script = os.path.join(self.complete_path,
                                                "special_command.py")

        # Models path
        self.models_path = os.path.join(self.complete_path, "output_models")
        self.lo_stdout_exception_model = os.path.join(
            self.models_path, "stdout_exception_job")
        self.lo_stdout_command_local = os.path.join(
            self.models_path, "stdout_local_special_command")

        for i in range(1, 10):
            self.lo_script[i] = os.path.join(self.complete_path,
                                             "job" + str(i) + ".py")
            self.lo_stdin[i] = os.path.join(self.complete_path,
                                            "stdin" + str(i))
            self.lo_stdout[i] = os.path.join(self.models_path,
                                             "stdout_job" + str(i))

        for i in [11, 12, 2, 3, 4, 13]:
            self.lo_file[i] = os.path.join(self.output_dir, "file" + str(i))
            self.lo_out_model_file[i] = os.path.join(self.models_path,
                                                     "file" + str(i))

    def job1(self, option=None):
        time_to_wait = 2
        job_name = "job1"
        job1 = Job([sys.executable,
                    self.lo_script[1], self.lo_file[0],
                    self.lo_file[11], self.lo_file[12],
                    repr(time_to_wait)],
                   None, None,
                   self.lo_stdin[1], False, 168, job_name,
                   native_specification=option)
        return job1

    def job2(self):
        time_to_wait = 2
        job_name = "job2"
        job2 = Job([sys.executable,
                    '%(script)s', '%(file1)s',
                    '%(file2)s', '%(file3)s',
                    repr(time_to_wait)],
                   None, None,
                   self.lo_stdin[2], False, 168, job_name,
                   param_dict={'script': self.lo_script[2],
                               'file1': self.lo_file[11],
                               'file2': self.lo_file[0],
                               'file3': self.lo_file[2]})
        return job2

    def job3(self):
        time_to_wait = 2
        job_name = "job3"
        job3 = Job([sys.executable,
                    '%(script)s', '%(filePathIn)s',
                    '%(filePathOut)s', '%(timeToSleep)s'],
                   None, None,
                   self.lo_stdin[3], False, 168, job_name,
                   param_dict={'script': self.lo_script[3],
                               'filePathIn': self.lo_file[12],
                               'filePathOut': self.lo_file[3],
                               'timeToSleep': str(time_to_wait)})
        return job3

    def job4(self):
        time_to_wait = 10
        job_name = "job4"
        job4 = Job([sys.executable,
                    '%(script)s', '%(file1)s',
                    '%(file2)s', '%(file3)s',
                    repr(time_to_wait)],
                   None, None,
                   self.lo_stdin[4], False, 168, job_name,
                   param_dict={'script': self.lo_script[4],
                               'file1': self.lo_file[2],
                               'file2': self.lo_file[3],
                               'file3': self.lo_file[4]})
        return job4

    def job_test_command_1(self):
        test_command = Job([sys.executable, self.lo_cmd_check_script,
                            "[13.5, 14.5, 15.0]", '[13.5, 14.5, 15.0]',
                            "['un', 'deux', 'trois']",
                            '["un", "deux", "trois"]'],
                           None, None,
                           None, False, 168, "test_command_1")
        return test_command

    def job_test_dir_contents(self):
        job = Job([sys.executable, self.lo_dir_contents_script,
                  self.lo_in_dir, self.lo_out_dir],
                  None, None,
                  None, False, 168, "dir_contents")
        return job

    def job_test_multi_file_format(self):
        job = Job([sys.executable, self.lo_mff_script,
                   self.lo_img_file, self.lo_img_out_file],
                  None, None,
                  None, False, 168, "multi file format test")
        return job

    def job_sleep(self, period):
        job = Job([sys.executable, self.lo_sleep_script, repr(period)],
                  None, None,
                  None, False, 168, "sleep " + repr(period) + " s")
        return job

    def job1_exception(self):
        job = Job([sys.executable, self.lo_exceptionJobScript],
                  None,
                  None,
                  self.lo_stdin[1], False, 168, "job1 with exception")
        return job

    def job3_exception(self):
        job = Job([sys.executable, self.lo_exceptionJobScript],
                  None,
                  None,
                  self.lo_stdin[3], False, 168, "job3 with exception")
        return job

    def job1_with_outputs1(self):
        time_to_wait = 2
        job_name = "job1_with_outputs"
        job1 = Job([sys.executable,
                    '%(script)s', '%(filePathIn)s', '%(filePathOut1)s',
                    '%(timeToSleep)s'],
                   None, None,
                   self.lo_stdin[1], False, 168, job_name,
                   param_dict={'script': self.lo_script[5],
                               'filePathIn': self.lo_file[0],
                               'filePathOut1': self.lo_file[11],
                               'timeToSleep': str(time_to_wait)},
                   has_outputs=True)
        return job1

    def job2_with_outputs1(self):
        time_to_wait = 2
        job_name = "job2_with_outputs"
        job2 = Job([sys.executable, '%(script)s'],
                   None, None,
                   self.lo_stdin[2], False, 168, job_name,
                   param_dict={'script': self.lo_script[6],
                               'filePathIn1': self.lo_file[11],
                               'filePathIn2': self.lo_file[0],
                               'timeToSleep': str(time_to_wait)},
                   has_outputs=True,
                   use_input_params_file=True)
        return job2

    def job8_with_output(self):
        time_to_wait = 1
        job_name = 'job8_with_output'
        job = Job([sys.executable, '%(script)s'],
                  None, None,
                  self.lo_stdin[2], False, 168, job_name,
                  param_dict={'script': self.lo_script[8],
                              'input': 'nothing',
                              'timeToSleep': str(time_to_wait)},
                   has_outputs=True,
                   use_input_params_file=True)
        return job

    def job_list_with_outputs(self):
        job_name = 'copy_files'
        job = Job([sys.executable, '%(script)s'],
                  None, None,
                  self.lo_stdin[2], False, 168, job_name,
                  param_dict={'script': self.lo_script[7],
                              'inputs': [self.lo_file[0], self.lo_file[1]],
                              'output_dir': os.path.join(
                                  self.output_dir, 'intermediate_results')},
                  has_outputs=True,
                  use_input_params_file=True)
        return job

    def job_reduce_cat(self):
        job_name = 'cat_files'
        job = Job([sys.executable, '%(script)s'],
                  None, None,
                  self.lo_stdin[2], False, 168, job_name,
                  param_dict={'script': self.lo_script[9],
                              'inputs': [],
                              'output': self.lo_file[13]},
                  use_input_params_file=True)
        return job

