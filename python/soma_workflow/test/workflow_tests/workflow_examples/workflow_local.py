# -*- coding: utf-8 -*-
from __future__ import with_statement
from __future__ import absolute_import

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

from six.moves import range
import os
import sys

from soma_workflow.client import Job
from soma_workflow.test.workflow_tests import WorkflowExamples
from soma_workflow.client import SpecialPath


#-----------------------------------------------------------------------------
# Classes and functions
#-----------------------------------------------------------------------------

class WorkflowExamplesLocal(WorkflowExamples):

    def __init__(self):
        super(WorkflowExamplesLocal, self).__init__()

        self.python = os.path.basename(sys.executable)

        # Initialize the dictionaries
        self.sh_file = {}
        self.lo_file = {}
        self.sh_script = {}
        self.sh_stdin = {}
        self.tr_file = {}
        self.tr_script = {}
        self.lo_stdout = {}
#        self.lo_stderr = {}
        self.lo_out_model_file = {}

        self.lo_in_dir = self.examples_dir
        self.tr_in_dir = self.transfer_function(
            self.examples_dir, '', "example", "in_dir", 168, True)

         # Complete path
        self.complete_path = os.path.join(self.examples_dir, "complete")
        for i in [0, 1, 5]:
            self.lo_file[i] = os.path.join(
                self.examples_dir, "complete", "file%d" % i)
            self.sh_file[i] = self.shared_function(
                self.examples_dir,
                os.path.join("complete", "file%d" % i),
                "example", "file%d" % i, 168, True)
        self.tr_exceptionJobScript = self.transfer_function(
            self.examples_dir,
            os.path.join("complete", "exception_job.py"),
            "example", "exception_job", 168, True)
        self.tr_sleep_script = self.transfer_function(
            self.examples_dir,
            os.path.join("complete", "sleep_job.py"),
            "example", "sleep_job", 168, True)
        self.tr_cmd_check_script = self.transfer_function(
            self.examples_dir,
            os.path.join("complete", "special_command.py"),
            "example", "special_command", 168, True)

        # Models path
        self.models_path = os.path.join(self.complete_path, "output_models")
        self.lo_stdout_exception_model = os.path.join(
            self.models_path, "stdout_exception_job")
        self.lo_stdout_command_local = os.path.join(
            self.models_path, "stdout_local_special_command")
        self.lo_stdout_command_remote = os.path.join(
            self.models_path,
            "stdout_remote_special_command")

        # Special transfer
        self.special_path = os.path.join(self.examples_dir,
                                         "special_transfers")
        self.tr_img_file = self.transfer_function(
            self.special_path,
            "example.img",
            "example", "img_file", 128, True,
            [os.path.join(self.special_path, "example.img"),
             os.path.join(self.special_path, "example.hdr")])
        self.tr_dir_contents_script = self.transfer_function(
            self.special_path,
            "dir_contents.py",
            "example", "dir_contents", 168, True)
        self.tr_mff_script = self.transfer_function(
            self.special_path,
            "multiple_file_format.py",
            "example", "mdd_script", 168, True)
        self.lo_mff_stdout = os.path.join(
            self.special_path,
            'stdout_multiple_file_format')

        # Output dir
        self.tr_out_dir = self.transfer_function(
            self.output_dir,
            "transfered_dir",
            "example", "out_dir", 168, False)
        self.tr_img_out_file = self.transfer_function(
            self.output_dir,
            "example.img",
            "example", "img_out", 168, False,
            [os.path.join(self.output_dir, "example.img"),
             os.path.join(self.output_dir, "example.hdr")])

        for i in range(1, 10):
            self.tr_script[i] = self.transfer_function(
                self.examples_dir,
                os.path.join("complete", "job%d.py" % i),
                "example", "job%d.py" % i, 168, True)
            self.sh_script[i] = self.shared_function(
                self.examples_dir,
                os.path.join("complete", "job%d.py" % i),
                "example", "job%d.py" % i, 168, True)
            self.sh_stdin[i] = self.shared_function(
                self.examples_dir,
                os.path.join("complete", "stdin%d" % i),
                "example", "stdin%d" % i, 168, True)
            self.lo_stdout[i] = os.path.join(self.models_path,
                                             "stdout_job" + str(i))

        for i in [11, 12, 2, 3, 4, 13, 14, 15, 16, 17]:
            self.lo_file[i] = os.path.join(
                self.output_dir, "file%d" % i)
            self.sh_file[i] = self.shared_function(
                self.output_dir,
                "file%d" % i,
                "example", "file%d" % i, 168, False)
            self.tr_file[i] = self.transfer_function(
                self.output_dir,
                "file%d" % i,
                "example", "file%d" % i, 168, False)
            self.lo_out_model_file[i] = os.path.join(self.models_path,
                                                     "file" + str(i))

    def transfer_function(self, dirname, filename, namespace, uuid,
                          disposal_timeout, is_input, client_paths=None):
        ''' Default function: use raw filename
        '''
        return os.path.join(dirname, filename)

    def shared_function(self, dirname, filename, namespace, uuid,
                        disposal_timeout, is_input, client_paths=None):
        ''' Default function: use raw filename
        '''
        return os.path.join(dirname, filename)

    def shared_list(self, ids):
        l = [rid for rid in ids if isinstance(rid, SpecialPath)]
        if l:
            return l
        return None

    def job1(self, option=None):
        time_to_wait = 2
        job_name = "job1"
        job1 = Job([self.python,
                    self.sh_script[1], self.sh_file[0],
                    self.tr_file[11], self.tr_file[12],
                    repr(time_to_wait)],
                   self.shared_list([self.sh_script[1], self.sh_file[0]]),
                   self.shared_list([self.tr_file[11], self.tr_file[12]]),
                   self.sh_stdin[1], False, 168, job_name,
                   native_specification=option)
        return job1

    def job2(self):
        time_to_wait = 2
        job_name = "job2"
        job2 = Job([self.python,
                    '%(script)s', '%(file1)s',
                    '%(file2)s', '%(file3)s',
                    repr(time_to_wait)],
                   self.shared_list([self.sh_script[2], self.tr_file[11]]),
                   self.shared_list([self.sh_file[2]]),
                   self.sh_stdin[2], False, 168, job_name,
                   param_dict={'script': self.sh_script[2],
                               'file1': self.tr_file[11],
                               'file2': self.sh_file[0],
                               'file3': self.sh_file[2]})
        return job2

    def job3(self):
        time_to_wait = 2
        job_name = "job3"
        job3 = Job([self.python,
                    '%(script)s', '%(filePathIn)s',
                    '%(filePathOut)s', '%(timeToSleep)s'],
                   self.shared_list([self.sh_script[3], self.tr_file[12]]),
                   self.shared_list([self.tr_file[3]]),
                   self.sh_stdin[3], False, 168, job_name,
                   param_dict={'script': self.sh_script[3],
                               'filePathIn': self.tr_file[12],
                               'filePathOut': self.tr_file[3],
                               'timeToSleep': str(time_to_wait)})
        return job3

    def job4(self):
        time_to_wait = 5
        job_name = "job4"
        job4 = Job([self.python,
                    '%(script)s', '%(file1)s',
                    '%(file2)s', '%(file3)s',
                    repr(time_to_wait)],
                   self.shared_list([self.sh_script[4], self.sh_file[2],
                                     self.tr_file[3]]),
                   self.shared_list([self.tr_file[4]]),
                   self.sh_stdin[4], False, 168, job_name,
                   param_dict={'script': self.sh_script[4],
                               'file1': self.sh_file[2],
                               'file2': self.tr_file[3],
                               'file3': self.tr_file[4]})
        return job4

    def job_test_command_1(self):
        test_command = Job([self.python, self.tr_cmd_check_script,
                            "[13.5, 14.5, 15.0]", '[13.5, 14.5, 15.0]',
                            "['un', 'deux', 'trois']",
                            '["un", "deux", "trois"]'],
                           self.shared_list([
                                            self.tr_cmd_check_script, self.tr_script[
                                                1],
                                            self.tr_script[
                                                2], self.tr_script[3]]),
                           None,
                           None, False, 168, "test_command_1")
        return test_command

    def job_test_dir_contents(self):
        job = Job([self.python, self.tr_dir_contents_script,
                  self.tr_in_dir, self.tr_out_dir],
                  self.shared_list([self.tr_dir_contents_script,
                                    self.tr_in_dir]),
                  self.shared_list([self.tr_out_dir]),
                  None, False, 168, "dir_contents")
        return job

    def job_test_multi_file_format(self):
        job = Job([self.python, self.tr_mff_script,
                   (self.tr_img_file, 'example.img'),
                   (self.tr_img_out_file, 'exampleout.img')],
                  self.shared_list([self.tr_mff_script, self.tr_img_file]),
                  self.shared_list([self.tr_img_out_file]),
                  None, False, 168, "multi file format test")
        return job

    def job_sleep(self, period):
        job = Job([self.python, self.tr_sleep_script, repr(period)],
                  self.shared_list([self.tr_sleep_script]),
                  None,
                  None, False, 168, "sleep " + repr(period) + " s")
        return job

    def job1_exception(self):
        job = Job([self.python, self.tr_exceptionJobScript],
                  self.shared_list([self.tr_exceptionJobScript]),
                  self.shared_list([self.tr_file[11], self.tr_file[12]]),
                  self.sh_stdin[1], False, 168, "job1 with exception")
        return job

    def job3_exception(self):
        job = Job([self.python, self.tr_exceptionJobScript],
                  self.shared_list([self.tr_exceptionJobScript]),
                  self.shared_list([self.tr_file[3]]),
                  self.sh_stdin[3], False, 168, "job3 with exception")
        return job

    def job1_with_outputs1(self):
        time_to_wait = 2
        job_name = "job1_with_outputs"
        job1 = Job([self.python,
                    '%(script)s', '%(script_job1)s', '%(filePathIn)s',
                    '%(filePathOut1)s', '%(timeToSleep)s'],
                   self.shared_list([self.sh_script[1], self.sh_script[5],
                                     self.sh_file[0]]),
                   self.shared_list([self.tr_file[11], self.tr_file[12]]),
                   self.sh_stdin[1], False, 168, job_name,
                   param_dict={'script': self.sh_script[5],
                               'script_job1': self.sh_script[1],
                               'filePathIn': self.sh_file[0],
                               'filePathOut1': self.tr_file[11],
                               'timeToSleep': str(time_to_wait),
                               'filePathOut2': self.tr_file[12],
                               'output_directory': self.output_dir},
                   has_outputs=True)
        return job1

    def job2_with_outputs1(self):
        time_to_wait = 2
        job_name = "job2_with_outputs"
        job2 = Job([self.python, '%(script)s'],
                   self.shared_list([self.sh_script[2], self.sh_script[6],
                                     self.tr_file[11], self.sh_file[0]]),
                   self.shared_list([self.sh_file[2]]),
                   self.sh_stdin[2], False, 168, job_name,
                   param_dict={'script': self.sh_script[6],
                               'script_job2': self.sh_script[2],
                               'filePathIn1': self.tr_file[11],
                               'filePathIn2': self.sh_file[0],
                               'timeToSleep': str(time_to_wait),
                               'filePathOut': self.sh_file[2]},
                   has_outputs=True,
                   use_input_params_file=True)
        return job2

    def job8_with_output(self):
        time_to_wait = 1
        job_name = 'job8_with_output'
        job = Job([self.python, '%(script)s'],
                  self.shared_list([self.sh_script[8]]),
                  self.shared_list([]),
                  None, False, 168, job_name,
                  param_dict={'script': self.sh_script[8],
                              'input': 'nothing'},
                  has_outputs=True,
                  use_input_params_file=True)
        return job

    def job_list_with_outputs(self):
        job_name = 'copy_files'
        job = Job([self.python, '%(script)s'],
                  self.shared_list([self.sh_script[7], self.sh_file[0],
                                    self.sh_file[1]]),
                  None,
                  None, False, 168, job_name,
                  param_dict={'script': self.sh_script[7],
                              'inputs': [self.sh_file[0], self.sh_file[1]],
                              'output_dir': os.path.join(
                                  self.output_dir, 'intermediate_results')},
                  has_outputs=True,
                  use_input_params_file=True)
        return job

    def job_list_with_outputs2(self):
        job_name = 'copy_files2'
        job = Job([self.python, '%(script)s'],
                  self.shared_list([self.sh_script[7], self.sh_file[0],
                                    self.sh_file[1], self.sh_file[5]]),
                  None,
                  None, False, 168, job_name,
                  param_dict={'script': self.sh_script[7],
                              'inputs': [self.sh_file[0], self.sh_file[1],
                                         self.sh_file[5], self.sh_file[0],
                                         self.sh_file[1], self.sh_file[5]],
                              'output_dir': os.path.join(
                                  self.output_dir, 'intermediate_results')},
                  has_outputs=True,
                  use_input_params_file=True)
        return job

    def job_reduce_cat(self, file_num=13):
        job_name = 'cat_files'
        job = Job([self.python, '%(script)s'],
                  self.shared_list([self.sh_script[9]]),
                  self.shared_list([self.sh_file[file_num]]),
                  None, False, 168, job_name,
                  param_dict={'script': self.sh_script[9],
                              'inputs': [],
                              'output': self.sh_file[file_num]},
                  use_input_params_file=True)
        return job
