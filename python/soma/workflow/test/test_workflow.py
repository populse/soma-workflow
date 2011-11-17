from __future__ import with_statement 

'''
@author: Soizic Laguitton
@organization: U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''


#-----------------------------------------------------------------------------
# Imports
#-----------------------------------------------------------------------------

import unittest
import os
import getpass
import sys

from soma.workflow.client import Job, SharedResourcePath, FileTransfer, Group, Workflow, WorkflowController, Helper
from soma.workflow.configuration import Configuration
from soma.workflow.errors import ConfigurationError, UnknownObjectError
import soma.workflow.constants as constants


#-----------------------------------------------------------------------------
# Classes and functions
#-----------------------------------------------------------------------------




class WorkflowExamples(object):
  
  @staticmethod
  def get_workflow_example_list():
    return ["simple", "multiple", "with exception 1", "with exception 2", "command check test", "special transfers", "hundred of jobs", "ten jobs", "fake pipelineT1", "serial", "hundred with dependencies"]
  
  def __init__(self, with_tranfers, with_shared_resource_path = False):
    '''
    @type with_tranfers: boolean
    @type with_shared_resource_path: boolean

    if with_transfer and not with_shared_resource_path:
      The input and ouput files are temporary files on the computing resource
      and these files can be transfered from and to the computing resource
      using soma workflow API

    if with_shared_resource_path and not with_transfers:
      The files are read and written on the computing resource.

    if with_shared_resource_path and with_transfer:
      The files are read from data located on the computing resource but the output
      will be written in temporary files and transfered to the computing resource.
      
    '''
    self.examples_dir = os.environ.get("SOMA_WORKFLOW_EXAMPLES")
    self.output_dir = os.environ.get("SOMA_WORKFLOW_EXAMPLES_OUT")

    if not self.examples_dir or not self.output_dir:
      raise ConfigurationError("The environment variables SOMA_WORKFLOW_EXAMPLES "  
                               "and SOMA_WORKFLOW_EXAMPLES_OUT must be set.")
    
    if not os.path.isdir(self.output_dir):
      os.mkdir(self.output_dir)
       
    self.with_transfers = with_tranfers
    self.with_shared_resource_path = with_shared_resource_path
    
    # local path
    
    complete_path = os.path.join(self.examples_dir, "complete")
    self.lo_in_dir = self.examples_dir
    self.lo_img_file = os.path.join(self.examples_dir, "special_transfers/example.img")
    self.lo_file0   = os.path.join(complete_path, "file0") 
    self.lo_script1 = os.path.join(complete_path, "job1.py")
    self.lo_stdin1  = os.path.join(complete_path, "stdin1")
    self.lo_script2 = os.path.join(complete_path, "job2.py")
    self.lo_stdin2  = os.path.join(complete_path, "stdin2")
    self.lo_script3 = os.path.join(complete_path, "job3.py")
    self.lo_stdin3  = os.path.join(complete_path, "stdin3")
    self.lo_script4 = os.path.join(complete_path, "job4.py")
    self.lo_stdin4  = os.path.join(complete_path, "stdin4")
    self.lo_exceptionJobScript = os.path.join(self.examples_dir, 
                                              "simple/exceptionJob.py")
    self.lo_cmd_check_script = os.path.join(self.examples_dir, 
                                            "command/argument_check.py")
    self.lo_sleep_script = os.path.join(self.examples_dir, "simple/sleep_job.py")
                                            
    self.lo_dir_contents_script = os.path.join(self.examples_dir, 
                                            "special_transfers/dir_contents.py")
    self.lo_mff_script = os.path.join(self.examples_dir, "special_transfers/multiple_file_format.py")
    
    self.lo_file11 = os.path.join(self.output_dir, "file11")
    self.lo_file12 = os.path.join(self.output_dir, "file12")
    self.lo_file2 = os.path.join(self.output_dir, "file2")
    self.lo_file3 = os.path.join(self.output_dir, "file3")
    self.lo_file4 = os.path.join(self.output_dir, "file4")
    self.lo_out_dir = os.path.join(self.output_dir, "transfered_dir")
    self.lo_img_out_file = os.path.join(self.output_dir, "example.img")
  
    # Shared resource path
    self.sh_in_dir = SharedResourcePath("", "example", "job_dir", 168)
    self.sh_img_file = SharedResourcePath("special_transfers/example.img", "example", "job_dir", 168)
    self.sh_file0   = SharedResourcePath("complete/file0", "example", "job_dir", 168)
    self.sh_script1 = SharedResourcePath("complete/job1.py", "example", "job_dir", 168)
    self.sh_stdin1  = SharedResourcePath("complete/stdin1", "example", "job_dir", 168)
    self.sh_script2 = SharedResourcePath("complete/job2.py", "example", "job_dir", 168)
    self.sh_stdin2  = SharedResourcePath("complete/stdin2", "example", "job_dir", 168)
    self.sh_script3 = SharedResourcePath("complete/job3.py", "example", "job_dir", 168)
    self.sh_stdin3  = SharedResourcePath("complete/stdin3", "example", "job_dir", 168)
    self.sh_script4 = SharedResourcePath("complete/job4.py", "example", "job_dir", 168)
    self.sh_stdin4  = SharedResourcePath("complete/stdin4", "example", "job_dir", 168)
    self.sh_exceptionJobScript = SharedResourcePath("simple/exceptionJob.py", 
                                                    "example", 
                                                    "job_dir", 168)
    self.sh_cmd_check_script = SharedResourcePath("command/argument_check.py",
                                                  "example",
                                                  "job_dir", 168)
    self.sh_sleep_script = SharedResourcePath("simple/sleep_job.py", 
                                              "example", 
                                              "job_dir", 168)
    self.sh_dir_contents_script = SharedResourcePath("special_transfers/dir_contents.py",
                                                     "example",
                                                     "job_dir", 168)
    self.sh_mff_script = SharedResourcePath("special_transfers/multiple_file_format.py", 
                       "example", "job_dir", 168)
   
    
    self.sh_file11 = SharedResourcePath("file11", "example", "output_dir", 168)
    self.sh_file12 = SharedResourcePath("file12", "example", "output_dir",168)
    self.sh_file2 = SharedResourcePath("file2", "example", "output_dir",168)
    self.sh_file3 = SharedResourcePath("file3", "example", "output_dir",168)
    self.sh_file4 = SharedResourcePath("file4", "example", "output_dir",168)
    self.sh_out_dir = SharedResourcePath("transfered_dir", "example", "output_dir", 168) 
    self.sh_img_out_file = SharedResourcePath("example.img", "example", "output_dir", 168)

    # Transfers
    
    complete_path = os.path.join(self.examples_dir, "complete")
    self.tr_in_dir = FileTransfer(True, self.examples_dir, 168, "in_dir")
    self.tr_img_file = FileTransfer(True, 
                                    os.path.join(self.examples_dir, 
                                                 "special_transfers/dir_contents.py"), 
                                    128, 
                                    "img_file", 
                                    [ os.path.join(self.examples_dir, "special_transfers/example.img"),
                                      os.path.join(self.examples_dir, "special_transfers/example.hdr")])
    self.tr_file0   = FileTransfer(True,os.path.join(complete_path, "file0"), 168, "file0")
    self.tr_script1 = FileTransfer(True,os.path.join(complete_path, "job1.py"), 168, "job1_py")
    self.tr_stdin1  = FileTransfer(True,os.path.join(complete_path, "stdin1"), 168, "stdin1")
    self.tr_script2 = FileTransfer(True,os.path.join(complete_path, "job2.py"), 168, "job2_py")
    self.tr_stdin2  = FileTransfer(True,os.path.join(complete_path, "stdin2"), 168, "stdin2")
    self.tr_script3 = FileTransfer(True,os.path.join(complete_path, "job3.py"), 168, "job3_py")
    self.tr_stdin3  = FileTransfer(True,os.path.join(complete_path, "stdin3"), 168, "stdin3")
    self.tr_script4 = FileTransfer(True,os.path.join(complete_path, "job4.py"), 168, "job4_py")
    self.tr_stdin4  = FileTransfer(True,os.path.join(complete_path, "stdin4"), 168, "stdin4")
    self.tr_exceptionJobScript = FileTransfer(True,os.path.join(self.examples_dir, 
                                                          "simple/exceptionJob.py"), 
                                                          168, "exception_job")
    self.tr_cmd_check_script = FileTransfer(True,os.path.join(self.examples_dir, 
                                                        "command/argument_check.py"),
                                                        168, "cmd_check")
    self.tr_sleep_script = FileTransfer(True,os.path.join(self.examples_dir, 
                                                    "simple/sleep_job.py"), 
                                                     168, "sleep_job")
    self.tr_dir_contents_script = FileTransfer(True,os.path.join(self.examples_dir, 
                                                        "special_transfers/dir_contents.py"),
                                                        168, "dir_contents")
    self.tr_mff_script = FileTransfer(True, os.path.join(self.examples_dir, "special_transfers/multiple_file_format.py"), 168, "mdd_script")
      
    self.tr_file11 = FileTransfer(False,os.path.join(self.output_dir, "file11"), 168, "file11")
    self.tr_file12 = FileTransfer(False,os.path.join(self.output_dir, "file12"), 168, "file12")
    self.tr_file2 = FileTransfer(False,os.path.join(self.output_dir, "file2"), 168, "file2")
    self.tr_file3 = FileTransfer(False,os.path.join(self.output_dir, "file3"), 168, "file3")
    self.tr_file4 = FileTransfer(False,os.path.join(self.output_dir, "file4"), 168, "file4")
    self.tr_out_dir = FileTransfer(False, os.path.join(self.output_dir, "transfered_dir"), 168, "out_dir")
    self.tr_img_out_file = FileTransfer(False, 
                                        os.path.join(self.output_dir, "example.img"), 
                                        168, 
                                        "img_out",
                                        [os.path.join(self.output_dir, "example.img"),
                                         os.path.join(self.output_dir, "example.hdr")])
  
      
  def get_workflow_example(self, example_index):
    workflow = None
    if example_index == 0:
      workflow = self.simpleExample()
    elif example_index == 1:
      workflow = self.multipleSimpleExample()
    elif example_index == 2:
      workflow = self.simpleExampleWithException1()
    elif example_index == 3:
      workflow = self.simpleExampleWithException2()
    elif example_index == 4:
      workflow = self.command_test()
    elif example_index == 5:
      workflow = self.special_transfer_test()
    elif example_index == 6:
      workflow = self.n_jobs(500)
    elif example_index == 7:
      workflow = self.n_jobs(50)
    elif example_index == 8:
      workflow = self.fake_pipelineT1()
    elif example_index == 9:
      workflow = self.serial_jobs()
    elif example_index == 10:
      workflow = self.n_jobs_with_dependencies(500)
    return workflow

  def job1(self):
    if self.with_transfers and not self.with_shared_resource_path: 
      job1 = Job(["python", self.tr_script1, self.tr_file0,  self.tr_file11, self.tr_file12, "20"], 
                  [self.tr_file0, self.tr_script1, self.tr_stdin1], 
                  [self.tr_file11, self.tr_file12], 
                  self.tr_stdin1, False, 168, "job1")
    elif self.with_transfers and self.with_shared_resource_path:
      job1 = Job( ["python", self.sh_script1, self.sh_file0,  self.tr_file11, self.tr_file12, "20"], 
                  [], 
                  [self.tr_file11, self.tr_file12], 
                  self.sh_stdin1, False, 168, "job1")
    elif not self.with_transfers and self.with_shared_resource_path:
      job1 = Job( ["python", self.sh_script1, self.sh_file0,  self.sh_file11, self.sh_file12, "20"], 
                  None, 
                  None, 
                  self.sh_stdin1, False, 168, "job1")
    else:
      job1 = Job( ["python", self.lo_script1, self.lo_file0,  self.lo_file11, self.lo_file12, "20"], 
                  None, 
                  None, 
                  self.lo_stdin1,  False, 168, "job1")
    return job1
    
  def job2(self):
    if self.with_transfers and not self.with_shared_resource_path:
      job2 = Job( ["python", self.tr_script2, self.tr_file11,  self.tr_file0, self.tr_file2, "30"], 
                  [self.tr_file0, self.tr_file11, self.tr_script2, self.tr_stdin2], 
                  [self.tr_file2], 
                  self.tr_stdin2, False, 168, "job2")
    elif self.with_transfers and self.with_shared_resource_path:
      job2 = Job( ["python", self.sh_script2, self.sh_file11,  self.sh_file0, self.tr_file2, "30"], 
                  [], 
                  [self.tr_file2], 
                  self.sh_stdin2, False, 168, "job2")
    elif not self.with_transfers and self.with_shared_resource_path:
      job2 = Job( ["python", self.sh_script2, self.sh_file11,  self.sh_file0, self.sh_file2, "30"], 
                  None, 
                  None, 
                  self.sh_stdin2, False, 168, "job2")
    else:
      job2 = Job( ["python", self.lo_script2, self.lo_file11,  self.lo_file0, self.lo_file2, "30"], 
                  None, 
                  None, 
                  self.lo_stdin2, False, 168, "job2")
    return job2
    
  def job3(self):
    if self.with_transfers and not self.with_shared_resource_path:
      job3 = Job( ["python", self.tr_script3, self.tr_file12,  self.tr_file3, "30"], 
                  [self.tr_file12, self.tr_script3, self.tr_stdin3], 
                  [self.tr_file3], 
                  self.tr_stdin3, False, 168, "job3")
    elif self.with_transfers and self.with_shared_resource_path:
      job3 = Job( ["python", self.sh_script3, self.sh_file12,  self.tr_file3, "30"], 
                  [], 
                  [self.tr_file3], 
                  self.sh_stdin3, False, 168, "job3")
    elif not self.with_transfers and self.with_shared_resource_path:
      job3 = Job( ["python", self.sh_script3, self.sh_file12,  self.sh_file3, "30"], 
                  None, 
                  None, 
                  self.sh_stdin3, False, 168, "job3")
    else:
      job3 = Job( ["python", self.lo_script3, self.lo_file12,  self.lo_file3, "30"], 
                  None, 
                  None, 
                  self.lo_stdin3,  False, 168, "job3")
    return job3
    
  def job4(self):
    if self.with_transfers and not self.with_shared_resource_path:
      job4 = Job( ["python", self.tr_script4, self.tr_file2,  self.tr_file3, self.tr_file4, "10"], 
                  [self.tr_file2, self.tr_file3, self.tr_script4, self.tr_stdin4], 
                  [self.tr_file4], 
                  self.tr_stdin4, False, 168, "job4")
    elif self.with_transfers and self.with_shared_resource_path:
      job4 = Job( ["python", self.sh_script4, self.sh_file2,  self.sh_file3, self.tr_file4, "10"], 
                  [], 
                  [self.tr_file4], 
                  self.sh_stdin4, False, 168, "job4")
    elif not self.with_transfers and self.with_shared_resource_path:
      job4 = Job( ["python", self.sh_script4, self.sh_file2,  self.sh_file3, self.sh_file4, "10"], 
                  None, 
                  None, 
                  self.sh_stdin4, False, 168, "job4")
    else:
      job4 = Job( ["python", self.lo_script4, self.lo_file2,  self.lo_file3, self.lo_file4, "10"], 
                  None, 
                  None, 
                  self.lo_stdin4, False, 168, "job4")
    return job4
    
  def job_test_command_1(self):
    if self.with_transfers:
      test_command = Job( ["python", 
                           self.tr_cmd_check_script,
                           [self.tr_script1, self.tr_script2, self.tr_script3], 
                           "[13.5, 14.5, 15.0]",
                           '[13.5, 14.5, 15.0]',
                           "['un', 'deux', 'trois']",
                           '["un", "deux", "trois"]'],
                          [self.tr_cmd_check_script, self.tr_script1, self.tr_script2, self.tr_script3],
                          [],
                          None, False, 168, "test_command_1")
    elif self.with_shared_resource_path:
      test_command = Job( ["python", self.sh_cmd_check_script,
                           [self.sh_script1, self.sh_script2, self.sh_script3],
                           "[13.5, 14.5, 15.0]",
                           '[13.5, 14.5, 15.0]',
                           "['un', 'deux', 'trois']",
                           '["un", "deux", "trois"]'],
                          None,
                          None,
                          None, False, 168, "test_command_1")
    else:
      test_command = Job( ["python", self.lo_cmd_check_script, 
                           [self.lo_script1, self.lo_script2, self.lo_script3],
                           "[13.5, 14.5, 15.0]",
                           '[13.5, 14.5, 15.0]',
                           "['un', 'deux', 'trois']",
                           '["un", "deux", "trois"]'],
                          None,
                          None,
                          None, False, 168, "test_command_1")
    
    return test_command
    
    
  def job_test_dir_contents(self):
    if self.with_transfers:
      job = Job(["python", 
                self.tr_dir_contents_script,
                self.tr_in_dir,
                self.tr_out_dir],
                [self.tr_dir_contents_script, self.tr_in_dir],
                [self.tr_out_dir],
                None, False, 168, "dir_contents")
    elif self.with_shared_resource_path:
      job = Job(["python", 
                self.sh_dir_contents_script, 
                self.sh_in_dir,
                self.sh_out_dir],
                None,
                None,
                None, False, 168, "dir_contents")
    else:
      job = Job(["python", 
                self.lo_dir_contents_script, 
                self.lo_in_dir,
                self.lo_out_dir],
                None,
                None,
                None, False, 168, "dir_contents")
    
    return job

  def job_test_multi_file_format(self):
    if self.with_transfers:
      job = Job( ["python", 
                  self.tr_mff_script,
                  (self.tr_img_file, "example.img"),
                  (self.tr_img_out_file, "example.img")],
                [self.tr_mff_script, self.tr_img_file],
                [self.tr_img_out_file],
                None, False, 168, "multi file format test")
    elif self.with_shared_resource_path:
      job = Job( ["python", 
                  self.sh_mff_script, 
                  self.sh_img_file,
                  self.sh_img_out_file],
                None,
                None,
                None, False, 168, "multi file format test")
    else:
      job = Job( ["python", 
                  self.lo_mff_script, 
                  self.lo_img_file,
                  self.lo_img_out_file],
                None,
                None,
                None, False, 168, "multi file format test")
    
    return job
    
    
  def job_sleep(self, period):
    if self.with_transfers:
      complete_path = os.path.join(self.examples_dir, "complete")
      transfer   = FileTransfer(True,os.path.join(complete_path, "file0"), 168, "file0")
      job = Job( ["python", self.tr_sleep_script, repr(period)],
                 [self.tr_sleep_script, transfer],
                 [],
                 None, False, 168, "sleep " + repr(period) + " s")
    elif self.with_shared_resource_path:
      job = Job( ["python", self.sh_sleep_script, repr(period)],
                 None,
                 None,
                 None, False, 168, "sleep " + repr(period) + " s")
    else:
      job = Job( ["python", self.lo_sleep_script, repr(period)],
                 None,
                 None,
                 None, False, 168, "sleep " + repr(period) + " s")
    
    return job
    
  def special_transfer_test(self):
     # jobs
    test_dir_contents = self.job_test_dir_contents()
    test_multi_file_format = self.job_test_multi_file_format()
        
    # building the workflow
    jobs = [test_dir_contents, test_multi_file_format]
    dependencies = []
       
    workflow = Workflow(jobs, dependencies)
    
    return workflow
      

  def command_test(self):
    
    # jobs
    test_command_job = self.job_test_command_1()
        
    # building the workflow
    jobs = [test_command_job]
    
    dependencies = []
     
    workflow = Workflow(jobs, dependencies)
    
    return workflow
      
  def simpleExample(self):
    
    # jobs
    job1 = self.job1()
    job2 = self.job2()
    job3 = self.job3()
    job4 = self.job4()
    
    #building the workflow
    jobs = [job1, job2, job3, job4]
  
    dependencies = [(job1, job2), 
                    (job1, job3),
                    (job2, job4), 
                    (job3, job4)]
  
    group_1 = Group(name='group_1', elements=[job2, job3])
    group_2 = Group(name='group_2', elements=[job1, group_1])
    
    workflow = Workflow(jobs, dependencies, root_group=[group_2, job4])
    
    return workflow
  
  def simpleExampleWithException1(self):
                                                          
    # jobs
    if self.with_transfers and not self.with_shared_resource_path:
      job1 = Job( ["python", self.tr_exceptionJobScript], 
                  [self.tr_exceptionJobScript, self.tr_file0, self.tr_script1, self.tr_stdin1], 
                  [self.tr_file11, self.tr_file12], 
                  self.tr_stdin1, False, 168, "job1 with exception")
    elif self.with_transfers and self.with_shared_resource_path:
      job1 = Job( ["python", self.sh_exceptionJobScript], 
                  [], 
                  [self.tr_file11, self.tr_file12], 
                  self.sh_stdin1, False, 168, "job1 with exception")
    elif not self.with_transfers and self.with_shared_resource_path:
      job1 = Job( ["python", self.sh_exceptionJobScript], 
                  None, 
                  None, 
                  self.sh_stdin1, False, 168, "job1 with exception")
    else:
      job1 = Job( ["python", self.lo_exceptionJobScript], 
                  None, 
                  None,  
                  self.lo_stdin1, False, 168, "job1 with exception")                   
    
    job2 = self.job2()
    job3 = self.job3()
    job4 = self.job4()
          
    jobs = [job1, job2, job3, job4]
  

    dependencies = [(job1, job2), 
                    (job1, job3),
                    (job2, job4), 
                    (job3, job4)]
  
    group_1 = Group(name='group_1', elements=[job2, job3])
    group_2 = Group(name='group_2', elements=[job1, group_1])
   
    workflow = Workflow(jobs, dependencies, root_group=[group_2, job4])
    
    return workflow
  
  
  def simpleExampleWithException2(self):
   
    # jobs
    job1 = self.job1()
    job2 = self.job2()
    job4 = self.job4()
    
    if self.with_transfers and not self.with_shared_resource_path:
      job3 = Job( ["python", self.tr_exceptionJobScript],
                  [self.tr_exceptionJobScript, self.tr_file12, self.tr_script3, self.tr_stdin3],
                  [self.tr_file3],
                  self.tr_stdin3, False, 168, "job3 with exception")
    elif self.with_transfers and not self.with_shared_resource_path:
      job3 = Job( ["python", self.sh_exceptionJobScript],
                  [],
                  [self.tr_file3],
                  self.sh_stdin3, False, 168, "job3 with exception")
    elif self.with_transfers and not self.with_shared_resource_path:
      job3 = Job( ["python", self.sh_exceptionJobScript],
                  None,
                  None,
                  self.sh_stdin3, False, 168, "job3 with exception")
    else:
      job3 = Job( ["python", self.lo_exceptionJobScript], 
                  None, 
                  None, 
                  self.lo_stdin3, False, 168, "job3 with exception")
      
      
           
    jobs = [job1, job2, job3, job4]

    dependencies = [(job1, job2), 
                    (job1, job3),
                    (job2, job4), 
                    (job3, job4)]
  
    group_1 = Group(name='group_1', elements=[job2, job3])
    group_2 = Group(name='group_2', elements=[job1, group_1])
     
    workflow = Workflow(jobs, dependencies, root_group=[group_2, job4])
    
    return workflow
  
  
  def multipleSimpleExample(self):
    workflow1 = self.simpleExample()
    workflow2 = self.simpleExampleWithException1()
    workflow3 = self.simpleExampleWithException2()
    
    jobs = workflow1.jobs
    jobs.extend(workflow2.jobs)
    jobs.extend(workflow3.jobs)
    
    dependencies = workflow1.dependencies
    dependencies.extend(workflow2.dependencies)
    dependencies.extend(workflow3.dependencies)
    
    group1 = Group(name="simple example", elements=workflow1.root_group)
    group2 = Group(name="simple with exception in Job1",          
                   elements=workflow2.root_group)
    group3 = Group(name="simple with exception in Job3",
                   elements=workflow3.root_group)
     
    workflow = Workflow(jobs, dependencies, root_group=[group1, group2, group3])
    return workflow
  
  
  def n_jobs(self, nb=300, time=60):
      
    jobs = []
    for i in range(0,nb):
      job = self.job_sleep(time)
      jobs.append(job)
     
    dependencies = []
    
    workflow = Workflow(jobs, dependencies)
    
    return workflow
  

  def n_jobs_with_dependencies(self, nb=500, time=60):
    
    dependencies = []
    jobs = []
    intermed_job1 = self.job_sleep(2)
    jobs.append(intermed_job1)
    intermed_job2 = self.job_sleep(2)
    jobs.append(intermed_job2)

    elem_group1 = []
    for i in range(0, nb):
      job = self.job_sleep(time)
      jobs.append(job)
      elem_group1.append(job)
      dependencies.append((job,intermed_job1))
    group1 = Group(name="Group 1", elements=elem_group1)

    elem_group2 = []
    for i in range(0, nb):
      job = self.job_sleep(time)
      jobs.append(job)
      elem_group2.append(job)
      dependencies.append((intermed_job1, job))
      dependencies.append((job, intermed_job2))
    group2 = Group(name="Group 2", elements=elem_group2)

    elem_group3 = []
    for i in range(0, nb):
      job = self.job_sleep(time)
      jobs.append(job)
      elem_group3.append(job)
      dependencies.append((intermed_job2, job))
    group3 = Group(name="Group 3", elements=elem_group3)

    root_group = [group1, intermed_job1, group2, intermed_job2, group3]
    workflow = Workflow(jobs, dependencies, root_group)
  
    return workflow

  

  def serial_jobs(self, nb=5):
    
    jobs = []
    dependencies = []
    previous_job = self.job_sleep(60)
    jobs.append(previous_job)
    for i in range(0,nb):
      job = self.job_sleep(60)
      jobs.append(job)
      dependencies.append((previous_job, job))
      previous_job = job
 
    workflow = Workflow(jobs, dependencies)
  
    return workflow


  def fake_pipelineT1(self):
    jobs = []
    dependencies = []
    root_group = []
    for i in range(0, 100):
      job1 = self.job_sleep(60) 
      job1.name = "Brain extraction"
      jobs.append(job1)

      job11 = self.job_sleep(1) 
      job11.name = "test 1"
      jobs.append(job11)
      job12 = self.job_sleep(1) 
      job12.name = "test 2"
      jobs.append(job12)
      job13 = self.job_sleep(1) 
      job13.name = "test 3"
      jobs.append(job13)

      job2 = self.job_sleep(120) 
      job2.name ="Gray/white segmentation"
      jobs.append(job2)
      job3 = self.job_sleep(400) 
      job3.name = "Left hemisphere sulci recognition"
      jobs.append(job3)
      job4 = self.job_sleep(400) 
      job4.name = "Right hemisphere sulci recognition"
      jobs.append(job4)

      #dependencies.append((job1, job2))
      dependencies.append((job1, job11))
      dependencies.append((job11, job12))
      dependencies.append((job12, job13))
      dependencies.append((job13, job2))
      dependencies.append((job2, job3))
      dependencies.append((job2, job4))
      
      group_sulci = Group(name="Sulci recognition", 
                          elements=[job3, job4])
      group_subject = Group(name="sulci recognition -- subject "+ repr(i), 
                            elements=[job1, job11, job12, job13, job2, group_sulci])

      root_group.append(group_subject)
   
    workflow = Workflow(jobs, dependencies, root_group)

    return workflow



class WfTest(unittest.TestCase):
  '''
  Abstract class for soma-workflow workflow test. 
  '''
  wf = None

  wf_id = None

  wf_ctrl = None
  
  wf_ex = None

  @staticmethod
  def setup_wf_controller(workflow_controller):
    WfTest.wf_ctrl = workflow_controller

  def setUp(self):
    raise Exception('WfTest is an abstract class. SetUp must be implemented in subclass')

  def tearDown(self): 
    if self.wf_id:
      WfTest.wf_ctrl.delete_workflow(self.wf_id)
    self.assertRaises(UnknownObjectError, WfTest.wf_ctrl.workflow, self.wf_id)

    
  #def test_workflows(self):
    #res = WfTest.wf_ctr.workflows.keys()
    #self.failUnless(self.wf_id in res)
    
  #def test_stop(self):
    ##TBI
    #pass

  #def test_restart(self):
    ##TBI
    #pass

  def test_result(self):
    raise Exception('WfTest is an abstract class. test_result must be implemented in subclass')

class Simple_Shared(WfTest):

  def setUp(self):
    workflow_examples = WorkflowExamples(with_tranfers=False,
                                         with_shared_resource_path=False)
    self.wf_ex = workflow_examples
    self.wf = self.wf_ex.simpleExample() 
    self.wf_id = WfTest.wf_ctrl.submit_workflow(workflow=self.wf, 
                                              name="unit test multiple")

  def test_result(self):
    Helper.wait_workflow(self.wf_id, WfTest.wf_ctrl)
    status = WfTest.wf_ctrl.workflow_status(self.wf_id)
    
    self.failUnless(status == constants.WORKFLOW_DONE)


class Multiple(WfTest):
  
  def setUp(self):
    workflow_examples = WorkflowExamples(False)
    self.wf_ex = workflow_examples
    self.wf = self.wf_ex.simpleExample() #multipleSimpleExample()
    self.wf_id = WfTest.wf_ctrl.submit_workflow(workflow=self.wf, 
                                                name="unit test multiple")

  def test_result(self):
    Helper.wait_workflow(self.wf_id, WfTest.wf_ctrl)
    status = WfTest.wf_ctrl.workflow_status(self.wf_id)
    
    self.failUnless(status == constants.WORKFLOW_DONE)
    



if __name__ == '__main__':
  
  job_examples_dir = os.environ.get("SOMA_WORKFLOW_EXAMPLES")
  output_dir = os.environ.get("SOMA_WORKFLOW_EXAMPLES_OUT")
  if not job_examples_dir or not output_dir:
    raise RuntimeError( 'The environment variables SOMA_WORKFLOW_EXAMPLES and SOMA_WORKFLOW_EXAMPLES_OUT must be set.')
     
  sys.stdout.write("----- soma-workflow tests: WORKFLOW -------------\n")

  config_file_path = Configuration.search_config_path()
  sys.stdout.write("Configuration file: " + config_file_path)
  resource_ids = Configuration.get_configured_resources(config_file_path)
  
  # Resource
  sys.stdout.write("Configured resources:\n")
  for i in range(0, len(resource_ids)):
    sys.stdout.write("  " + repr(i) + " -> " + repr(resource_ids[i]) + "\n")
  sys.stdout.write("Select a resource number: ")
  resource_index = int(sys.stdin.readline())
  resource_id = resource_ids[resource_index]
  sys.stdout.write("Selected resource => " + repr(resource_id) + "\n")
  sys.stdout.write("---------------------------------\n")
  login = None
  password = None
  
  config = Configuration.load_from_file(resource_id, config_file_path)

  if config.get_mode() == 'remote':
    sys.stdout.write("This is a remote connection\n")
    sys.stdout.write("login:")
    login = sys.stdin.readline()
    login = login.rstrip()
    password = getpass.getpass()
  sys.stdout.write("Login => " + repr(login) + "\n")
  sys.stdout.write("---------------------------------\n")
  
  # Workflow types
  wf_types = ["multiple", "command check test", "special transfers", "ten jobs"]
  sys.stdout.write("Workflow example to test: \n")
  sys.stdout.write("all -> all \n")
  for i in range(0, len(wf_types)):
    sys.stdout.write("  " + repr(i) + " -> " + repr(wf_types[i]) + "\n")
  sys.stdout.write("Select one or several workflow(s) : \n")
  selected_wf_type_indexes = []
  line = sys.stdin.readline()
  line = line.rstrip()
  if line == "all":
    selected_wf_type = wf_types
    sys.stdout.write("Selected workflow(s): all \n")
  else:
    for strindex in line.split(" "):
      selected_wf_type_indexes.append(int(strindex))
    selected_wf_type = []
    sys.stdout.write("Selected workflow(s): \n" )
    for wf_type_index in selected_wf_type_indexes:
      selected_wf_type.append(wf_types[int(wf_type_index)])
      sys.stdout.write("  => " + repr(wf_types[int(wf_type_index)])  + "\n")

  # File path type 
  sys.stdout.write("---------------------------------\n")
  path_types = ["local path", "shared resource path", "file transfer"]
  sys.stdout.write("File path type: \n")
  sys.stdout.write("all -> all \n")
  for i in range(0, len(path_types)):
    sys.stdout.write("  " + repr(i) + " -> " + repr(path_types[i]) + "\n")
  sys.stdout.write("Select one or several path type : \n")
  selected_path_type_indexes = []
  line = sys.stdin.readline()
  line = line.rstrip()
  if line == "all":
    selected_path_type = path_types
    sys.stdout.write("Selected path type: all \n")
  else:
    for strindex in line.split(" "):
      selected_path_type_indexes.append(int(strindex))
    selected_path_type = []
    sys.stdout.write("Selected path types: \n")
    for path_type_index in selected_path_type_indexes:
      selected_path_type.append(path_types[int(path_type_index)])
      sys.stdout.write("  => " + repr(path_types[int(path_type_index)])  + "\n")

  # Test type
  sys.stdout.write("---------------------------------\n")
  test_types = ["test_result"]#, "test_workflows", "test_stop", "test_restart"]
  sys.stdout.write("Tests to perform: \n")
  sys.stdout.write("all -> all \n")
  for i in range(0, len(test_types)):
    sys.stdout.write("  " + repr(i) + " -> " + repr(test_types[i]) + "\n")
  sys.stdout.write("Select one or several test : \n")
  selected_test_type_indexes = []
  line = sys.stdin.readline()
  line = line.rstrip()
  if line == "all":
    selected_test_type = test_types
    sys.stdout.write("Selected test types: all \n")
  else:
    for strindex in line.split(" "):
      selected_test_type_indexes.append(int(strindex))
    selected_test_type = []
    sys.stdout.write("Selected test types: \n")
    for test_type_index in selected_test_type_indexes:
      selected_test_type.append(test_types[int(test_type_index)])
      sys.stdout.write("  => " + repr(test_types[int(test_type_index)])  + "\n")
  sys.stdout.write("---------------------------------\n")

  wf_controller = WorkflowController(resource_id, login, password)
  WfTest.setup_wf_controller(wf_controller)

  suite_list =  []
  tests = selected_test_type
  wf_types = ["multiple", "command check test", "special transfers", "ten jobs"]

  if "multiple" in selected_wf_type:
    suite_list.append(unittest.TestSuite(map(Multiple, tests)))
  if "command check test" in selected_wf_type:
    suite_list.append(unittest.TestSuite(map(CommandCheck, tests)))
  if "special transfers" in selected_wf_type:
    suite_list.append(unittest.TestSuite(map(SpecialTransfers, tests)))
  if "ten jobs" in selected_wf_type:
    suite_list.append(unittest.TestSuite(map(TenJobs, tests)))

  alltests = unittest.TestSuite(suite_list)
  unittest.TextTestRunner(verbosity=2).run(alltests)
  
  sys.exit(0)


