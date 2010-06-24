import soma.workflow.workflow as workflow
import soma.jobs.jobClient
import socket
import os

if __name__ == '__main__':
  
  jobs = soma.jobs.jobClient.Jobs(os.environ["SOMA_JOBS_CONFIG"], 'neurospin_test_cluster')
  examples_dir = "/home/sl225510/svn/brainvisa/soma/soma-pipeline/trunk/test/jobExamples/"
  ouput_dir = "/home/sl225510/output/"
  python = "/i2bm/research/Mandriva-2008.0-i686/bin/python"
  
  # outputs
  file11 = workflow.FileRetrieving(jobs, ouput_dir + "file11", 168, "file11")
  file12 = workflow.FileRetrieving(jobs, ouput_dir + "file12", 168, "file12")
  file2 = workflow.FileRetrieving(jobs, ouput_dir + "file2", 168, "file2")
  file3 = workflow.FileRetrieving(jobs, ouput_dir + "file3", 168, "file3")
  file4 = workflow.FileRetrieving(jobs, ouput_dir + "file4", 168, "file4")
  
  # inputs
  file0 = workflow.FileSending(jobs, examples_dir + "complete/" + "file0", 168, "file0")
  script1 = workflow.FileSending(jobs, examples_dir + "complete/" + "job1.py", 168, "job1.py")
  stdin1 = workflow.FileSending(jobs, examples_dir + "complete/" + "stdin1", 168, "stdin1")
  script2 = workflow.FileSending(jobs, examples_dir + "complete/" + "job2.py", 168, "job2.py")
  stdin2 = workflow.FileSending(jobs, examples_dir + "complete/" + "stdin2", 168, "stdin2")
  script3 = workflow.FileSending(jobs, examples_dir + "complete/" + "job3.py", 168, "job3.py")
  stdin3 = workflow.FileSending(jobs, examples_dir + "complete/" + "stdin3", 168, "stdin3")
  script4 = workflow.FileSending(jobs, examples_dir + "complete/" + "job4.py", 168, "job4.py")
  stdin4 = workflow.FileSending(jobs, examples_dir + "complete/" + "stdin4", 168, "stdin4")
  
                                                                                                         
  # jobs
  job1 = workflow.JobRunning(jobs, 
                             [python, script1, file0,  file11, file12, "5"], 
                             [file0, script1, stdin1], 
                             [file11, file12], 
                             stdin1, False, 168, "job1")
                             
  job2 = workflow.JobRunning(jobs, 
                            [python, script2, file11,  file0, file2, "20"], 
                            [file0, file11, script2, stdin2], 
                            [file2], 
                            stdin2, False, 168, "job2")
                            
  job3 = workflow.JobRunning(jobs, 
                            [python, script3, file12,  file3, "20"], 
                            [file12, script3, stdin3], 
                            [file3], 
                            stdin3, False, 168, "job3")
  
  job4 = workflow.JobRunning(jobs, 
                            [python, script4, file2,  file3, file4, "20"], 
                            [file2, file3, script4, stdin4], 
                            [file4], 
                            stdin4, False, 168, "job4")
  
  #building the workflow
                             
  myWorkflow = workflow.SerialWorkflow([
                                       workflow.ParallelWorkflow([file0, script1, stdin1, script2, stdin2, script3, stdin3, script4, stdin4]),
                                       job1,
                                       workflow.ParallelWorkflow([job2, job3]),
                                       job4,
                                       workflow.ParallelWorkflow([file11, file12, file3, file4])
                                       ])
  
                             
  workflow.workflowToDot(myWorkflow, "/home/sl225510/myWorkflow.dot")
  
  myWorkflow.run()