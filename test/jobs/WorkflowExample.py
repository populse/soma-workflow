
from soma.jobs.constants import *
from soma.jobs.jobClient import *
import socket
import os
import pickle
import subprocess
import time
import threading
import ConfigParser
import sys
import getpass


class TestWorkflow(object):
  def __init__(self, test_no=None):
    self.jobs = None
    if not test_no: 
      self.test_no = 1
    else:
      self.test_no = test_no
      
    self.test_config = ConfigParser.ConfigParser()
    self.test_config.read('TestJobs.cfg')
    
    self.hostname = socket.gethostname()
    self.examples_dir = self.test_config.get(self.hostname, 'job_examples_dir')
    self.ouput_dir = self.test_config.get(self.hostname, 'job_output_dir') + repr(self.test_no) + "/"
    python = self.test_config.get(self.hostname, 'python')
    
    #############################
    # OFFLINE WORKFLOW BUILDING #
    #############################
    
    # outputs
    file11 = FileRetrieving(self.ouput_dir + "file11", 168, "file11")
    file12 = FileRetrieving(self.ouput_dir + "file12", 168, "file12")
    file2 = FileRetrieving(self.ouput_dir + "file2", 168, "file2")
    file3 = FileRetrieving(self.ouput_dir + "file3", 168, "file3")
    file4 = FileRetrieving(self.ouput_dir + "file4", 168, "file4")
    
    # inputs
    file0 = FileSending(self.examples_dir + "complete/" + "file0", 168, "file0")
    script1 = FileSending(self.examples_dir + "complete/" + "job1.py", 168, "job1_py")
    stdin1 = FileSending(self.examples_dir + "complete/" + "stdin1", 168, "stdin1")
    script2 = FileSending(self.examples_dir + "complete/" + "job2.py", 168, "job2_py")
    stdin2 = FileSending(self.examples_dir + "complete/" + "stdin2", 168, "stdin2")
    script3 = FileSending(self.examples_dir + "complete/" + "job3.py", 168, "job3_py")
    stdin3 = FileSending(self.examples_dir + "complete/" + "stdin3", 168, "stdin3")
    script4 = FileSending(self.examples_dir + "complete/" + "job4.py", 168, "job4_py")
    stdin4 = FileSending(self.examples_dir + "complete/" + "stdin4", 168, "stdin4")
    
    exceptionJobScript = FileSending(self.examples_dir + "simple/exceptionJob.py", 168, "exception_job")
                                                                                                          
    # jobs
    job1 = JobTemplate([python, script1, file0,  file11, file12, "20"], 
                      [file0, script1, stdin1], 
                      [file11, file12], 
                      stdin1, False, 168, "job1")
                              
    job2 = JobTemplate([python, script2, file11,  file0, file2, "30"], 
                      [file0, file11, script2, stdin2], 
                      [file2], 
                      stdin2, False, 168, "job2")
                              
    job3 = JobTemplate([python, script3, file12,  file3, "30"], 
                      [file12, script3, stdin3], 
                      [file3], 
                      stdin3, False, 168, "job3")
    
    #job3 = JobTemplate([python, exceptionJobScript],
                      #[exceptionJobScript, file12, script3, stdin3],
                      #[file3],
                      #None, False, 168, "job3")
    
    job4 = JobTemplate([python, script4, file2,  file3, file4, "10"], 
                              [file2, file3, script4, stdin4], 
                              [file4], 
                              stdin4, False, 168, "job4")
  
    
  
    #building the workflow
    
    
    #nodes = [file11, file12, file2, file3, file4,
           #file0, script1, stdin1, 
           #script2, stdin2, 
           #script3, stdin3, #exceptionJobScript,
           #script4, stdin4, 
           #job1, job2, job3, job4]
           
    nodes = [job1, job2, job3, job4]
  
    #dependencies = [(script1, job1),
                    #(stdin1, job1),
                    #(file0, job1),
                    #(file0, job2),
                    #(job1, file11),
                    #(job1, file12),
                    #(file11, job2),
                    #(file12, job3),
                    #(script2, job2),
                    #(stdin2, job2),
                    #(job2, file2),
                    #(script3, job3),
                    #(stdin3, job3),
                    #(job3, file3),
                    #(script4, job4),
                    #(stdin4, job4),
                    #(file2, job4),
                    #(file3, job4),
                    #(job4, file4)]#,
                    ##(exceptionJobScript, job3)]
   
    dependencies = [(job1, job2), 
                    (job1, job3),
                    (job2, job4), 
                    (job3, job4)]
  
   
                    
    group_1 = Group([job2, job3], 'group_1')
    group_2 = Group([job1, group_1], 'group_2')
    mainGroup = Group([group_2, job4])
    
    self.workflow = Workflow(nodes, dependencies, mainGroup, [group_1, group_2])
  

  def workflowSubmission(self):
    if self.test_config.get(self.hostname, 'mode') == 'remote':
      print "Ressource => " + test_config.get(self.hostname, 'ressource_id')
      print "login: ",
      login = raw_input()
      password = getpass.getpass()
    else:
      login = None
      password = None
    
    self.jobs = Jobs(os.environ["SOMA_JOBS_CONFIG"],
                self.test_config.get(self.hostname, 'ressource_id'), 
                login, 
                password,
                log=self.test_no)
    
    self.submitted_workflow = self.jobs.submitWorkflow(self.workflow)
    
    self.printWorkflow(self.submitted_workflow, self.ouput_dir + "/myWorkflow1.dot", self.ouput_dir + "/graph01.png")
      
      
  def transferInputFiles(self):
    for node in self.submitted_workflow.full_nodes:
      if isinstance(node, FileSending):
        if self.jobs.transferStatus(node.local_file_path) == READY_TO_TRANSFER:
          self.jobs.sendRegisteredFile(node.local_file_path)
          
          
  def transferOutputFiles(self): 
    for node in self.submitted_workflow.full_nodes:
      if isinstance(node, FileRetrieving):
        print "Transfer " + node.name + " status: " + self.jobs.transferStatus(node.local_file_path)
        if self.jobs.transferStatus(node.local_file_path) == READY_TO_TRANSFER:
          self.jobs.retrieveFile(node.local_file_path)
       

  def printWorkflow(self, workflow, dot_file_path, graph_file_path):
    if dot_file_path and os.path.isfile(dot_file_path):
      os.remove(dot_file_path)
    file = open(dot_file_path, "w")
    print >> file, "digraph G {"
    for ar in workflow.dependencies:
      print >> file, ar[0].name + " -> " + ar[1].name
    for node in workflow.nodes:
      if isinstance(node, FileTransfer):
        print >> file, node.name + "[shape=box];"
    print >> file, "}"
    file.close()

    command = "dot -Tpng " + dot_file_path + " -o " + graph_file_path
    dot_process = subprocess.Popen(command, shell = True)
    print command
    



if __name__ == '__main__':
  
  if len(sys.argv) == 0:
    test_no = 1
  else:
    test_no = sys.argv[1]
  
 
      
  
  
  
 
 