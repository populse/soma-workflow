import soma.workflow.workflow as workflow
#from soma.workflow.workflow import *
from soma.jobs.constants import *
import soma.jobs.jobClient
import socket
import os
import pickle
import subprocess
import time
import threading

def printWorkflow(workflow, dot_file_path, graph_file_path):
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
  
  
def printSubmittedWorkflow(jobs, workflow, cmpt):#, dot_file_path, graph_file_path):
  
  dot_file_path = "/home/sl225510/myWorkflow" + repr(cmpt) + ".dot"
  if cmpt<10 :
    graph_file_path = "/home/sl225510/graph0" + repr(cmpt) +".png"
  else:
    graph_file_path = "/home/sl225510/graph" + repr(cmpt) +".png"
    
  if dot_file_path and os.path.isfile(dot_file_path):
    os.remove(dot_file_path)
  
  file = open(dot_file_path, "w")
  print >> file, "digraph G {"
  for ar in workflow.dependencies:
    print >> file, ar[0].name + " -> " + ar[1].name
  for node in workflow.nodes:
    if isinstance(node, JobTemplate):
      status = jobs.status(node.job_id)
      if status == NOT_SUBMITTED:
        print >> file, node.name + "[shape=box];"
      else:
        if status == DONE or status == FAILED:
          print >> file, node.name + "[shape=box label="+ node.name + "_done, style=filled, color=gray];"#\"0.9,0.9,0.9\"];"
        else:
          print >> file, node.name + "[shape=box label="+ node.name + "_"+status+", style=filled, color=red];"#\"0.9,0.7,0.7\"];"
    if isinstance(node, FileTransfer):
      status = jobs.transferStatus(node.local_file_path)
      #if status == TRANSFER_NOT_READY:
      #  print >> file, node.name + "[shape=box];"
      if status == READY_TO_TRANSFER:
        print >> file, node.name + "[label="+ node.name + "_ready, style=filled, color=blue];"#\"0.7,0.9,0.7\"];"
      if status == TRANSFERING:
        print >> file, node.name + "[label="+ node.name + ", style=filled, color=red];"#\"0.9,0.7,0.7\"];"
      if status == TRANSFERED:
        print >> file, node.name + "[label="+ node.name + "_done, style=filled, color=gray];"#\"0.9,0.9,0.9\"];"
      
  print >> file, "}"
  file.close()
  
  command = "dot -Tpng " + dot_file_path + " -o " + graph_file_path
  dot_process = subprocess.Popen(command, shell = True)
  print command


def viewUpdateLoop(jobs, submitted_workflow):
  #for i in range(1, 30):
  i = 0
  while True:
    printSubmittedWorkflow(jobs, submitted_workflow, i)
    i = i +1
    time.sleep(2)

if __name__ == '__main__':
  
  examples_dir = "/home/sl225510/svn/brainvisa/soma/soma-pipeline/trunk/test/jobExamples/"
  ouput_dir = "/home/sl225510/output/"
  python = "/i2bm/research/Mandriva-2008.0-i686/bin/python"
  
  #############################
  # OFFLINE WORKFLOW BUILDING #
  #############################
  
  
  
  # outputs
  file11 = FileRetrieving(ouput_dir + "file11", 168, "file11")
  file12 = FileRetrieving(ouput_dir + "file12", 168, "file12")
  file2 = FileRetrieving(ouput_dir + "file2", 168, "file2")
  file3 = FileRetrieving(ouput_dir + "file3", 168, "file3")
  file4 = FileRetrieving(ouput_dir + "file4", 168, "file4")
  
  # inputs
  file0 = FileSending(examples_dir + "complete/" + "file0", 168, "file0")
  script1 = FileSending(examples_dir + "complete/" + "job1.py", 168, "job1_py")
  stdin1 = FileSending(examples_dir + "complete/" + "stdin1", 168, "stdin1")
  script2 = FileSending(examples_dir + "complete/" + "job2.py", 168, "job2_py")
  stdin2 = FileSending(examples_dir + "complete/" + "stdin2", 168, "stdin2")
  script3 = FileSending(examples_dir + "complete/" + "job3.py", 168, "job3_py")
  stdin3 = FileSending(examples_dir + "complete/" + "stdin3", 168, "stdin3")
  script4 = FileSending(examples_dir + "complete/" + "job4.py", 168, "job4_py")
  stdin4 = FileSending(examples_dir + "complete/" + "stdin4", 168, "stdin4")
  
                                                                                                         
  # jobs
  job1 = JobTemplate([python, script1, file0,  file11, file12, "5"], 
                    [file0, script1, stdin1], 
                    [file11, file12], 
                    stdin1, False, 168, "job1")
                             
  job2 = JobTemplate([python, script2, file11,  file0, file2, "20"], 
                    [file0, file11, script2, stdin2], 
                    [file2], 
                    stdin2, False, 168, "job2")
                            
  job3 = JobTemplate([python, script3, file12,  file3, "20"], 
                    [file12, script3, stdin3], 
                    [file3], 
                    stdin3, False, 168, "job3")
  
  job4 = JobTemplate([python, script4, file2,  file3, file4, "20"], 
                             [file2, file3, script4, stdin4], 
                             [file4], 
                             stdin4, False, 168, "job4")
  
  #building the workflow
  myWorkflow = Workflow()
  myWorkflow.nodes.extend([file11, file12, file2, file3, file4,
                          file0, script1, stdin1, 
                          script2, stdin2, 
                          script3, stdin3, 
                          script4, stdin4, 
                          job1, job2, job3, job4])
                          
  myWorkflow.dependencies.extend([(script1, job1),
                                 (stdin1, job1),
                                 (file0, job1),
                                 (file0, job2),
                                 (job1, file11),
                                 (job1, file12),
                                 (file11, job2),
                                 (file12, job3),
                                 (script2, job2),
                                 (stdin2, job2),
                                 (job2, file2),
                                 (script3, job3),
                                 (stdin3, job3),
                                 (job3, file3),
                                 (script4, job4),
                                 (stdin4, job4),
                                 (file2, job4),
                                 (file3, job4),
                                 (job4, file4)]
                                 )
                                 
  printWorkflow(myWorkflow, "/home/sl225510/myWorkflow.dot", "/home/sl225510/graph.png")
   
  jobs = soma.jobs.jobClient.Jobs(os.environ["SOMA_JOBS_CONFIG"], 'neurospin_test_cluster')
 
  submitted_workflow = jobs.submitWorkflow(myWorkflow)
  print submitted_workflow
 
 
  printWorkflow(submitted_workflow, "/home/sl225510/myWorkflow1.dot", "/home/sl225510/graph01.png")
   
  printSubmittedWorkflow(jobs, submitted_workflow, 2)
  
  view_thread = threading.Thread(name = "View", 
                                 target = viewUpdateLoop, 
                                 args = (jobs, submitted_workflow))
  view_thread.setDaemon(True)
  view_thread.start()
  
  cmpt = 3
  for node in submitted_workflow.nodes:
    if isinstance(node, FileSending):
      if jobs.transferStatus(node.local_file_path) == READY_TO_TRANSFER:
        jobs.sendRegisteredFile(node.local_file_path)
        #printSubmittedWorkflow(jobs, submitted_workflow, cmpt)
        #cmpt = cmpt+1
        
        
        
        
  #for i in range(cmpt, cmpt + 25):
    #printSubmittedWorkflow(jobs, submitted_workflow, i)
    #time.sleep(2)
  
  
  
 
 