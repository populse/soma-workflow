# -*- coding: utf-8 -*-

from soma.workflow.constants import *
from soma.workflow.client import Job, SharedResourcePath, FileTransfer, FileSending, FileRetrieving, WorkflowNodeGroup, Workflow, WorkflowController
import soma.workflow.engine
import socket
import os
import ConfigParser
import pickle
import subprocess
import commands

class JobsControler(object):
  
  def __init__(self):
    pass
   
  @staticmethod
  def getConfigFile():
    conf_file = os.environ.get( 'SOMA_WORKFLOW_CONFIG' )
    if not conf_file or not os.path.exists( conf_file ):
      conf_file = os.path.join( os.environ.get( 'HOME', '' ), '.brainvisa', 'soma_workflow.cfg' )
      if not os.path.exists( conf_file ):
        raise RuntimeError( 'Cannot find soma-workflow configuration file. Perhaps SOMA_WORKFLOW_CONFIG is not proprely set.' )
    print "Configuration file: " + repr(conf_file)
    return conf_file
  
  
  def get_configured_queues(self, resource_id):
    queues = [" "] # default queue
    somajobs_config = ConfigParser.ConfigParser()
    somajobs_config.read( self.getConfigFile() )
    if somajobs_config.has_option(resource_id, OCFG_QUEUES):      
      queues.extend(somajobs_config.get(resource_id, OCFG_QUEUES).split())
    return queues

  def getRessourceIds(self):
    resource_ids = []
    somajobs_config = ConfigParser.ConfigParser()
    somajobs_config.read( self.getConfigFile() )
    for cid in somajobs_config.sections():
      resource_ids.append(cid)
        
    return resource_ids
        
  def getConnection(self, resource_id, login, password, test_no):
    try: 
      connection = WorkflowController( self.getConfigFile(),
                                       resource_id, 
                                       login, 
                                       password,
                                       log=test_no)
    except Exception, e:
      return (None, "%s: %s" %(type(e),e) )
      
    return (connection, "") 
    
  def isRemoteConnection(self, resource_id):
    somajobs_config = ConfigParser.ConfigParser()
    somajobs_config.read( self.getConfigFile() )
    submitting_machines = somajobs_config.get(resource_id, CFG_SUBMITTING_MACHINES).split()
    hostname = socket.gethostname()
    is_remote = True
    for machine in submitting_machines: 
      if hostname == machine: 
        is_remote = False
        break
    return is_remote
    
    
  def readWorkflowFromFile(self, file_path):
    file = open(file_path, "r")
    workflow = pickle.load(file)
    file.close()

    return workflow
  
  def saveWorkflowToFile(self, file_path, workflow):
    file = open(file_path, "w")
    pickle.dump(workflow, file)
    file.close()
  
  def getSubmittedWorkflows(self, connection):
    return connection.workflows()
    
  def getWorkflow(self, wf_id, connection):
    workflow = connection.workflow(wf_id)
    expiration_date = connection.workflows([wf_id])[wf_id][1]
    return (workflow, expiration_date)
  
  
  def getWorkflowExampleList(self):
    return ["simple", "multiple", "with exception 1", "with exception 2", "command check test", "directory transfer", "hundred of jobs", "ten jobs", "fake pipelineT1"]
    
  def generateWorkflowExample(self, 
                              with_file_transfer, 
                              with_shared_resource_path, 
                              example_type, 
                              workflow_file_path):
    
    job_examples_dir = os.environ.get("SOMA_WORKFLOW_EXAMPLES")
    output_example_dir = os.environ.get("SOMA_WORKFLOW_EXAMPLES_OUT")
    if not job_examples_dir or not output_example_dir:
       raise RuntimeError( 'The environment variables SOMA_WORKFLOW_EXAMPLES and SOMA_WORKFLOW_EXAMPLES_OUT must be set.')

    wfExamples = WorkflowExamples(with_tranfers=with_file_transfer, 
                                  examples_dir=job_examples_dir, 
                                  output_dir=output_example_dir, 
                                  with_shared_resource_path=with_shared_resource_path)
    workflow = None
    if example_type == 0:
      workflow = wfExamples.simpleExample()
    elif example_type == 1:
      workflow = wfExamples.multipleSimpleExample()
    elif example_type == 2:
      workflow = wfExamples.simpleExampleWithException1()
    elif example_type == 3:
      workflow = wfExamples.simpleExampleWithException2()
    elif example_type == 4:
      workflow = wfExamples.command_test()
    elif example_type == 5:
      workflow = wfExamples.special_transfer_test()
    elif example_type == 6:
      workflow = wfExamples.hundred_of_jobs()
    elif example_type == 7:
      workflow = wfExamples.ten_jobs()
    elif example_type == 8:
      workflow = wfExamples.fake_pipelineT1()
      
      
    if workflow:
      file = open(workflow_file_path, 'w')
      pickle.dump(workflow, file)
      file.close()
    
    
  def submit_workflow(self, 
                      workflow, 
                      name, 
                      expiration_date,
                      queue,
                      connection):
    
    wf_id = connection.submit_workflow( workflow=workflow,
                                        expiration_date=expiration_date,
                                        name=name,
                                        queue=queue) 
    wf = connection.workflow(wf_id)

    return wf
  
                                    
  def restart_workflow(self, workflow, connection):
    return connection.restart_workflow(workflow.wf_id)
                                    
  def delete_workflow(self, wf_id, connection):
    return connection.delete_workflow(wf_id)
  
  def change_workflow_expiration_date(self, wf_id, date, connection):
    return connection.change_workflow_expiration_date(wf_id, date)
    
  def transferInputFiles(self, workflow, connection, buffer_size = 512**2):
   
    to_transfer = []
    for ft in workflow.transfers.itervalues():
      status, info = connection.transfer_status(ft.local_path)
      if status == READY_TO_TRANSFER:
        to_transfer.append((0, ft.local_path))
      if status == TRANSFERING:
        to_transfer.append((info[1], ft.local_path))
          
    to_transfer = sorted(to_transfer, key = lambda element: element[1])
    for transmitted, local_path in to_transfer:
      print "send to " + local_path + " already transmitted size =" + repr(transmitted)
      connection.send(local_path, buffer_size)
    
  def transferOutputFiles(self, workflow, connection, buffer_size = 512**2):
    to_transfer = []
    for ft in workflow.transfers.itervalues():
      status, info = connection.transfer_status(ft.local_path)
      if status == READY_TO_TRANSFER:
        to_transfer.append((0, ft.local_path))
      if status == TRANSFERING:
        to_transfer .append((info[1], ft.local_path))

    to_transfer = sorted(to_transfer, key = lambda element: element[1])
    for transmitted, local_path in to_transfer:
      print "retrieve " + local_path + " already transmitted size" + repr(transmitted)
      connection.retrieve(local_path, buffer_size)

          
  def printWorkflow(self, workflow, connection):
    
    output_dir = "/tmp/"
    
    GRAY="\"#C8C8B4\""
    BLUE="\"#00C8FF\""
    RED="\"#FF6432\""
    GREEN="\"#9BFF32\""
    LIGHT_BLUE="\"#C8FFFF\""
    
    names = dict()
    current_id = 0
    
    dot_file_path = output_dir + "tmp.dot"
    graph_file_path = output_dir + "tmp.png"
    if dot_file_path and os.path.isfile(dot_file_path):
      os.remove(dot_file_path)
    file = open(dot_file_path, "w")
    print >> file, "digraph G {"
    for node in workflow.nodes:
      current_id = current_id + 1
      names[node] = ("node" + repr(current_id), "\""+node.name+"\"")
    for ar in workflow.dependencies:
      print >> file, names[ar[0]][0] + " -> " + names[ar[1]][0] 
    for node in workflow.nodes:
      if isinstance(node, Job):
        if node.job_id == -1:
          print >> file, names[node][0] + "[shape=box label="+ names[node][1] +"];"
        else:
          status = connection.job_status(node.job_id)
          if status == NOT_SUBMITTED:
            print >> file, names[node][0] + "[shape=box label="+ names[node][1] +", style=filled, color=" + GRAY +"];"
          elif status == DONE:
            exit_status, exit_value, term_signal, resource_usage = connection.job_termination_status(node.job_id)
            if exit_status == FINISHED_REGULARLY and exit_value == 0:
              print >> file, names[node][0] + "[shape=box label="+ names[node][1] +", style=filled, color=" + LIGHT_BLUE +"];"
            else: 
              print >> file, names[node][0] + "[shape=box label="+ names[node][1] +", style=filled, color=" + RED +"];"
          elif status == FAILED:
            print >> file, names[node][0] + "[shape=box label="+ names[node][1] +", style=filled, color=" + RED +"];"
          else:
            print >> file, names[node][0] + "[shape=box label="+ names[node][1] +", style=filled, color=" + GREEN +"];"
      if isinstance(node, FileTransfer):
        if not node.local_path:
          print >> file, names[node][0] + "[label="+ names[node][1] +"];"
        else:
          status = connection.transfer_status(node.local_path)[0]
          if status == TRANSFER_NOT_READY:
            print >> file, names[node][0] + "[label="+ names[node][1] +", style=filled, color=" + GRAY +"];"
          elif status == READY_TO_TRANSFER:
            print >> file, names[node][0] + "[label="+ names[node][1] +", style=filled, color=" + BLUE +"];"
          elif status == TRANSFERING:
            print >> file, names[node][0] + "[label="+ names[node][1] +", style=filled, color=" + GREEN +"];"
          elif status == TRANSFERED:
            print >> file, names[node][0] + "[label="+ names[node][1] +", style=filled, color=" + LIGHT_BLUE +"];"
          
    print >> file, "}"
    file.close()
    
    command = "dot -Tpng " + dot_file_path + " -o " + graph_file_path
    #print command
    #dot_process = subprocess.Popen(command, shell = True)
    commands.getstatusoutput(command)
    return graph_file_path
    
  
  
    
class WorkflowExamples(object):
  
  
  
  def __init__(self, with_tranfers, examples_dir, output_dir, with_shared_resource_path = False):
    '''
    @type with_tranfers: boolean
    '''
    self.examples_dir = examples_dir
    
    self.output_dir = output_dir
    if not os.path.isdir(output_dir):
      os.mkdir(output_dir)
       
    self.with_transfers = with_tranfers
    self.with_shared_resource_path = with_shared_resource_path
    
    # local path
    
    
    complete_path = os.path.join(self.examples_dir, "complete")
    self.lo_in_dir = self.examples_dir
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
    
    
    self.lo_file11 = os.path.join(self.output_dir, "file11")
    self.lo_file12 = os.path.join(self.output_dir, "file12")
    self.lo_file2 = os.path.join(self.output_dir, "file2")
    self.lo_file3 = os.path.join(self.output_dir, "file3")
    self.lo_file4 = os.path.join(self.output_dir, "file4")
    
    # Shared resource path
    self.sh_in_dir = SharedResourcePath("", "example", "job_dir", 168)
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
   
    
    self.sh_file11 = SharedResourcePath("file11", "example", "output_dir", 168)
    self.sh_file12 = SharedResourcePath("file12", "example", "output_dir",168)
    self.sh_file2 = SharedResourcePath("file2", "example", "output_dir",168)
    self.sh_file3 = SharedResourcePath("file3", "example", "output_dir",168)
    self.sh_file4 = SharedResourcePath("file4", "example", "output_dir",168)
    
    # Transfers
    
    complete_path = os.path.join(self.examples_dir, "complete")
    self.tr_in_dir = FileSending(self.examples_dir, 168, "in_dir")
    self.tr_file0   = FileSending(os.path.join(complete_path, "file0"), 168, "file0")
    self.tr_script1 = FileSending(os.path.join(complete_path, "job1.py"), 168, "job1_py")
    self.tr_stdin1  = FileSending(os.path.join(complete_path, "stdin1"), 168, "stdin1")
    self.tr_script2 = FileSending(os.path.join(complete_path, "job2.py"), 168, "job2_py")
    self.tr_stdin2  = FileSending(os.path.join(complete_path, "stdin2"), 168, "stdin2")
    self.tr_script3 = FileSending(os.path.join(complete_path, "job3.py"), 168, "job3_py")
    self.tr_stdin3  = FileSending(os.path.join(complete_path, "stdin3"), 168, "stdin3")
    self.tr_script4 = FileSending(os.path.join(complete_path, "job4.py"), 168, "job4_py")
    self.tr_stdin4  = FileSending(os.path.join(complete_path, "stdin4"), 168, "stdin4")
    self.tr_exceptionJobScript = FileSending(os.path.join(self.examples_dir, 
                                                          "simple/exceptionJob.py"), 
                                                          168, "exception_job")
    self.tr_cmd_check_script = FileSending(os.path.join(self.examples_dir, 
                                                        "command/argument_check.py"),
                                                        168, "cmd_check")
    self.tr_sleep_script = FileSending(os.path.join(self.examples_dir, 
                                                    "simple/sleep_job.py"), 
                                                     168, "sleep_job")
    self.tr_dir_contents_script = FileSending(os.path.join(self.examples_dir, 
                                                        "special_transfers/dir_contents.py"),
                                                        168, "dir_contents")
      
    self.tr_file11 = FileRetrieving(os.path.join(self.output_dir, "file11"), 168, "file11")
    self.tr_file12 = FileRetrieving(os.path.join(self.output_dir, "file12"), 168, "file12")
    self.tr_file2 = FileRetrieving(os.path.join(self.output_dir, "file2"), 168, "file2")
    self.tr_file3 = FileRetrieving(os.path.join(self.output_dir, "file3"), 168, "file3")
    self.tr_file4 = FileRetrieving(os.path.join(self.output_dir, "file4"), 168, "file4")
    
      
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
      test_command = Job( ["python", 
                           self.tr_dir_contents_script,
                           self.tr_in_dir],
                          [self.tr_dir_contents_script, self.tr_in_dir],
                          [],
                          None, False, 168, "dir_contents")
    elif self.with_shared_resource_path:
      test_command = Job( ["python", self.sh_dir_contents_script, self.sh_script1],
                          None,
                          None,
                          None, False, 168, "dir_contents")
    else:
      test_command = Job( ["python", self.lo_dir_contents_script, self.lo_in_dir],
                          None,
                          None,
                          None, False, 168, "dir_contents")
    
    return test_command
    
    
  def job_sleep(self, period):
    if self.with_transfers:
      job = Job( ["python", self.tr_sleep_script, repr(period)],
                 [self.tr_sleep_script],
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
        
    # building the workflow
    nodes = [test_dir_contents]
    
    dependencies = []
    
    mainGroup = WorkflowNodeGroup([test_dir_contents])
    
    workflow = Workflow(nodes, dependencies, mainGroup, [])
    
    return workflow
      

  def command_test(self):
    
    # jobs
    test_command_job = self.job_test_command_1()
        
    # building the workflow
    nodes = [test_command_job]
    
    dependencies = []
    
    mainGroup = WorkflowNodeGroup([test_command_job])
    
    workflow = Workflow(nodes, dependencies, mainGroup, [])
    
    return workflow
      
  def simpleExample(self):
    
    # jobs
    job1 = self.job1()
    job2 = self.job2()
    job3 = self.job3()
    job4 = self.job4()
    
    #building the workflow
    nodes = [job1, job2, job3, job4]
  
    dependencies = [(job1, job2), 
                    (job1, job3),
                    (job2, job4), 
                    (job3, job4)]
  
    group_1 = WorkflowNodeGroup([job2, job3], 'group_1')
    group_2 = WorkflowNodeGroup([job1, group_1], 'group_2')
    mainGroup = WorkflowNodeGroup([group_2, job4])
    
    workflow = Workflow(nodes, dependencies, mainGroup, [group_1, group_2])
    
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
          
    nodes = [job1, job2, job3, job4]
  

    dependencies = [(job1, job2), 
                    (job1, job3),
                    (job2, job4), 
                    (job3, job4)]
  
    group_1 = WorkflowNodeGroup([job2, job3], 'group_1')
    group_2 = WorkflowNodeGroup([job1, group_1], 'group_2')
    mainGroup = WorkflowNodeGroup([group_2, job4])
    
    workflow = Workflow(nodes, dependencies, mainGroup, [group_1, group_2])
    
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
      
      
           
    nodes = [job1, job2, job3, job4]

    dependencies = [(job1, job2), 
                    (job1, job3),
                    (job2, job4), 
                    (job3, job4)]
  
    group_1 = WorkflowNodeGroup([job2, job3], 'group_1')
    group_2 = WorkflowNodeGroup([job1, group_1], 'group_2')
    mainGroup = WorkflowNodeGroup([group_2, job4])
    
    workflow = Workflow(nodes, dependencies, mainGroup, [group_1, group_2])
    
    return workflow
  
  
  def multipleSimpleExample(self):
    workflow1 = self.simpleExample()
    workflow2 = self.simpleExampleWithException1()
    workflow3 = self.simpleExampleWithException2()
    
    nodes = workflow1.nodes
    nodes.extend(workflow2.nodes)
    nodes.extend(workflow3.nodes)
    
    dependencies = workflow1.dependencies
    dependencies.extend(workflow2.dependencies)
    dependencies.extend(workflow3.dependencies)
    
    group1 = WorkflowNodeGroup(workflow1.mainGroup.elements, "simple example")
    group2 = WorkflowNodeGroup(workflow2.mainGroup.elements, "simple with exception in Job1")
    group3 = WorkflowNodeGroup(workflow3.mainGroup.elements, "simple with exception in Job3")
    mainGroup = WorkflowNodeGroup([group1, group2, group3])
     
    groups = [group1, group2, group3]
    groups.extend(workflow1.groups)
    groups.extend(workflow2.groups)
    groups.extend(workflow3.groups)
    
    workflow = Workflow(nodes, dependencies, mainGroup, groups)
    return workflow
  
  
  def ten_jobs(self):
      
    nodes = []
    for i in range(0,10):
      job = self.job_sleep(60)
      nodes.append(job)
     
    dependencies = []
    
    mainGroup = WorkflowNodeGroup(nodes)
    
    workflow = Workflow(nodes, dependencies, mainGroup, [])
    
    return workflow
  
  def hundred_of_jobs(self):
    
    nodes = []
    for i in range(0,100):
      job = self.job_sleep(60)
      nodes.append(job)
     
    dependencies = []
    
    mainGroup = WorkflowNodeGroup(nodes)
    
    workflow = Workflow(nodes, dependencies, mainGroup, [])
  
    return workflow


  def fake_pipelineT1(self):
    
    nodes = []
    dependencies = []
    groups = []
    mainGroupList = []
    for i in range(0, 100):
      job1 = self.job_sleep(60) 
      job1.name = "Brain extraction"
      nodes.append(job1)
      job2 = self.job_sleep(120) 
      job2.name ="Gray/white segmentation"
      nodes.append(job2)
      job3 = self.job_sleep(400) 
      job3.name = "Left hemisphere sulci recognition"
      nodes.append(job3)
      job4 = self.job_sleep(400) 
      job4.name = "Right hemisphere sulci recognition"
      nodes.append(job4)

      dependencies.append((job1, job2))
      dependencies.append((job2, job3))
      dependencies.append((job2, job4))
      
      groupSulci = WorkflowNodeGroup([job3, job4], "Sulci recognition")
      groupPt1 = WorkflowNodeGroup([job1, job2, groupSulci], "sulci recognition -- subject " + repr(i))

      groups.append(groupSulci)
      groups.append(groupPt1)
      mainGroupList.append(groupPt1) 
      
    mainGroup = WorkflowNodeGroup(mainGroupList)
   
    workflow = Workflow(nodes, dependencies, mainGroup, groups)

    return workflow
    