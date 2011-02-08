# -*- coding: utf-8 -*-

from soma.workflow.constants import *
from soma.workflow.client import Job, SharedResourcePath, FileTransfer,  Group, Workflow, WorkflowController
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
    config_path = os.getenv('SOMA_WORKFLOW_CONFIG')
    if not config_path or not os.path.isfile(config_path):
      config_path = os.path.expanduser("~/.soma-workflow.cfg")
    if not config_path or not os.path.isfile(config_path):
      config_path = os.path.dirname(__file__)
      config_path = os.path.dirname(__file__)
      config_path = os.path.dirname(__file__)
      config_path = os.path.dirname(__file__)
      config_path = os.path.join(config_path, "etc/soma-workflow.cfg")
    if not config_path or not os.path.isfile(config_path):
      config_path = "/etc/soma-workflow.cfg"
    if not config_path or not os.path.isfile(config_path):
      raise Exception("Can't find the soma-workflow configuration file \n")
    return config_path
  
  
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
      connection = WorkflowController(resource_id, 
                                      login, 
                                      password)
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
    if workflow:
      expiration_date = connection.workflows([wf_id])[wf_id][1]
    else:
      expiration_date = None
    return (workflow, expiration_date)
  
  
  def getWorkflowExampleList(self):
    return ["simple", "multiple", "with exception 1", "with exception 2", "command check test", "special transfers", "hundred of jobs", "ten jobs", "fake pipelineT1"]
    
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
  
                                    
  def restart_workflow(self, workflow_id, connection):
    return connection.restart_workflow(workflow_id)
                                    
  def delete_workflow(self, wf_id, connection):
    return connection.delete_workflow(wf_id)
  
  def change_workflow_expiration_date(self, wf_id, date, connection):
    return connection.change_workflow_expiration_date(wf_id, date)
    
  def transferInputFiles(self, workflow, connection, buffer_size = 512**2):
   
    to_transfer = []
    for ft in workflow.transfer_dict.itervalues():
      status, info = connection.transfer_status(ft.engine_path)
      if status == FILES_ON_CLIENT:
        to_transfer.append((0, ft.engine_path))
      if status == TRANSFERING_FROM_CLIENT_TO_CR:
        to_transfer.append((info[1], ft.engine_path))
          
    to_transfer = sorted(to_transfer, key = lambda element: element[1])
    for transmitted, engine_path in to_transfer:
      print "send to " + engine_path + " already transmitted size =" + repr(transmitted)
      connection.transfer_files(engine_path, buffer_size)
    
  def transferOutputFiles(self, workflow, connection, buffer_size = 512**2):
    to_transfer = []
    for ft in workflow.transfer_dict.itervalues():
      status, info = connection.transfer_status(ft.engine_path)
      if status == FILES_ON_CR:
        to_transfer.append((0, ft.engine_path))
      if status == TRANSFERING_FROM_CR_TO_CLIENT:
        to_transfer .append((info[1], ft.engine_path))

    to_transfer = sorted(to_transfer, key = lambda element: element[1])
    for transmitted, engine_path in to_transfer:
      print "retrieve " + engine_path + " already transmitted size" + repr(transmitted)
      connection.transfer_files(engine_path, buffer_size)

          
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
    for node in workflow.jobs:
      current_id = current_id + 1
      names[node] = ("node" + repr(current_id), "\""+node.name+"\"")
    for ar in workflow.dependencies:
      print >> file, names[ar[0]][0] + " -> " + names[ar[1]][0] 
    for node in workflow.jobs:
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
        if not node.engine_path:
          print >> file, names[node][0] + "[label="+ names[node][1] +"];"
        else:
          status = connection.transfer_status(node.engine_path)[0]
          if status == FILES_DONT_EXIST:
            print >> file, names[node][0] + "[label="+ names[node][1] +", style=filled, color=" + GRAY +"];"
          elif status == FILES_ON_CR or status == FILES_ON_CLIENT_AND_CR or status == FILES_ON_CLIENT:
            print >> file, names[node][0] + "[label="+ names[node][1] +", style=filled, color=" + BLUE +"];"
          elif status == TRANSFERING_FROM_CLIENT_TO_CR or status == TRANSFERING_FROM_CR_TO_CLIENT:
            print >> file, names[node][0] + "[label="+ names[node][1] +", style=filled, color=" + GREEN +"];"
          elif status == FILES_UNDER_EDITION:
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
  
  
  def ten_jobs(self):
      
    jobs = []
    for i in range(0,10):
      job = self.job_sleep(60)
      jobs.append(job)
     
    dependencies = []
    
    workflow = Workflow(jobs, dependencies)
    
    return workflow
  
  def hundred_of_jobs(self):
    
    jobs = []
    for i in range(0,100):
      job = self.job_sleep(60)
      jobs.append(job)
     
    dependencies = []
 
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
      job2 = self.job_sleep(120) 
      job2.name ="Gray/white segmentation"
      jobs.append(job2)
      job3 = self.job_sleep(400) 
      job3.name = "Left hemisphere sulci recognition"
      jobs.append(job3)
      job4 = self.job_sleep(400) 
      job4.name = "Right hemisphere sulci recognition"
      jobs.append(job4)

      dependencies.append((job1, job2))
      dependencies.append((job2, job3))
      dependencies.append((job2, job4))
      
      group_sulci = Group(name="Sulci recognition", 
                          elements=[job3, job4])
      group_subject = Group(name="sulci recognition -- subject "+ repr(i), 
                            elements=[job1, job2, group_sulci])

      root_group.append(group_subject)
   
    workflow = Workflow(jobs, dependencies, root_group)

    return workflow
    