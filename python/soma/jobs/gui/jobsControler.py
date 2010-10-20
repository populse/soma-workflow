
from soma.jobs.constants import *
from soma.jobs.jobClient import JobTemplate, UniversalResourcePath, FileTransfer, FileSending, FileRetrieving, Group, Workflow, Jobs
import socket
import os
import ConfigParser
import pickle
import subprocess
import commands

class JobsControler(object):
  
  def __init__(self, test_config_file_path, test_no = 1):
    
    # Test config 
    self.test_config_file_path = test_config_file_path
    self.test_no = 1
   
  @staticmethod
  def getConfigFile():
    result = os.environ.get( 'SOMA_JOBS_CONFIG' )
    if not result or not os.path.exists( result ):
      result = os.path.join( os.environ.get( 'HOME', '' ), '.brainvisa', 'jobs.cfg' )
      if not os.path.exists( result ):
        raise RuntimeError( 'Cannot find soma-jobs configuration file. Perhaps SOMA_JOBS_CONFIG is not proprely set.' )
    return result
  
  
  def getRessourceIds(self):
    resource_ids = []
    somajobs_config = ConfigParser.ConfigParser()
    somajobs_config.read( self.getConfigFile() )
    for cid in somajobs_config.sections():
      if cid != OCFG_SECTION_CLIENT:
        resource_ids.append(cid)
        
    return resource_ids
        
  def getConnection(self, resource_id, login, password, test_no):
    try: 
      connection = Jobs( self.getConfigFile(),
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
  
  def getSubmittedWorkflows(self, connection):
    '''
    returns a list of tuple:
    - workflow id
    - expiration date
    - workflow name
    '''
    result = []
    result.append((-1, None, " "))
    for wf_id in connection.workflows():
      expiration_date, name = connection.workflowInformation(wf_id)
      result.append((wf_id, expiration_date, name))
    return result
    
  def getWorkflow(self, wf_id, connection):
    workflow = connection.submittedWorkflow(wf_id)
    expiration_date = connection.workflowInformation(wf_id)[0]
    return (workflow, expiration_date)
  
  
  def getWorkflowExampleList(self):
    return ["simple", "multiple", "with exception 1", "with exception 2"]
    
  def generateWorkflowExample(self, with_file_transfer, with_universal_resource_path, example_type, file_path):

    wfExamples = WorkflowExamples(with_tranfers = with_file_transfer, test_config_file_path = self.test_config_file_path, test_no = self.test_no, with_universal_resource_path = with_universal_resource_path)
    workflow = None
    if example_type == 0:
      workflow = wfExamples.simpleExample()
    elif example_type == 1:
      workflow = wfExamples.multipleSimpleExample()
    elif example_type == 2:
      workflow = wfExamples.simpleExampleWithException1()
    elif example_type == 3:
      workflow = wfExamples.simpleExampleWithException2()
      
    if workflow:
      file = open(file_path, 'w')
      pickle.dump(workflow, file)
      file.close()
    
    
  def submitWorkflow(self, workflow, name, expiration_date, connection):
    return connection.submitWorkflow(workflow = workflow,                                               expiration_date = expiration_date,
                                    name = name) 
                                    
  def deleteWorkflow(self, wf_id, connection):
    return connection.disposeWorkflow(wf_id)
  
  def changeWorkflowExpirationDate(self, wf_id, date, connection):
    return connection.changeWorkflowExpirationDate(wf_id, date)
    
  def transferInputFiles(self, workflow, connection):
    to_run = []
    for node in workflow.full_nodes:
      if isinstance(node, FileSending):
        status, info = connection.transferStatus(node.local_path)
        if status == READY_TO_TRANSFER:
          to_run.append((0, node.local_path))
        if status == TRANSFERING:
          to_run .append((info[1], node.local_path))
          
    to_run = sorted(to_run, key = lambda element: element[1])
    for transmitted, local_path in to_run:
      print "transfer " + local_path + "already transmitted " + repr(transmitted)
      connection.sendRegisteredTransfer(local_path)
    
  def transferOutputFiles(self, workflow, connection):
    for node in workflow.full_nodes:
      if isinstance(node, FileRetrieving):
        if connection.transferStatus(node.local_path)[0] == READY_TO_TRANSFER:
          connection.retrieve(node.local_path)
          
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
      if isinstance(node, JobTemplate):
        if node.job_id == -1:
          print >> file, names[node][0] + "[shape=box label="+ names[node][1] +"];"
        else:
          status = connection.status(node.job_id)
          if status == NOT_SUBMITTED:
            print >> file, names[node][0] + "[shape=box label="+ names[node][1] +", style=filled, color=" + GRAY +"];"
          elif status == DONE:
            exit_status, exit_value, term_signal, resource_usage = connection.exitInformation(node.job_id)
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
          status = connection.transferStatus(node.local_path)[0]
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
  
  
  
  def __init__(self, with_tranfers, test_config_file_path, test_no, with_universal_resource_path = False):
    '''
    @type with_tranfers: boolean
    '''
    test_config = ConfigParser.ConfigParser()
    test_config.read(test_config_file_path)
    
    hostname = socket.gethostname()
    self.examples_dir = test_config.get(hostname, 'job_examples_dir')
    self.output_dir = test_config.get(hostname, 'job_output_dir') + repr(test_no) + "/"
    
    self.with_transfers = with_tranfers
    self.with_universal_resource_path = with_universal_resource_path
    
    
    if self.with_universal_resource_path:
      # inputs
      self.file0 = UniversalResourcePath("complete/" + "file0", "example", "job_dir", 168)
      self.script1 = UniversalResourcePath("complete/" + "job1.py", "example", "job_dir", 168)
      self.stdin1 = UniversalResourcePath("complete/" + "stdin1", "example", "job_dir", 168)
      self.script2 = UniversalResourcePath("complete/" + "job2.py", "example", "job_dir", 168)
      self.stdin2 = UniversalResourcePath("complete/" + "stdin2", "example", "job_dir", 168)
      self.script3 = UniversalResourcePath("complete/" + "job3.py", "example", "job_dir", 168)
      self.stdin3 = UniversalResourcePath("complete/" + "stdin3", "example", "job_dir", 168)
      self.script4 = UniversalResourcePath("complete/" + "job4.py", "example", "job_dir", 168)
      self.stdin4 = UniversalResourcePath("complete/" + "stdin4", "example", "job_dir", 168)
      
      self.exceptionJobScript = UniversalResourcePath("simple/exceptionJob.py", "example", "job_dir", 168, "exception_job")
      
      if self.with_transfers:
        print "transfers !!!"
        # outputs
        self.file11 = FileRetrieving(self.output_dir + "file11", 168, "file11")
        self.file12 = FileRetrieving(self.output_dir + "file12", 168, "file12")
        self.file2 = FileRetrieving(self.output_dir + "file2", 168, "file2")
        self.file3 = FileRetrieving(self.output_dir + "file3", 168, "file3")
        self.file4 = FileRetrieving(self.output_dir + "file4", 168, "file4")
      else:
        # outputs
        self.file11 = UniversalResourcePath("file11", "example", "output_dir", 168)
        self.file12 = UniversalResourcePath("file12", "example", "output_dir",168)
        self.file2 = UniversalResourcePath("file2", "example", "output_dir",168)
        self.file3 = UniversalResourcePath("file3", "example", "output_dir",168)
        self.file4 = UniversalResourcePath("file4", "example", "output_dir",168)
      
    elif self.with_transfers and not self.with_universal_resource_path:
      # outputs
      self.file11 = FileRetrieving(self.output_dir + "file11", 168, "file11")
      self.file12 = FileRetrieving(self.output_dir + "file12", 168, "file12")
      self.file2 = FileRetrieving(self.output_dir + "file2", 168, "file2")
      self.file3 = FileRetrieving(self.output_dir + "file3", 168, "file3")
      self.file4 = FileRetrieving(self.output_dir + "file4", 168, "file4")
      
      # inputs
      self.file0 = FileSending(self.examples_dir + "complete/" + "file0", 168, "file0")
      self.script1 = FileSending(self.examples_dir + "complete/" + "job1.py", 168, "job1_py")
      self.stdin1 = FileSending(self.examples_dir + "complete/" + "stdin1", 168, "stdin1")
      self.script2 = FileSending(self.examples_dir + "complete/" + "job2.py", 168, "job2_py")
      self.stdin2 = FileSending(self.examples_dir + "complete/" + "stdin2", 168, "stdin2")
      self.script3 = FileSending(self.examples_dir + "complete/" + "job3.py", 168, "job3_py")
      self.stdin3 = FileSending(self.examples_dir + "complete/" + "stdin3", 168, "stdin3")
      self.script4 = FileSending(self.examples_dir + "complete/" + "job4.py", 168, "job4_py")
      self.stdin4 = FileSending(self.examples_dir + "complete/" + "stdin4", 168, "stdin4")
      
      self.exceptionJobScript = FileSending(self.examples_dir + "simple/exceptionJob.py", 168, "exception_job")
    else:
      # outputs
      self.file11 = self.output_dir + "file11"
      self.file12 = self.output_dir + "file12"
      self.file2 = self.output_dir + "file2"
      self.file3 = self.output_dir + "file3"
      self.file4 = self.output_dir + "file4"
      
      # inputs
      self.file0 = self.examples_dir + "complete/" + "file0"
      self.script1 = self.examples_dir + "complete/" + "job1.py"
      self.stdin1 = self.examples_dir + "complete/" + "stdin1"
      self.script2 = self.examples_dir + "complete/" + "job2.py"
      self.stdin2 = self.examples_dir + "complete/" + "stdin2"
      self.script3 = self.examples_dir + "complete/" + "job3.py"
      self.stdin3 = self.examples_dir + "complete/" + "stdin3"
      self.script4 = self.examples_dir + "complete/" + "job4.py"
      self.stdin4 = self.examples_dir + "complete/" + "stdin4"
      
      self.exceptionJobScript = self.examples_dir + "simple/exceptionJob.py"
      
  def job1(self):
    if self.with_transfers: 
      if not self.with_universal_resource_path:
        job1 = JobTemplate(["python", self.script1, self.file0,  self.file11, self.file12, "20"], 
                          [self.file0, self.script1, self.stdin1], 
                          [self.file11, self.file12], 
                          self.stdin1, False, 168, "job1")
      else:
        job1 = JobTemplate(["python", self.script1, self.file0,  self.file11, self.file12, "20"], 
                          [], 
                          [self.file11, self.file12], 
                          self.stdin1, False, 168, "job1")
    else:
      job1 = JobTemplate(["python", self.script1, self.file0,  self.file11, self.file12, "20"], None, None, self.stdin1, False, 168, "job1")
    return job1
    
  def job2(self):
    if self.with_transfers: 
      if not self.with_universal_resource_path:
        job2 = JobTemplate(["python", self.script2, self.file11,  self.file0, self.file2, "30"], 
                          [self.file0, self.file11, self.script2, self.stdin2], 
                          [self.file2], 
                          self.stdin2, False, 168, "job2")
      else:
        job2 = JobTemplate(["python", self.script2, self.file11,  self.file0, self.file2, "30"], 
                          [], 
                          [self.file2], 
                          self.stdin2, False, 168, "job2")
    else:
      job2 = JobTemplate(["python", self.script2, self.file11,  self.file0, self.file2, "30"], None, None, self.stdin2, False, 168, "job2")
    return job2
    
  def job3(self):
    if self.with_transfers: 
      if not self.with_universal_resource_path:
        job3 = JobTemplate(["python", self.script3, self.file12,  self.file3, "30"], 
                            [self.file12, self.script3, self.stdin3], 
                            [self.file3], 
                            self.stdin3, False, 168, "job3")
      else:
        job3 = JobTemplate(["python", self.script3, self.file12,  self.file3, "30"], 
                           [], 
                           [self.file3], 
                           self.stdin3, False, 168, "job3")
    else:
      job3 = JobTemplate(["python", self.script3, self.file12,  self.file3, "30"], None, None, self.stdin3, False, 168, "job3")
    return job3
    
  def job4(self):
    if self.with_transfers: 
      if not self.with_universal_resource_path:
        job4 = JobTemplate(["python", self.script4, self.file2,  self.file3, self.file4, "10"], 
                           [self.file2, self.file3, self.script4, self.stdin4], 
                           [self.file4], 
                           self.stdin4, False, 168, "job4")
      else:
        job4 = JobTemplate(["python", self.script4, self.file2,  self.file3, self.file4, "10"], 
                           [], 
                           [self.file4], 
                           self.stdin4, False, 168, "job4")
    else:
      job4 = JobTemplate(["python", self.script4, self.file2,  self.file3, self.file4, "10"], None, None, self.stdin4, False, 168, "job4")
    return job4
    

      
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
  
    group_1 = Group([job2, job3], 'group_1')
    group_2 = Group([job1, group_1], 'group_2')
    mainGroup = Group([group_2, job4])
    
    workflow = Workflow(nodes, dependencies, mainGroup, [group_1, group_2])
    
    return workflow
  
  def simpleExampleWithException1(self):
                                                          
    # jobs
    if self.with_transfers:
      if not self.with_universal_resource_path:
        job1 = JobTemplate(["python", self.exceptionJobScript], 
                          [self.exceptionJobScript, self.file0, self.script1, self.stdin1], 
                          [self.file11, self.file12], 
                          self.stdin1, False, 168, "job1 with exception")
      else:
        job1 = JobTemplate(["python", self.exceptionJobScript], 
                  [], 
                  [self.file11, self.file12], 
                  self.stdin1, False, 168, "job1 with exception")
    else:
      job1 = JobTemplate(["python", self.exceptionJobScript], None, None,  self.stdin1, False, 168, "job1 with exception")                   
    
    job2 = self.job2()
    job3 = self.job3()
    job4 = self.job4()
          
    nodes = [job1, job2, job3, job4]
  

    dependencies = [(job1, job2), 
                    (job1, job3),
                    (job2, job4), 
                    (job3, job4)]
  
    group_1 = Group([job2, job3], 'group_1')
    group_2 = Group([job1, group_1], 'group_2')
    mainGroup = Group([group_2, job4])
    
    workflow = Workflow(nodes, dependencies, mainGroup, [group_1, group_2])
    
    return workflow
  
  
  def simpleExampleWithException2(self):
   
    # jobs
    job1 = self.job1()
    job2 = self.job2()
    job4 = self.job4()
    
    if self.with_transfers:
      if not self.with_universal_resource_path:
        job3 = JobTemplate(["python", self.exceptionJobScript],
                          [self.exceptionJobScript, self.file12, self.script3, self.stdin3],
                          [self.file3],
                          None, False, 168, "job3 with exception")
      else:
        job3 = JobTemplate(["python", self.exceptionJobScript],
                          [],
                          [self.file3],
                          None, False, 168, "job3 with exception")
    else:
      job3 = JobTemplate(["python", self.exceptionJobScript], None, None, None, False, 168, "job3 with exception")
      
      
           
    nodes = [job1, job2, job3, job4]

    dependencies = [(job1, job2), 
                    (job1, job3),
                    (job2, job4), 
                    (job3, job4)]
  
    group_1 = Group([job2, job3], 'group_1')
    group_2 = Group([job1, group_1], 'group_2')
    mainGroup = Group([group_2, job4])
    
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
    
    group1 = Group(workflow1.mainGroup.elements, "simple example")
    group2 = Group(workflow2.mainGroup.elements, "simple with exception in Job1")
    group3 = Group(workflow3.mainGroup.elements, "simple with exception in Job3")
    mainGroup = Group([group1, group2, group3])
     
    groups = [group1, group2, group3]
    groups.extend(workflow1.groups)
    groups.extend(workflow2.groups)
    groups.extend(workflow3.groups)
    
    workflow = Workflow(nodes, dependencies, mainGroup, groups)
    return workflow