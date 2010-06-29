import subprocess

'''
@author: Soizic Laguitton
@organization: U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''

class Observer(object):
  '''Design pattern observer. Observer is an abstract class. '''
  def update(self, subject):
    raise Exception('Observer is an abstract class')
  


class Subject(object):
  '''Design pattern observer. Subject is an abstract class. '''
  
  def __init__(self):
    self.observer_list = []
    
  def attach(self, observer):
    self.observer_list.append(observer)
    
  def detach(self, observer):
    self.observer_list.remove(observer)
    
  def notify(self):
    for observer in self.observer_list:
      observer.update(self)
  




class Workflow(Subject):
  '''
  Workflow is an abstract class.
  members:
  - is_asynchronous : boolean, 
                   True if the run method is asynchronous (ie 
                   the methods returns immediately). 
                   If False the method wait returns immediately.
  '''
  RUNNING = "running"
  DONE = "done"
  
  def __init__(self, is_asynchronous):
    '''
    @type  is_asynchronous: boolean
    @param is_asynchronous: True if the run method is asynchronous. 
                         If False the wait method returns immediately
    @type  wf_sequence: sequence of L{Workflow}
    @param wf_sequence: Empty if the workflow is an elementary workflow. 
                        Hold a sequence of sub-workflows if the workflow is composite.
    @type  status: string
    @param status: RUNNING or DONE
    @type  name: string
    @param name: name of the workflow
    '''
    Subject.__init__(self)
    self.is_asynchronous = is_asynchronous
    self.wf_sequence = []
    self.status = ""
    self.name = ""
    
  def run(self):
    '''
    starts the execution of the workflow. 
    if self.is_asynchronous is True the method returns immediately.
    '''
    raise Exception('Workflow is an abstract class.')
  
  def wait(self):
    '''
    waits for the workflow to be executed.
    If self.is_asynchronous is False the method returns immediately.
    '''
    raise Exception('Workflow is an abstract class.')
  
  
class SerialWorkflow(Workflow):
  '''
  Workflows meant to be executed one after the other.
  '''
  def __init__(self, wf_sequence):
    '''
    @type  wf_sequence: sequence of L{Workflow}
    @param wf_sequence: instances of L{Workflow} which are meant 
                              to be executed one after the other.
                              The sequence must contain at least 2 workflows.
                              The workflows can't be instances of L{ParallelWorkflow}.
    L{SerialWorkflow} are always synchronous.
    '''
    if len(wf_sequence) < 2: 
      raise Exception('SerialWorkflow must contain at least 2 elements.')
    for wf in wf_sequence:
      if isinstance(wf,SerialWorkflow): 
        raise Exception("The components of a SerialWorkflow can't be instances of SerialWorkflow.")
    
    Workflow.__init__(self, False)
    self.wf_sequence = wf_sequence
    self.progress = 0
  
  def run(self):
    '''
    Implementation of the L{Workflow} method.
    '''
    self.status = self.RUNNING
    self.notify()
    for wf in self.wf_sequence:
      wf.run()
      wf.wait()
    self.status = self.DONE
    self.notify()
      
  def wait(self):
    '''
    Implementation of the L{Workflow} method.
    '''
    pass
    
  
class ParallelWorkflow(Workflow):
  '''
  Workflows meant to be exected in parallel.
  '''
  def __init__(self, wf_sequence):
    '''
    @type  wf_sequence: sequence of L{Workflow}
    @param wf_sequence: instances of L{Workflow} which are meant 
                              to be executed in parallel.
                              The sequence must contain at least 2 workflows.
                              The workflows can't be instances of L{ParallelWorkflow}.
    A L{ParallelWorkflow} is asynchronous if and only if each workflow 
    of the sequence is asynchronous.
    '''
    if len(wf_sequence) < 2: 
      raise Exception('ParallelWorkflow must contain at least 2 elements.')
    for wf in wf_sequence:
      if isinstance(wf,ParallelWorkflow): 
        raise Exception("The components of a ParallelWorkflow can't be instances of ParallelWorkflow.")
    
    is_asynchronous = True
    for wf in wf_sequence:
      is_asynchronous = is_asynchronous and wf.is_asynchronous
    Workflow.__init__(self, is_asynchronous)
    self.wf_sequence = wf_sequence
    
  def run(self):
    '''
    Implementation of the L{Workflow} method.
    '''
    self.status = self.RUNNING
    self.notify()
    if self.is_asynchronous:
      for wf in self.wf_sequence:
        wf.run()
    else:
      # runs the asynchronous workflows first
      for wf in self.wf_sequence:
        if wf.is_asynchronous:
          wf.run()
      # runs the synchronous worflows
      for wf in self.wf_sequence:
        if not wf.is_asynchronous:
          wf.run()
      # waits for the asynchronous workflows
      for wf in self.wf_sequence:
        if wf.is_asynchronous: 
          wf.wait() 
      self.status = self.DONE
      self.notify()
   
      
  def wait(self):
    '''
    Implementation of the L{Workflow} method.
    '''
    if self.is_asynchronous:
      for wf in self.wf_sequence:
        wf.wait()
      self.status = self.DONE
      self.notify()
  

class FileSending(Workflow):
  '''
  FileSending is an elementary synchronous Workflow. 
  It transfer a file from the user machine (remote location) to the cluster (local location) using the method L{sendFile} of an instance of soma.jobs.jobClient.Jobs.
  '''
  def __init__(self, file_transferer, remote_file_path, disposal_timeout = 168, name = None):
    '''
    @type  file_transferer: L{soma.jobs.jobClient.Jobs}
    @param file_transferer: instance of soma.jobs.jobClient.Jobs used to transfer the file
    The other parameters are the argument of the method L{sendFile} of soma.jobs.jobClient.Jobs.
    '''
    Workflow.__init__(self, False)
    #instance of L{soma.jobs.jobClient.Jobs}:
    self.file_transferer = file_transferer
    #input of L{sendFile} method:
    self.remote_file_path = remote_file_path
    self.disposal_timeout = disposal_timeout
    #output of L{sendFile} method:
    self.local_file_path = ""
    
    if name:
      self.name = name
    else:
      self.name = "send_" + self.remote_file_path
      
  def run(self):
    '''
    Implementation of the L{Workflow} method.
    '''
    self.status = self.RUNNING
    self.notify()
    print "send file " + self.name
    self.local_file_path = self.file_transferer.sendFile(self.remote_file_path)
    self.status = self.DONE
    self.notify()
    
    
  def wait(self):
    '''
    Implementation of the L{Workflow} method.
    '''
    pass
  
  

class FileRetrieving(Workflow):
  '''
  FileRetrieving is an elementary synchronous Workflow. 
  It transfer a file from the cluster (local location) to the user machine (remote location)using the method L{retriveFile} of an instance of soma.jobs.jobClient.Jobs.
  '''
  def __init__(self, file_transferer, remote_file_path, disposal_timeout = 168, name = None):
    '''
    @type  file_transferer: L{soma.jobs.jobClient.Jobs}
    @param file_transferer: instance of soma.jobs.jobClient.Jobs used to transfer the file
    The other parameters are the argument of the method L{retrieveFile} of soma.jobs.jobClient.Jobs.
    '''
    Workflow.__init__(self, False)
    #instance of L{soma.jobs.jobClient.Jobs}:
    self.file_transferer = file_transferer
    
    self.local_file_path = self.file_transferer.registerFileTransfer(remote_file_path, disposal_timeout)
    self.remote_file_path = remote_file_path
    self.name = name

    if name:
      self.name = name
    else:
      self.name = "retr_" + self.remote_file_path

  def run(self):
    '''
    Implementation of the L{Workflow} method.
    '''
    self.status = self.RUNNING
    self.notify()
    print "retrive file " + self.name
    self.file_transferer.retrieveFile(self.local_file_path)
    self.status = self.DONE
    self.notify()
    
  def wait(self):
    '''
    Implementation of the L{Workflow} method.
    '''
    pass
  

class JobRunning(Workflow):
  '''
  Job is an elementary asynchronous Workflow. 
  It runs a job using an instance of soma.jobs.jobClient.Jobs.
  '''
  def __init__(self, 
               job_submitter, 
               command,
               referenced_input_files=None,
               referenced_output_files=None,
               stdin=None,
               join_stderrout=False,
               disposal_timeout=168,
               name_description=None,
               stdout_path=None,
               stderr_path=None,
               working_directory=None,
               parallel_job_info=None):
    '''
    @type  job_submitter: L{soma.jobs.jobClient.Jobs}
    @param job_submitter: instance of soma.jobs.jobClient.Jobs used to submit the job.
    The other parameters are the argument of the method L{submit} of soma.jobs.jobClient.Jobs except that local the file path (valid on the cluster)can be replaced by a instance of L{FileSending} or L{FileRetrieving}
     '''
    Workflow.__init__(self, True)
    #instance of L{soma.jobs.jobClient.Jobs}:
    self.job_submitter = job_submitter
    #input of L{submit} method:
    self.command = command
    self.referenced_input_files = referenced_input_files
    self.referenced_output_files = referenced_output_files
    self.stdin = stdin
    self.join_stderrout = join_stderrout
    self.disposal_timeout = disposal_timeout
    self.name_description = name_description
    self.stdout_path = stdout_path
    self.stderr_path = stderr_path
    self.working_directory = working_directory
    self.parallel_job_info = parallel_job_info
    #output of L{submit} method
    self.job_id = -1
      
    self.name = name_description
  
  def run(self):
    '''
    Implementation of the L{Workflow} method.
    '''
    # replacement of every instance of L{FileSending} and L{FileRetrieving} 
    # objet by a the corresponding local file path
    
    self.status = self.RUNNING
    self.notify()
    
    tr_command = self.__tr_seq(self.command)
    tr_referenced_input_files = self.__tr_seq(self.referenced_input_files)
    tr_referenced_output_files = self.__tr_seq(self.referenced_output_files)
    tr_stdin = self.__tr(self.stdin)
    tr_stdout_path = self.__tr(self.stdout_path)
    tr_stderr_path = self.__tr(self.stderr_path)
      
    self.job_id = self.job_submitter.submit(command = tr_command,
                                            referenced_input_files =tr_referenced_input_files,
                                            referenced_output_files=tr_referenced_output_files,
                                            stdin=tr_stdin,
                                            join_stderrout=self.join_stderrout,
                                            disposal_timeout=self.disposal_timeout,
                                            name_description=self.name_description,
                                            stdout_path=tr_stdout_path,
                                            stderr_path=tr_stderr_path,
                                            working_directory=self.working_directory,
                                            parallel_job_info=self.parallel_job_info)
    print self.name_description + "submitted."
    
  def wait(self):
    '''
    Implementation of the L{Workflow} method.
    '''
    if self.job_id != -1:
      print ">> waiting for " + self.name_description 
      self.job_submitter.wait([self.job_id])
      print "<< end"
      self.status = self.DONE
      self.notify()
      
  def __tr_seq(self, seq):
    tr_seq = []
    for elt in seq:
      if isinstance(elt, FileSending) or isinstance(elt, FileRetrieving):
        tr_seq.append(elt.local_file_path)
      else:
        tr_seq.append(elt)
    return tr_seq
      
  def __tr(self,value):
    if isinstance(value, FileSending) or isinstance(value, FileRetrieving):
      tr = value.local_file_path
    else:
      tr = value
    return tr



class workflowDotView(Observer):
  
  def __init__(self, workflow, dot_file_path, graph_file_path):
    self.workflow = workflow
    self.__attachToSubjects(self.workflow)
    self.dot_file_path = dot_file_path
    self.graph_file_path = graph_file_path
    
  def __del__(self):
    self.workflow.detach(self)
  
  def update(self, workflow):
    '''
    Creates a dot file which is a view the workflow.
    @type  workflow: L{Workflow}
    @param workflow: model of workflow
    @type  filePath: string
    @param filePath: output file path
    '''
    representation = self.representation(self.workflow)
    if isinstance(self.workflow, ParallelWorkflow):
      representation = representation[1:len(representation)-2:1]
    file = open(self.dot_file_path, "w")
    print >> file, "digraph structs {"
    print >> file, "node [shape=record];"
    print >> file, "struct1 [shape=record, label=\""+representation+"\" ];"
    print >> file, "}"
    file.close()
    
    command = "dot -Tpng " + self.dot_file_path + " -o " + self.graph_file_path
    dot_process = subprocess.Popen(command, shell = True)
    print command
    raw_input()
  
  def representation(self, workflow):
    if len(workflow.wf_sequence)==0: 
      if workflow.status == Workflow.RUNNING:
        return workflow.name + "_R"
      if workflow.status == Workflow.DONE:
        return workflow.name + "_done"
      else: return workflow.name
      
    result = "{ " + self.representation(workflow.wf_sequence[0])
    for wf in workflow.wf_sequence[1::1]:
      result = result + "|" + self.representation(wf)
    result = result + " }"
    return result
    

  def __attachToSubjects(self, workflow):
    if len(workflow.wf_sequence)==0: 
      workflow.attach(self)
    else:
      for wf in workflow.wf_sequence:
        self.__attachToSubjects(wf)