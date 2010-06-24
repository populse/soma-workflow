'''
@author: Soizic Laguitton
@organization: U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''

class Workflow(object):
  '''
  Workflow is an abstract class.
  members:
  - asynchronous : boolean, 
                   True if the run method is asynchronous (ie 
                   the methods returns immediately). 
                   If False the method wait returns immediately.
  '''
  def __init__(self, asynchronous):
    '''
    @type  asynchronous: boolean
    @param asynchronous: True if the run method is asynchronous. 
                         If False the wait method returns immediately
    '''
    self.asynchronous = asynchronous
    
  def run(self):
    '''
    starts the execution of the workflow. 
    if self.asynchronous is True the method returns immediately.
    '''
    raise Exception('Workflow is an abstract class.')
  
  def wait(self):
    '''
    waits for the workflow to be executed.
    If self.asynchronous is False the method returns immediately.
    '''
    raise Exception('Workflow is an abstract class.')
  
  #def executionProgress(self):
    #'''
    #gives an estimation of the workflow execution progression.
    #@rtype:  percentage
    #'''
    #raise Exception('Workflow is an abstract class.')
  
  def representation(self):
    raise Exception('Workflow is an abstract class.')
  
  
class SerialWorkflow(Workflow):
  '''
  Workflows meant to be executed one after the other.
  '''
  def __init__(self, workflow_sequence):
    '''
    @type  workflow_sequence: sequence of L{Workflow}
    @param workflow_sequence: instances of L{Workflow} which are meant 
                              to be executed one after the other.
                              The sequence must contain at least 2 workflows.
                              The workflows can't be instances of L{ParallelWorkflow}.
    L{SerialWorkflow} are always synchronous.
    '''
    if len(workflow_sequence) < 2: 
      raise Exception('SerialWorkflow must contain at least 2 elements.')
    for wf in workflow_sequence:
      if isinstance(wf,SerialWorkflow): 
        raise Exception("The components of a SerialWorkflow can't be instances of SerialWorkflow.")
    
    
    Workflow.__init__(self, False)
    self.workflow_sequence = workflow_sequence
    self.progress = 0
  
  def run(self):
    '''
    Implementation of the L{Workflow} method.
    '''
    for wf in self.workflow_sequence:
      wf.run()
      wf.wait()
       
  def wait(self):
    '''
    Implementation of the L{Workflow} method.
    '''
    pass
  
  def representation(self):
    result = "{ " + self.workflow_sequence[0].representation()
    for wf in self.workflow_sequence[1::1]:
      result = result + "|" + wf.representation()
    result = result + " }"
    return result
  #def executionProgress(self):
    
  
class ParallelWorkflow(Workflow):
  '''
  Workflows meant to be exected in parallel.
  '''
  def __init__(self, workflow_sequence):
    '''
    @type  workflow_sequence: sequence of L{Workflow}
    @param workflow_sequence: instances of L{Workflow} which are meant 
                              to be executed in parallel.
                              The sequence must contain at least 2 workflows.
                              The workflows can't be instances of L{ParallelWorkflow}.
    A L{ParallelWorkflow} is asynchronous if and only if each workflow 
    of the sequence is asynchronous.
    '''
    if len(workflow_sequence) < 2: 
      raise Exception('ParallelWorkflow must contain at least 2 elements.')
    for wf in workflow_sequence:
      if isinstance(wf,ParallelWorkflow): 
        raise Exception("The components of a ParallelWorkflow can't be instances of ParallelWorkflow.")
    
    asynchronous = True
    for wf in workflow_sequence:
      asynchronous = asynchronous and wf.asynchronous
    Workflow.__init__(self, asynchronous)
    self.workflow_sequence = workflow_sequence
    
  def run(self):
    '''
    Implementation of the L{Workflow} method.
    '''
    if self.asynchronous:
      for wf in self.workflow_sequence:
        wf.run()
    else:
      # runs the asynchronous workflows first
      for wf in self.workflow_sequence:
        if wf.asynchronous:
          wf.run()
      # runs the synchronous worflows
      for wf in self.workflow_sequence:
        if not wf.asynchronous:
          wf.run()
      # waits for the asynchronous workflows
      for wf in self.workflow_sequence:
        if wf.asynchronous: wf.wait()
   
      
  def wait(self):
    '''
    Implementation of the L{Workflow} method.
    '''
    if self.asynchronous:
      for wf in self.workflow_sequence:
        wf.wait()
  
  def representation(self):
    result = "{ " + self.workflow_sequence[0].representation()
    for wf in self.workflow_sequence[1::1]:
      result = result + "|" + wf.representation()
    result = result + " }"
    return result
    

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
    Workflow.__init__(self, True)
    #instance of L{soma.jobs.jobClient.Jobs}:
    self.file_transferer = file_transferer
    #input of L{sendFile} method:
    self.remote_file_path = remote_file_path
    self.disposal_timeout = disposal_timeout
    #output of L{sendFile} method:
    self.local_file_path = ""
    
    self.name = name

  def run(self):
    '''
    Implementation of the L{Workflow} method.
    '''
    print "send file " + self.name
    self.local_file_path = self.file_transferer.sendFile(self.remote_file_path)
    
  def wait(self):
    '''
    Implementation of the L{Workflow} method.
    '''
    pass
  
  def representation(self):
    if self.name:
      return "send_" + self.name
    else:
      return "send_" + self.remote_file_path

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
    Workflow.__init__(self, True)
    #instance of L{soma.jobs.jobClient.Jobs}:
    self.file_transferer = file_transferer
    
    self.local_file_path = self.file_transferer.registerFileTransfer(remote_file_path, disposal_timeout)
    self.remote_file_path = remote_file_path
    self.name = name

  def run(self):
    '''
    Implementation of the L{Workflow} method.
    '''
    print "retrive file " + self.name
    self.file_transferer.retrieveFile(self.local_file_path)
    
  def wait(self):
    '''
    Implementation of the L{Workflow} method.
    '''
    pass
  
  def representation(self):
    if self.name:
      return "retr_" + self.name
    else:
      return "retr_" + self.file_transferer.transferInformation(self.local_file)[1]



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
    
  
  def run(self):
    '''
    Implementation of the L{Workflow} method.
    '''
    # replacement of every instance of L{FileSending} and L{FileRetrieving} 
    # objet by a the corresponding local file path
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
      
  def representation(self):
      return self.name_description
      
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

def workflowToDot(workflow, filePath):
  '''
  Creates a dot file which is a view the workflow.
  @type  workflow: L{Workflow}
  @param workflow: model of workflow
  @type  filePath: string
  @param filePath: output file path
  '''
  representation = workflow.representation()
  
  if isinstance(workflow, ParallelWorkflow):
    representation = representation[1:len(representation)-2:1]
  file = open(filePath, "w")
  print >> file, "digraph structs {"
  print >> file, "node [shape=record];"
  print >> file, "struct1 [shape=record, label=\""+representation+"\" ];"
  print >> file, "}"
  file.close()
  

    

    