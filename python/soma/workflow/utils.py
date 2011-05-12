
import copy
from soma.workflow.client import Workflow, Group, Job


def optimize_workflow(workflow):

  # find job without input dep:
  starting_points = []
  for job in workflow.jobs:
    no_dep = True
    for dep in workflow.dependencies:
      if dep[1] == job:
        no_dep = False
        break
    if no_dep:
      starting_points.append(job)
        
  # explore all the graph from the starting_points:
  explored = set()
  current_serial_job = SerialJob()
  serial_jobs = []
  for root in starting_points:
    explore(root, 
            current_serial_job,
            serial_jobs,
            explored,
            workflow)

  # built the new workflow :
  to_remove = {}
  new_jobs = []
  for serial_job in serial_jobs:
    # new job creation:
    new_command = []
    new_name = "|| "
    new_referenced_input_files = []
    new_referenced_output_files = []
    for job in serial_job.job_sequence():
      new_command.extend(job.command)
      new_command.append(";")
      new_name = new_name + job.name + " || "
      for in_el in job.referenced_input_files:
        if not in_el in new_referenced_input_files:
          new_referenced_input_files.append(in_el)
      for out_el in job.referenced_output_files:
        if not out_el in new_referenced_output_files:
          new_referenced_output_files.append(in_el)

    if isinstance(serial_job.group(), Group) and \
      set(serial_job.group().elements) == set(serial_job.job_sequence()):
      new_name = serial_job.group().name
 
    new_job = Job(command=new_command,
                  referenced_input_files=new_referenced_input_files,
                  referenced_output_files=new_referenced_output_files,
                  name=new_name,
                  working_directory=serial_job.working_directory())
    
    new_jobs.append(new_job)
    for job in serial_job.job_sequence():
      to_remove[job] = new_job

  # second loop for the dependencies 
  dependencies = []
  for dep in workflow.dependencies:
    from_valid = not dep[0] in to_remove
    to_valid = not dep[1] in to_remove
    if from_valid and to_valid:
      dependencies.append(dep)
    elif not from_valid and to_valid:
      dependencies.append((to_remove[dep[0]], dep[1]))
    elif not to_valid and from_valid:
      dependencies.append((dep[0], to_remove[dep[1]]))
    elif not to_valid and not from_valid:
      if to_remove[dep[0]] != to_remove[dep[1]]:
        dependencies.append((to_remove[dep[0]], to_remove[dep[1]]))

  root_group = process_group(workflow.root_group, to_remove, "root_group")
  
  jobs = []
  for job in workflow.jobs:
    if job in to_remove:
      if not to_remove[job] in jobs:
        jobs.append(to_remove[job])
    else:
      jobs.append(job) 
  
  new_workflow = Workflow(jobs,
                          dependencies,
                          root_group)

  return new_workflow

def process_group(group, to_remove, name):
  new_group = []
  for element in group:
    if isinstance(element, Job):
      if element in to_remove:
        if not to_remove[element] in new_group:
          new_group.append(to_remove[element])
      else:
        new_group.append(element)
    else:
      
      new_group.append(process_group(element.elements, to_remove, element.name))  
  return Group(new_group, name)


class SerialJob(object):

  _job_sequence = None
  _input_dep = None
  _output_dep = None
  _working_directory = None
  _group = None

  def __init__(self):
    self._job_sequence = []
    self._input_dep = []
    self._output_dep = []
    self._working_directory = None
    self._group = None

  def job_sequence(self):
    return self._job_sequence

  def input_dep(self):
    return self._input_dep

  def output_dep(self):
    return self._output_dep

  def working_directory(self):
    return self._working_directory

  def group(self):
    return self._group

  def is_valid(self):
    return len(self._job_sequence) > 1
    
  def add_job(self, job, group, input_dep, output_dep):
    assert self.is_consistent(job, group)

    if not self._job_sequence:
      self._working_directory = job.working_directory
      self._group = group
      self._input_dep = input_dep
    
    self._job_sequence.append(job)
    self._output_dep = output_dep

  def is_consistent(self, job, group):
    if not self._job_sequence:
      return True

    is_consistent = job.working_directory == self._working_directory and \
                    group == self._group and \
                    job.stdin == None and \
                    job.stdout_file == None and \
                    job.stderr_file == None and \
                    job.parallel_job_info == None
    return is_consistent
           


def explore(root_job, 
            current_serial_job,
            serial_jobs, 
            explored, 
            workflow):

  if root_job in explored:
    return 

  input_dep = []
  output_dep = []
  for dep in workflow.dependencies:
    if dep[0] == root_job:
      output_dep.append(dep[1])
    elif dep[1] == root_job:
      input_dep.append(dep[0])

  group = None
  if root_job in workflow.root_group:
    group = workflow.root_group
  else:
    for gp in workflow.groups:
      if root_job in gp.elements:
        group = gp 
        break

  explored.add(root_job)

  if len(input_dep) == 1:
    if current_serial_job.is_consistent(root_job, group):
      current_serial_job.add_job(root_job, group, input_dep, output_dep)
    else:
      if current_serial_job.is_valid():
        serial_jobs.append(current_serial_job)
      current_serial_job = SerialJob()
      current_serial_job.add_job(root_job, group, input_dep, output_dep)

    if len(output_dep) > 1:
      if current_serial_job.is_valid():
        serial_jobs.append(current_serial_job)
      for job in output_dep:
        current_serial_job = SerialJob()
        explore(job, 
                current_serial_job, 
                serial_jobs, 
                explored, 
                workflow)
    elif len(output_dep) == 0:
      if current_serial_job.is_valid():
        serial_jobs.append(current_serial_job)
    elif len(output_dep) == 1:
      explore(output_dep[0], 
              current_serial_job, 
              serial_jobs, 
              explored, 
              workflow)

  elif len(input_dep) == 0:
    if len(output_dep) == 1:
      current_serial_job = SerialJob()
      current_serial_job.add_job(root_job, group, input_dep, output_dep)
      explore(output_dep[0],
              current_serial_job,
              serial_jobs,
              explored,
              workflow)
    elif len(output_dep) > 1:
      for job in output_dep:
        current_serial_job = SerialJob()
        explore(job, 
                current_serial_job, 
                serial_jobs, 
                explored, 
                workflow)

  elif len(input_dep) > 1:
    if current_serial_job.is_valid():
      serial_jobs.append(current_serial_job)
    current_serial_job = SerialJob()
    if len(output_dep) == 1:
      current_serial_job.add_job(root_job, group, input_dep, output_dep)  
      explore(output_dep[0],
              current_serial_job,
              serial_jobs,
              explored,
              workflow)
      
    elif len(output_dep) > 1:
      for job in output_dep:
        current_serial_job = SerialJob()
        explore(job, 
                current_serial_job, 
                serial_jobs, 
                explored, 
                workflow)