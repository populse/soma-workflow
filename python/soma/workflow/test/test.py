'''
@author: Soizic Laguitton
@organization: U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''

import unittest
import time
import os
import getpass
import sys
from datetime import datetime
from datetime import timedelta

import soma.workflow.constants as constants
from soma.workflow.client import WorkflowController, Job, FileTransfer
import soma.workflow.configuration as configuration

def checkFiles(files, filesModels, tolerance = 0):
  index = 0
  for file in files:
    t = tolerance
    (identical, msg) = identicalFiles(file, filesModels[index])
    if not identical: 
      if t <= 0: 
        return (identical, msg)
      else: 
        t = t -1
        print "\n checkFiles: "+ msg
    index = index +1
  return (True, None)


def identicalFiles(filepath1, filepath2):
  file1 = open(filepath1)
  file2 = open(filepath2)
  lineNb = 1
  line1 = file1.readline()
  line2 = file2.readline()
  identical = line1 == line2
  
  while identical and line1 :
    line1 = file1.readline()
    line2 = file2.readline()
    lineNb = lineNb + 1
    identical = line1 == line2
  
  if identical: identical = line1 == line2
  if not identical:
    return (False, "%s and %s are different. line %d: \n file1: %s file2:%s" %(filepath1, filepath2, lineNb, line1, line2))
  else:
    return (True, None)



class JobExamples(object):
  '''
  Job submission example.
  Each method submits 1 job and return the tuple (job_id, local_ouput_files, std_out_err)
  => pipeline of 4 jobs with file transfer: submitJob1, submitJob2, submitJob3, and 
  submitJob4 methods. 
  => job raising an exception with file transfer: submitExceptionJob
  => local job using user's files only (even for stdout and stderr): localCustomSubmission
  => local job regular submission: localSubmission
  '''
  
  
  
  def __init__(self, wf_ctrl, inpath, outpath, python, transfer_timeout = -24, jobs_timeout = 1):
    self.wf_ctrl = wf_ctrl
    self.inpath = inpath
    self.outpath = outpath
    self.tr_timeout = transfer_timeout
    self.jobs_timeout = jobs_timeout
    self.python = python
    
    self.job1OutputFileModels = [self.inpath + "complete/outputModels/file11",
                                 self.inpath + "complete/outputModels/file12"]
    self.job2OutputFileModels = [self.inpath + "complete/outputModels/file2"]
    self.job3OutputFileModels = [self.inpath + "complete/outputModels/file3"]
    self.job4OutputFileModels = [self.inpath + "complete/outputModels/file4"]
    
    self.job1stdouterrModels = [self.inpath + "complete/outputModels/stdoutjob1",
                                self.inpath + "complete/outputModels/stderrjob1"]
    self.job2stdouterrModels = [self.inpath + "complete/outputModels/stdoutjob2",
                                self.inpath + "complete/outputModels/stderrjob2"]
    self.job3stdouterrModels = [self.inpath + "complete/outputModels/stdoutjob3",
                                self.inpath + "complete/outputModels/stderrjob3"]
    self.job4stdouterrModels = [self.inpath + "complete/outputModels/stdoutjob4",
                                self.inpath + "complete/outputModels/stderrjob4"]
     
    self.exceptionjobstdouterr = [self.inpath + "simple/outputModels/stdout_exception_job",
                                  self.inpath + "simple/outputModels/stderr_exception_job"]
    
  def setNewConnection(self, wf_ctrl):
    '''
    For the disconnection test
    '''
    self.wf_ctrl = wf_ctrl
  
  def submitJob1(self, time=2):
    self.file11_tr = self.wf_ctrl.register_transfer(
                              FileTransfer(is_input=False,         
                                           client_path=self.outpath + "file11", 
                                           disposal_timeout=self.tr_timeout)) 

    self.file12_tr = self.wf_ctrl.register_transfer(
                              FileTransfer(is_input=False,
                                           client_path=self.outpath + "file12",        disposal_timeout=self.tr_timeout))
    
    self.file0_tr = self.wf_ctrl.register_transfer(
                              FileTransfer(True,
                                           self.inpath + "complete/" + "file0", 
                                           self.tr_timeout)) 

    script1_tr = self.wf_ctrl.register_transfer(
                              FileTransfer(True,
                                           self.inpath + "complete/" + "job1.py", 
                                           self.tr_timeout))

    stdin1_tr = self.wf_ctrl.register_transfer(
                              FileTransfer(True, 
                                           self.inpath + "complete/" + "stdin1", 
                                           self.tr_timeout))

    self.wf_ctrl.transfer_files(self.file0_tr.engine_path) 
    self.wf_ctrl.transfer_files(script1_tr.engine_path) 
    self.wf_ctrl.transfer_files(stdin1_tr.engine_path) 
    
    print "files transfered "

    job1_id = self.wf_ctrl.submit_job(Job(
                                      command=[self.python, 
                                              script1_tr, 
                                              self.file0_tr, 
                                              self.file11_tr, 
                                              self.file12_tr, 
                                              repr(time)], 
                                      referenced_input_files=[self.file0_tr, 
                                                              script1_tr, 
                                                              stdin1_tr], 
                                      referenced_output_files=[self.file11_tr, 
                                                              self.file12_tr], 
                                      stdin=stdin1_tr, 
                                      join_stderrout=False, 
                                      disposal_timeout=self.jobs_timeout, 
                                      name="job1 with transfers")) 
                                    
    return (job1_id, [self.file11_tr.engine_path, self.file12_tr.engine_path], None)
  

  def submitJob2(self, time=2):
    self.file2_tr = self.wf_ctrl.register_transfer(
                                  FileTransfer(False,
                                               self.outpath + "file2", 
                                               self.tr_timeout))

    script2_tr = self.wf_ctrl.register_transfer(
                                FileTransfer(True,
                                             self.inpath + "complete/" + "job2.py", 
                                             self.tr_timeout)) 

    stdin2_tr = self.wf_ctrl.register_transfer(
                                FileTransfer(True, 
                                             self.inpath + "complete/" + "stdin1", 
                                             self.tr_timeout))
  
    self.wf_ctrl.transfer_files(script2_tr.engine_path) 
    self.wf_ctrl.transfer_files(stdin2_tr.engine_path) 
  
    job2_id = self.wf_ctrl.submit_job(Job(command=[self.python, 
                                                script2_tr, 
                                                self.file11_tr, 
                                                self.file0_tr, 
                                                self.file2_tr, 
                                                repr(time)], 
                                        referenced_input_files=[self.file0_tr, 
                                                                self.file11_tr, 
                                                                script2_tr, 
                                                                stdin2_tr], 
                                        referenced_output_files=[self.file2_tr], 
                                        stdin=stdin2_tr, 
                                        join_stderrout=False, 
                                        disposal_timeout=self.jobs_timeout, 
                                        name="job2 with transfers"))
    return (job2_id, [self.file2_tr.engine_path], None)


  def submitJob3(self, time=2):
    self.file3_tr = self.wf_ctrl.register_transfer(
                                  FileTransfer(False,
                                               self.outpath + "file3",
                                               self.tr_timeout)) 
    
    script3_tr = self.wf_ctrl.register_transfer(
                                  FileTransfer(True, 
                                               self.inpath + "complete/" + "job3.py", 
                                               self.tr_timeout))
    stdin3_tr = self.wf_ctrl.register_transfer(
                                  FileTransfer(True,
                                               self.inpath + "complete/" + "stdin3", 
                                               self.tr_timeout))
    
    self.wf_ctrl.transfer_files(script3_tr.engine_path) 
    self.wf_ctrl.transfer_files(stdin3_tr.engine_path) 

    job3_id = self.wf_ctrl.submit_job(Job(command=[self.python, 
                                                script3_tr,  
                                                self.file12_tr,  
                                                self.file3_tr,  
                                                repr(time)], 
                                        referenced_input_files=[self.file12_tr, 
                                                                script3_tr,  
                                                                stdin3_tr], 
                                        referenced_output_files=[self.file3_tr], 
                                        stdin=stdin3_tr, 
                                        join_stderrout=False,  
                                        disposal_timeout=self.jobs_timeout, 
                                        name="job3 with transfers")) 
  
    return (job3_id, [self.file3_tr.engine_path], None)
  
  
  def submitJob4(self):
    self.file4_tr = self.wf_ctrl.register_transfer(
                                  FileTransfer(False,
                                               self.outpath + "file4",
                                               self.tr_timeout))
  
    script4_tr = self.wf_ctrl.register_transfer(
                                  FileTransfer(True,
                                               self.inpath + "complete/" + "job4.py", 
                                               self.tr_timeout)) 
    stdin4_tr = self.wf_ctrl.register_transfer(
                                  FileTransfer(True,
                                               self.inpath + "complete/" + "stdin4", 
                                               self.tr_timeout))
  
    self.wf_ctrl.transfer_files(script4_tr.engine_path) 
    self.wf_ctrl.transfer_files(stdin4_tr.engine_path) 
 
    job4_id = self.wf_ctrl.submit_job(Job( 
                                          command=[self.python, 
                                                   script4_tr, 
                                                   self.file2_tr, 
                                                   self.file3_tr, 
                                                   self.file4_tr], 
                                          referenced_input_files=[self.file2_tr, 
                                                                  self.file3_tr, 
                                                                  script4_tr, 
                                                                  stdin4_tr], 
                                          referenced_output_files=[self.file4_tr], 
                                          stdin=stdin4_tr, 
                                          join_stderrout=False, 
                                          disposal_timeout=self.jobs_timeout, 
                                          name="job4 with transfers"))
    return (job4_id, [self.file4_tr.engine_path], None)
  
  
  def submitExceptionJob(self):
    script_tr = self.wf_ctrl.register_transfer(
                                FileTransfer(True,
                                             self.inpath + "simple/exceptionJob.py",
                                             self.tr_timeout))
  
    self.wf_ctrl.transfer_files(script_tr.engine_path)
  
    job_id = self.wf_ctrl.submit_job(Job(command=[self.python, script_tr], 
                                      referenced_input_files=[script_tr], 
                                      referenced_output_files=[], 
                                      stdin=None, 
                                      join_stderrout=False, 
                                      disposal_timeout=self.jobs_timeout, 
                                      name="job with exception"))
    
    return (job_id, None, None)


  def localCustomSubmission(self):
    stdout = self.outpath + "stdout_local_custom_submission"
    stderr = self.outpath + "stderr_local_custom_submission"
    file11 = self.outpath + "file11"
    file12 = self.outpath + "file12"
    job_id = self.wf_ctrl.submit_job(Job(command = [self.python, 
                                                       self.inpath + "complete/" + "job1.py", 
                                                       self.inpath + "complete/" + "file0",
                                                       file11, 
                                                       file12, "2"], 
                                            stdin = self.inpath + "complete/" + "stdin1",
                                            join_stderrout=False,
                                            disposal_timeout = self.jobs_timeout,
                                            name = "job1 local custom submission",
                                            stdout_file=stdout,
                                            stderr_file=stderr,
                                            working_directory=self.outpath ))
      
    return (job_id, 
            [file11, file12],  
            [stdout, stderr])
              
  def localSubmission(self):
    file11 = self.outpath + "file11"
    file12 = self.outpath + "file12"
    job_id = self.wf_ctrl.submit_job(Job(command = [self.python, 
                                                      self.inpath + "complete/" + "job1.py", 
                                                      self.inpath + "complete/" + "file0",
                                                      file11, 
                                                      file12, "2"], 
                                            stdin = self.inpath + "complete/" + "stdin1",
                                            join_stderrout=False, 
                                            disposal_timeout = self.jobs_timeout,
                                            name = "job1 local submission"))
    return (job_id,
            [file11, file12],
            None)
     
  def mpiJobSubmission(self, node_num):
    
    #compilation 
    
    source_tr = self.wf_ctrl.register_transfer(
                                 FileTransfer(True, 
                                              self.inpath + "mpi/simple_mpi.c",
                                              self.tr_timeout))
    
    self.wf_ctrl.transfer_files(source_tr.engine_path)

    object_tr = self.wf_ctrl.register_transfer(
                                 FileTransfer(False,
                                              self.outpath + "simple_mpi.o",
                                              self.tr_timeout))
    #/volatile/laguitton/sge6-2u5/mpich/mpich-1.2.7/bin/
    #/opt/mpich/gnu/bin/
    
    mpibin = self.wf_ctrl.config.get(self.wf_ctrl.resource_id, 
                                     configuration.OCFG_PARALLEL_ENV_MPI_BIN)
    print "mpibin = " + mpibin
    
    print "source_tr.engine_path = " + source_tr.engine_path
    print "object_tr.engine_path = " + object_tr.engine_path
    compil1job_id = self.wf_ctrl.submit_job(Job(command=[ mpibin+"/mpicc", 
                                                             "-c", source_tr, 
                                                             "-o", object_tr ], 
                                             referenced_input_files=[source_tr],
                                             referenced_output_files=[object_tr],
                                             join_stderrout=False, 
                                             disposal_timeout=self.jobs_timeout,
                                             name="job compil1 mpi"))
    
    self.wf_ctrl.wait_job([compil1job_id])
    
    bin_tr = self.wf_ctrl.register_transfer(
                              FileTransfer(True,
                                           self.outpath + "simple_mpi",
                                           self.tr_timeout))
    print "bin_tr.engine_path= " + bin_tr.engine_path
    
    compil2job_id = self.wf_ctrl.submit_job(Job(command=[ mpibin+"/mpicc", 
                                                              "-o", bin_tr, 
                                                              object_tr ], 
                                             referenced_input_files=[object_tr],
                                             referenced_output_files=[bin_tr],
                                             join_stderrout=False, 
                                             disposal_timeout=self.jobs_timeout,
                                             name="job compil2 mpi"))
    
    self.wf_ctrl.wait_job([compil2job_id])
    self.wf_ctrl.delete_transfer(object_tr.engine_path)

    
    # mpi job submission
    script = self.wf_ctrl.register_transfer(
                              FileTransfer(True,
                                           self.inpath + "mpi/simple_mpi.sh",
                                           self.tr_timeout))
    
    self.wf_ctrl.transfer_files(script.engine_path)

    job_id = self.wf_ctrl.submit_job(Job(command=[ script, repr(node_num), bin_tr], 
                                      referenced_input_files=[script, bin_tr],
                                      join_stderrout=False, 
                                      disposal_timeout=self.jobs_timeout,
                                      name="parallel job mpi",
                                      parallel_job_info= (configuration.OCFG_PARALLEL_PC_MPI,node_num)))

    self.wf_ctrl.delete_job(compil1job_id)
    self.wf_ctrl.delete_job(compil2job_id)
    

    return (job_id, [source_tr.engine_path], None)
          
     
class JobsTest(unittest.TestCase):
  '''
  Abstract class for soma_workflow job and transfer common tests.
  '''
  @staticmethod
  def setupConnection(resource_id, 
                      login, 
                      password, 
                      job_examples_dir, 
                      output_dir):
    
    JobsTest.login = login
    JobsTest.password = password
    JobsTest.resource_id = resource_id
  
    JobsTest.wf_ctrl = WorkflowController(resource_id, 
                                          login, 
                                          password)
    
    JobsTest.transfer_timeout = -24 
    JobsTest.jobs_timeout = 1
  
    JobsTest.jobExamples = JobExamples(JobsTest.wf_ctrl, 
                              job_examples_dir, 
                              output_dir, 
                              'python',
                              JobsTest.transfer_timeout, 
                              JobsTest.jobs_timeout)   
  
    JobsTest.outpath = output_dir
                                   
  def setUp(self):
    raise Exception('JobTest is an abstract class. SetUp must be implemented in subclass')
                                   
   
  def tearDown(self):
    for jid in self.myJobs:
      JobsTest.wf_ctrl.delete_job(jid)
    remainingJobs = frozenset(JobsTest.wf_ctrl.jobs().keys())
    self.failUnless(len(remainingJobs.intersection(self.myJobs)) == 0)
     
                                   
  def test_jobs(self):
    res = set(JobsTest.wf_ctrl.jobs().keys())
    self.failUnless(res.issuperset(self.myJobs))
     
  def test_wait(self):
    JobsTest.wf_ctrl.wait_job(self.myJobs)
    for jid in self.myJobs:
      status = JobsTest.wf_ctrl.job_status(jid)
      self.failUnless(status == constants.DONE or 
                      status == constants.FAILED,
                      'Job %s status after wait: %s' %(jid, status))
                      
  def test_wait2(self):
    startTime = datetime.now()
    interval = 5
    JobsTest.wf_ctrl.wait_job(self.myJobs, interval)
    delta = datetime.now() - startTime
    if delta < timedelta(seconds=interval):
      for jid in self.myJobs:
        status = JobsTest.wf_ctrl.job_status(jid)
        self.failUnless(status == constants.DONE or 
                        status == constants.FAILED,
                        'Job %s status after wait: %s' %(self.myJobs[0], status))
    else:
      self.failUnless( abs(delta-timedelta(seconds=interval)) < timedelta(seconds=1))
    
  def test_restart(self):
    jobid = self.myJobs[len(self.myJobs)-1]
    JobsTest.wf_ctrl.kill_job(jobid)
    JobsTest.wf_ctrl.restart_job(jobid)
    status = JobsTest.wf_ctrl.job_status(jobid)
    self.failUnless(not status == constants.USER_ON_HOLD and  
                    not status == constants.USER_SYSTEM_ON_HOLD and
                    not status == constants.USER_SUSPENDED and
                    not status == constants.USER_SYSTEM_SUSPENDED,
                    'Job status after restart: %s' %status)
   
  def test_kill(self):
    jobid = self.myJobs[0]
    time.sleep(2)
    JobsTest.wf_ctrl.kill_job(jobid)
    job_termination_status = JobsTest.wf_ctrl.job_termination_status(jobid)
    exitStatus = job_termination_status[0]
    status = JobsTest.wf_ctrl.job_status(jobid)
    self.failUnless(status == constants.FAILED or status == constants.DONE, 
                    'Job status after kill: %s' %status)
    self.failUnless(exitStatus == constants.USER_KILLED or exitStatus == constants.FINISHED_REGULARLY, 
                    'Job exit status after kill: %s' %exitStatus)
                    
  def test_result(self):
    raise Exception('JobTest is an abstract class. test_result must be implemented in subclass')
 
 
 
 
class LocalCustomSubmission(JobsTest):
  '''
  Submission of a job using user's files only (even for stdout and stderr)
  '''
  def setUp(self):
    self.myJobs = []
    self.myTransfers = []
    info = JobsTest.jobExamples.localCustomSubmission()
    self.myJobs.append(info[0]) 
    self.outputFiles = info[1]
    self.stdouterrFiles = info[2]
   
  def tearDown(self):
    JobsTest.tearDown(self)
    for file in self.outputFiles:
      if os.path.isfile(file): os.remove(file)
    for file in self.stdouterrFiles:
      if os.path.isfile(file): os.remove(file)
  
  def test_result(self):
    jobid = self.myJobs[0]
    JobsTest.wf_ctrl.wait_job(self.myJobs)
    status = JobsTest.wf_ctrl.job_status(jobid)

    self.failUnless(status == constants.DONE,
                    'Job %s status after wait: %s' %(jobid, status))
    job_termination_status = JobsTest.wf_ctrl.job_termination_status(jobid)
    exitStatus = job_termination_status[0]
    self.failUnless(exitStatus == constants.FINISHED_REGULARLY, 
                    'Job %s exit status: %s' %(jobid, exitStatus))
    exitValue = job_termination_status[1]
    self.failUnless(exitValue == 0,
                    'Job %s exit value: %d' %(jobid,exitValue))
      
    # checking output files
    for file in self.outputFiles:
      self.failUnless(os.path.isfile(file), 'File %s doesn t exit' %file)
      
    (correct, msg) = checkFiles(self.outputFiles, JobsTest.jobExamples.job1OutputFileModels)
    self.failUnless(correct, msg)
      
    # checking stderr and stdout files
    for file in self.stdouterrFiles:
      self.failUnless(os.path.isfile(file), 'File %s doesn t exit' %file)
    (correct, msg) = checkFiles(self.stdouterrFiles, JobsTest.jobExamples.job1stdouterrModels)
    self.failUnless(correct, msg)
 

class LocalSubmission(JobsTest):
  '''
  Submission of a job without transfer
  '''
  def setUp(self):
    self.myJobs = []
    self.myTransfers = []
    info = JobsTest.jobExamples.localSubmission()
    self.myJobs.append(info[0]) 
    self.outputFiles = info[1]
   
  def tearDown(self):
    JobsTest.tearDown(self)
    for file in self.outputFiles:
      if os.path.isfile(file): os.remove(file)
      
  def test_result(self):
    jobid = self.myJobs[0]
    JobsTest.wf_ctrl.wait_job(self.myJobs)
    status = JobsTest.wf_ctrl.job_status(jobid)
    self.failUnless(status == constants.DONE,
                    'Job %s status after wait: %s' %(jobid, status))
    job_termination_status = JobsTest.wf_ctrl.job_termination_status(jobid)
    exitStatus = job_termination_status[0]
    self.failUnless(exitStatus == constants.FINISHED_REGULARLY, 
                    'Job %s exit status: %s' %(jobid, exitStatus))
    exitValue = job_termination_status[1]
    self.failUnless(exitValue == 0,
                    'Job exit value: %d' %exitValue)
                    
                    
    # checking output files
    for file in self.outputFiles:
      self.failUnless(os.path.isfile(file), 'File %s doesn t exit' %file)
    
    (correct, msg) = checkFiles(self.outputFiles, JobsTest.jobExamples.job1OutputFileModels)
    self.failUnless(correct, msg)
    
    # checking stdout and stderr
    stdout = JobsTest.outpath + "/stdout_local_submission"
    stderr = JobsTest.outpath + "/stderr_local_submission"
    JobsTest.wf_ctrl.retrieve_job_stdouterr(self.myJobs[0], stdout, stderr)
    self.outputFiles.append(stdout)
    self.outputFiles.append(stderr)
    
    (correct, msg) = checkFiles(self.outputFiles[2:5], JobsTest.jobExamples.job1stdouterrModels)
    self.failUnless(correct, msg)
      
class SubmissionWithTransfer(JobsTest):
  '''
  Submission of a job with transfer
  '''
  def setUp(self):
    self.myJobs = []
    self.myTransfers = []
    info = JobsTest.jobExamples.submitJob1()
    self.myJobs.append(info[0]) 
    self.outputFiles = info[1]
    self.clientFiles = []
   
  def tearDown(self):
    JobsTest.tearDown(self)
    for file in self.clientFiles:
      if os.path.isfile(file): os.remove(file)
      
  def test_result(self):
    jobid = self.myJobs[0]
    JobsTest.wf_ctrl.wait_job(self.myJobs)
    status = JobsTest.wf_ctrl.job_status(jobid)
    self.failUnless(status == constants.DONE,
                    'Job %s status after wait: %s' %(jobid, status))
    job_termination_status = JobsTest.wf_ctrl.job_termination_status(jobid)
    exitStatus = job_termination_status[0]
    self.failUnless(exitStatus == constants.FINISHED_REGULARLY, 
                    'Job %s exit status: %s' %(jobid, exitStatus))
    exitValue = job_termination_status[1]
    self.failUnless(exitValue == 0,
                    'Job exit value: %d' %exitValue)
    
    # checking output files
    for file in self.outputFiles:
      client_file = JobsTest.wf_ctrl.transfers([file])[file][0]
      self.failUnless(client_file)
      JobsTest.wf_ctrl.transfer_files(file)
      self.failUnless(os.path.isfile(client_file), 'File %s doesn t exit' %file)
      self.clientFiles.append(client_file)
   
    (correct, msg) = checkFiles(self.clientFiles, JobsTest.jobExamples.job1OutputFileModels)
    self.failUnless(correct, msg)
    
    # checking stdout and stderr
    client_stdout = JobsTest.outpath + "/stdout_submit_with_transfer"
    client_stderr = JobsTest.outpath + "/stderr_submit_with_transfer"
    JobsTest.wf_ctrl.retrieve_job_stdouterr(self.myJobs[0], client_stdout, client_stderr)
    self.clientFiles.append(client_stdout)
    self.clientFiles.append(client_stderr)
  
    (correct, msg) = checkFiles(self.clientFiles[2:5], JobsTest.jobExamples.job1stdouterrModels)
    self.failUnless(correct, msg)
    
    


class EndedJobWithTransfer(JobsTest):
  '''
  Submission of a job with transfer
  '''
  def setUp(self):
    self.myJobs = []
    self.myTransfers = []
    info = JobsTest.jobExamples.submitJob1()
    self.myJobs.append(info[0]) 
    self.outputFiles = info[1]
    self.clientFiles = []

    JobsTest.wf_ctrl.wait_job(self.myJobs)
   
  def tearDown(self):
    JobsTest.tearDown(self)
  
  def test_result(self):
    self.failUnless(True)

    
class JobPipelineWithTransfer(JobsTest):
  '''
  Submission of a job pipeline with transfer
  '''
  def setUp(self):
    self.myJobs = []
    self.myTransfers = []
    self.clientFiles = []
    self.outputFiles = []
    
    # Job1 
    
    info1 = JobsTest.jobExamples.submitJob1()
    self.myJobs.append(info1[0]) 
    self.outputFiles.extend(info1[1])

    JobsTest.wf_ctrl.wait_job(self.myJobs)
    status = JobsTest.wf_ctrl.job_status(self.myJobs[0])
    self.failUnless(status == constants.DONE,
                    'Job %s status after wait: %s' %(self.myJobs[0], status))
    job_termination_status = JobsTest.wf_ctrl.job_termination_status(self.myJobs[0])
    exitStatus = job_termination_status[0]
    self.failUnless(exitStatus == constants.FINISHED_REGULARLY, 
                    'Job %s exit status: %s' %(self.myJobs[0], exitStatus))
    exitValue = job_termination_status[1]
    self.failUnless(exitValue == 0,
                    'Job exit value: %d' %exitValue)
                    
    # Job2 & 3
    
    info2 = JobsTest.jobExamples.submitJob2()
    self.myJobs.append(info2[0]) 
    self.outputFiles.extend(info2[1])
    
    info3 = JobsTest.jobExamples.submitJob3()
    self.myJobs.append(info3[0]) 
    self.outputFiles.extend(info3[1])

    JobsTest.wf_ctrl.wait_job(self.myJobs)
    status = JobsTest.wf_ctrl.job_status(self.myJobs[1])
    self.failUnless(status == constants.DONE,
                    'Job %s status after wait: %s' %(self.myJobs[1], status))
    job_termination_status = JobsTest.wf_ctrl.job_termination_status(self.myJobs[1])
    exitStatus = job_termination_status[0]
    self.failUnless(exitStatus == constants.FINISHED_REGULARLY, 
                    'Job %s exit status: %s' %(self.myJobs[1], exitStatus))
    exitValue = job_termination_status[1]
    self.failUnless(exitValue == 0,
                    'Job exit value: %d' %exitValue)
                    
    status = JobsTest.wf_ctrl.job_status(self.myJobs[2])
    self.failUnless(status == constants.DONE,
                    'Job %s status after wait: %s' %(self.myJobs[2], status))
    job_termination_status = JobsTest.wf_ctrl.job_termination_status(self.myJobs[2])
    exitStatus = job_termination_status[0]
    self.failUnless(exitStatus == constants.FINISHED_REGULARLY, 
                    'Job %s exit status: %s' %(self.myJobs[2], exitStatus))
    exitValue = job_termination_status[1]
    self.failUnless(exitValue == 0,
                    'Job exit value: %d' %exitValue)
    
    # Job 4
    
    info4 = JobsTest.jobExamples.submitJob4()
    self.myJobs.append(info4[0]) 
    self.outputFiles.extend(info4[1])

                    
                    
   
  def tearDown(self):
    JobsTest.tearDown(self)
    for file in self.clientFiles:
      if os.path.isfile(file): os.remove(file)
      
  def test_result(self):
    jobid = self.myJobs[len(self.myJobs)-1]
    JobsTest.wf_ctrl.wait_job(self.myJobs)
    status = JobsTest.wf_ctrl.job_status(jobid)
    self.failUnless(status == constants.DONE,
                    'Job %s status after wait: %s' %(jobid, status))
    job_termination_status = JobsTest.wf_ctrl.job_termination_status(jobid)
    exitStatus = job_termination_status[0]
    self.failUnless(exitStatus == constants.FINISHED_REGULARLY, 
                    'Job %s exit status: %s' %(jobid, exitStatus))
    exitValue = job_termination_status[1]
    self.failUnless(exitValue == 0,
                    'Job exit value: %d' %exitValue)
    
    
    # checking output files
    for file in self.outputFiles:
      client_file = JobsTest.wf_ctrl.transfers([file])[file][0]
      self.failUnless(client_file)
      JobsTest.wf_ctrl.transfer_files(file)
      self.failUnless(os.path.isfile(client_file), 'File %s doesn t exit' %file)
      self.clientFiles.append(client_file)
    
    models = JobsTest.jobExamples.job1OutputFileModels + JobsTest.jobExamples.job2OutputFileModels + JobsTest.jobExamples.job3OutputFileModels + JobsTest.jobExamples.job4OutputFileModels
    (correct, msg) = checkFiles(self.clientFiles, models)
    self.failUnless(correct, msg)
    

    # checking stdout and stderr
    client_stdout = JobsTest.outpath + "/stdout_pipeline_job1"
    client_stderr = JobsTest.outpath + "/stderr_pipeline_job1"
    JobsTest.wf_ctrl.retrieve_job_stdouterr(self.myJobs[0], client_stdout, client_stderr)
    self.clientFiles.append(client_stdout)
    self.clientFiles.append(client_stderr)
    
    client_stdout = JobsTest.outpath + "/stdout_pipeline_job2"
    client_stderr = JobsTest.outpath + "/stderr_pipeline_job2"
    JobsTest.wf_ctrl.retrieve_job_stdouterr(self.myJobs[1], client_stdout, client_stderr)
    self.clientFiles.append(client_stdout)
    self.clientFiles.append(client_stderr)
  
    client_stdout = JobsTest.outpath + "/stdout_pipeline_job3"
    client_stderr = JobsTest.outpath + "/stderr_pipeline_job3"
    JobsTest.wf_ctrl.retrieve_job_stdouterr(self.myJobs[2], client_stdout, client_stderr)
    self.clientFiles.append(client_stdout)
    self.clientFiles.append(client_stderr)
    
    client_stdout = JobsTest.outpath + "/stdout_pipeline_job4"
    client_stderr = JobsTest.outpath + "/stderr_pipeline_job4"
    JobsTest.wf_ctrl.retrieve_job_stdouterr(self.myJobs[3], client_stdout, client_stderr)
    self.clientFiles.append(client_stdout)
    self.clientFiles.append(client_stderr)
   
    models = JobsTest.jobExamples.job1stdouterrModels + JobsTest.jobExamples.job2stdouterrModels + JobsTest.jobExamples.job3stdouterrModels + JobsTest.jobExamples.job4stdouterrModels
    (correct, msg) = checkFiles(self.clientFiles[5:13], models)
    self.failUnless(correct, msg)

                    
class ExceptionJobTest(JobsTest):
  '''
  Submission of a job raising an exception
  '''
  def setUp(self):
    self.myJobs = []
    self.myTransfers = []
    info = JobsTest.jobExamples.submitExceptionJob()
    self.myJobs.append(info[0])
    self.clientFiles = []
    
  def tearDown(self):
    JobsTest.tearDown(self)
    for file in self.clientFiles:
      if os.path.isfile(file): os.remove(file)
   
  def test_result(self):
    jobid = self.myJobs[0]
    JobsTest.wf_ctrl.wait_job(self.myJobs)
    status = JobsTest.wf_ctrl.job_status(jobid)
    self.failUnless(status == constants.DONE or constants.FAILED,
                    'Job %s status after wait: %s' %(jobid, status))
    job_termination_status = JobsTest.wf_ctrl.job_termination_status(jobid)
    exitStatus = job_termination_status[0]
    self.failUnless(exitStatus == constants.FINISHED_REGULARLY, 
                    'Job %s exit status: %s' %(jobid, exitStatus))
    exitValue = job_termination_status[1]
    self.failUnless(exitValue == 1,
                    'Job exit value: %d' %exitValue)
    # checking stdout and stderr
    client_stdout = JobsTest.outpath + "/stdout_exception_job"
    client_stderr = JobsTest.outpath + "/stderr_exception_job"
    JobsTest.wf_ctrl.retrieve_job_stdouterr(jobid, client_stdout, client_stderr)
    self.clientFiles.append(client_stdout)
    self.clientFiles.append(client_stderr)
    
    (identical, msg) = checkFiles(self.clientFiles, JobsTest.jobExamples.exceptionjobstdouterr,1)
    self.failUnless(identical, msg)

    
class DisconnectionTest(JobsTest):
  '''
  Submission of a job pipeline with transfer
  '''
  def setUp(self):
    self.myJobs = []
    self.myTransfers = []
    self.clientFiles = []
    self.outputFiles = []
    
    # Job1 
    
    info1 = JobsTest.jobExamples.submitJob1()
    self.myJobs.append(info1[0]) 
    self.outputFiles.extend(info1[1])

    JobsTest.wf_ctrl.wait_job(self.myJobs)
    status = JobsTest.wf_ctrl.job_status(self.myJobs[0])
    self.failUnless(status == constants.DONE,
                    'Job %s status after wait: %s' %(self.myJobs[0], status))
    job_termination_status = JobsTest.wf_ctrl.job_termination_status(self.myJobs[0])
    exitStatus = job_termination_status[0]
    self.failUnless(exitStatus == constants.FINISHED_REGULARLY, 
                    'Job %s exit status: %s' %(self.myJobs[0], exitStatus))
    exitValue = job_termination_status[1]
    self.failUnless(exitValue == 0,
                    'Job exit value: %d' %exitValue)
                    
    # Job2 & 3
    
    info2 = JobsTest.jobExamples.submitJob2(time = 60)
    self.myJobs.append(info2[0]) 
    self.outputFiles.extend(info2[1])
    
    info3 = JobsTest.jobExamples.submitJob3(time = 30)
    self.myJobs.append(info3[0]) 
    self.outputFiles.extend(info3[1])

    time.sleep(10)
    print "Disconnection...."
    JobsTest.wf_ctrl.disconnect()
    del JobsTest.wf_ctrl
    time.sleep(20)
    print ".... Reconnection"

    JobsTest.wf_ctrl = WorkflowController(JobsTest.resource_id, 
                                       JobsTest.login, 
                                       JobsTest.password)


    JobsTest.jobExamples.setNewConnection(JobsTest.wf_ctrl)
    #time.sleep(1)
   
  def tearDown(self):
    #pass
    JobsTest.tearDown(self)
    for file in self.clientFiles:
      if os.path.isfile(file): 
        print "remove " + file 
        os.remove(file)
      
  def test_result(self):

    JobsTest.wf_ctrl.wait_job(self.myJobs)
    status = JobsTest.wf_ctrl.job_status(self.myJobs[1])
    self.failUnless(status == constants.DONE,
                    'Job %s status after wait: %s' %(self.myJobs[1], status))
    job_termination_status = JobsTest.wf_ctrl.job_termination_status(self.myJobs[1])
    exitStatus = job_termination_status[0]
    self.failUnless(exitStatus == constants.FINISHED_REGULARLY, 
                    'Job %s exit status: %s' %(self.myJobs[1], exitStatus))
    exitValue = job_termination_status[1]
    self.failUnless(exitValue == 0,
                    'Job exit value: %d' %exitValue)
                    
    status = JobsTest.wf_ctrl.job_status(self.myJobs[2])
    self.failUnless(status == constants.DONE,
                    'Job %s status after wait: %s' %(self.myJobs[2], status))
    job_termination_status = JobsTest.wf_ctrl.job_termination_status(self.myJobs[2])
    exitStatus = job_termination_status[0]
    self.failUnless(exitStatus == constants.FINISHED_REGULARLY, 
                    'Job %s exit status: %s' %(self.myJobs[2], exitStatus))
    exitValue = job_termination_status[1]
    self.failUnless(exitValue == 0,
                    'Job exit value: %d' %exitValue)
    
    # Job 4
    
    info4 = JobsTest.jobExamples.submitJob4()
    self.failUnless(not info4[0] == -1, "The job was not submitted.")
    self.myJobs.append(info4[0]) 
    self.outputFiles.extend(info4[1])

    jobid = self.myJobs[len(self.myJobs)-1]
    JobsTest.wf_ctrl.wait_job(self.myJobs)
    status = JobsTest.wf_ctrl.job_status(jobid)
    self.failUnless(status == constants.DONE,
                    'Job %s status after wait: %s' %(jobid, status))
    job_termination_status = JobsTest.wf_ctrl.job_termination_status(jobid)
    exitStatus = job_termination_status[0]
    self.failUnless(exitStatus == constants.FINISHED_REGULARLY, 
                    'Job %s exit status: %s' %(jobid, exitStatus))
    exitValue = job_termination_status[1]
    self.failUnless(exitValue == 0,
                    'Job exit value: %d' %exitValue)
    
    
    # checking output files
    for file in self.outputFiles:
      client_file = JobsTest.wf_ctrl.transfers([file])[file][0]
      self.failUnless(client_file)
      JobsTest.wf_ctrl.transfer_files(file)
      self.failUnless(os.path.isfile(client_file), 'File %s doesn t exit' %file)
      self.clientFiles.append(client_file)
    
    models = JobsTest.jobExamples.job1OutputFileModels + JobsTest.jobExamples.job2OutputFileModels + JobsTest.jobExamples.job3OutputFileModels + JobsTest.jobExamples.job4OutputFileModels
    (correct, msg) = checkFiles(self.clientFiles, models)
    self.failUnless(correct, msg)
    

    # checking stdout and stderr
    client_stdout = JobsTest.outpath + "/stdout_pipeline_job1"
    client_stderr = JobsTest.outpath + "/stderr_pipeline_job1"
    JobsTest.wf_ctrl.retrieve_job_stdouterr(self.myJobs[0], client_stdout, client_stderr)
    self.clientFiles.append(client_stdout)
    self.clientFiles.append(client_stderr)
    
    client_stdout = JobsTest.outpath + "/stdout_pipeline_job2"
    client_stderr = JobsTest.outpath + "/stderr_pipeline_job2"
    JobsTest.wf_ctrl.retrieve_job_stdouterr(self.myJobs[1], client_stdout, client_stderr)
    self.clientFiles.append(client_stdout)
    self.clientFiles.append(client_stderr)
  
    client_stdout = JobsTest.outpath + "/stdout_pipeline_job3"
    client_stderr = JobsTest.outpath + "/stderr_pipeline_job3"
    JobsTest.wf_ctrl.retrieve_job_stdouterr(self.myJobs[2], client_stdout, client_stderr)
    self.clientFiles.append(client_stdout)
    self.clientFiles.append(client_stderr)
    
    client_stdout = JobsTest.outpath + "/stdout_pipeline_job4"
    client_stderr = JobsTest.outpath + "/stderr_pipeline_job4"
    JobsTest.wf_ctrl.retrieve_job_stdouterr(self.myJobs[3], client_stdout, client_stderr)
    self.clientFiles.append(client_stdout)
    self.clientFiles.append(client_stderr)
   
    models = JobsTest.jobExamples.job1stdouterrModels + JobsTest.jobExamples.job2stdouterrModels + JobsTest.jobExamples.job3stdouterrModels + JobsTest.jobExamples.job4stdouterrModels
    (correct, msg) = checkFiles(self.clientFiles[5:13], models,1)
    self.failUnless(correct, msg)
    #pass



class MPIParallelJobTest(JobsTest):
  '''
  Submission of a parallel job (MPI)
  '''
  def setUp(self):
    self.myJobs = []
    self.myTransfers = []
    self.node_num = 4
    info = JobsTest.jobExamples.mpiJobSubmission(node_num = self.node_num)
    self.myJobs.append(info[0]) 
    self.outputFiles = info[1]
   
  def tearDown(self):
    JobsTest.tearDown(self)
    #for file in self.outputFiles:
      #if os.path.isfile(file): os.remove(file)
      
  def test_result(self):
    jobid = self.myJobs[0]
    JobsTest.wf_ctrl.wait_job(self.myJobs)
    
    status = JobsTest.wf_ctrl.job_status(jobid)
    self.failUnless(status == constants.DONE,
                    'Job %s status after wait: %s' %(jobid, status))
    job_termination_status = JobsTest.wf_ctrl.job_termination_status(jobid)
    exitStatus = job_termination_status[0]
    self.failUnless(exitStatus == constants.FINISHED_REGULARLY, 
                    'Job %s exit status: %s' %(jobid, exitStatus))
    exitValue = job_termination_status[1]
    self.failUnless(exitValue == 0,
                    'Job exit value: %d' %exitValue)
                    

    print "stdout: "
    line = JobsTest.wf_ctrl.stdoutReadLine(jobid)
    process_num = 1
    while line:
      splitted_line = line.split()
      if splitted_line[0] == "Grettings":
        self.failUnless(line.rstrip() == "Grettings from process %d!" %(process_num), 
                        "stdout line:  %sinstead of  : 'Grettings from process %d!'" %(line, process_num))
        process_num = process_num +1
      line = JobsTest.wf_ctrl.stdoutReadLine(jobid)

    self.failUnless(process_num==self.node_num, 
                    "%d process(es) run instead of %d." %(process_num-1, self.node_num))

      



if __name__ == '__main__':
  
  job_examples_dir = os.environ.get("SOMA_WORKFLOW_EXAMPLES")
  output_dir = os.environ.get("SOMA_WORKFLOW_EXAMPLES_OUT")
  if not job_examples_dir or not output_dir:
    raise RuntimeError( 'The environment variables SOMA_WORKFLOW_EXAMPLES and SOMA_WORKFLOW_EXAMPLES_OUT must be set.')
     
  sys.stdout.write("----- soma-workflow tests: JOBS -------------\n")

  config_file_path = configuration.Configuration.search_config_path()
  sys.stdout.write("Configuration file: " + config_file_path)
  resource_ids = configuration.Configuration.get_configured_resources(config_file_path)
  
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
  
  config = configuration.Configuration.load_from_file(resource_id, config_file_path)

  if config.get_mode() == 'remote':
    sys.stdout.write("This is a remote connection\n")
    sys.stdout.write("login:")
    login = sys.stdin.readline()
    login = login.rstrip()
    password = getpass.getpass()
  sys.stdout.write("Login => " + repr(login) + "\n")
  sys.stdout.write("---------------------------------\n")
  
  # Job type
  job_types = ["LocalCustomSubmission", "LocalSubmission", "SubmissionWithTransfer", "ExceptionJobTest", "JobPipelineWithTransfer", "DisconnectionTest", "EndedJobWithTransfer", "MPIParallelJobTest"]
  sys.stdout.write("Jobs example to test: \n")
  sys.stdout.write("all -> all \n")
  for i in range(0, len(job_types)):
    sys.stdout.write("  " + repr(i) + " -> " + repr(job_types[i]) + "\n")
  sys.stdout.write("Select one or several job type : \n")
  selected_job_type_indexes = []
  line = sys.stdin.readline()
  line = line.rstrip()
  if line == "all":
    selected_job_type = job_types
    sys.stdout.write("Selected job types: all \n")
  else:
    for strindex in line.split(" "):
      selected_job_type_indexes.append(int(strindex))
    selected_job_type = []
    sys.stdout.write("Selected job types: \n" )
    for job_type_index in selected_job_type_indexes:
      selected_job_type.append(job_types[int(job_type_index)])
      sys.stdout.write("  => " + repr(job_types[int(job_type_index)])  + "\n")

  # Test type
  sys.stdout.write("---------------------------------\n")
  test_types = ["test_result", "test_jobs", "test_wait", "test_wait2", "test_kill", "test_restart"]
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

  JobsTest.setupConnection(resource_id, 
                           login, 
                           password, 
                           job_examples_dir, 
                           output_dir)
  suite_list =  []
  tests = selected_test_type
  if "LocalCustomSubmission" in selected_job_type:
    suite_list.append(unittest.TestSuite(map(LocalCustomSubmission, tests)))
  if "LocalSubmission" in selected_job_type:
    print "tests = " + repr(tests)
    suite_list.append(unittest.TestSuite(map(LocalSubmission, tests)))
  if "SubmissionWithTransfer" in selected_job_type:
    suite_list.append(unittest.TestSuite(map(SubmissionWithTransfer, tests)))
  if "ExceptionJobTest" in selected_job_type:
    suite_list.append(unittest.TestSuite(map(ExceptionJobTest, tests)))
  if "JobPipelineWithTransfer" in selected_job_type:
    suite_list.append(unittest.TestSuite(map(JobPipelineWithTransfer, tests)))
  if "DisconnectionTest" in selected_job_type:
    suite_list.append(unittest.TestSuite(map(DisconnectionTest, tests)))
  if "EndedJobWithTransfer" in selected_job_type:
    suite_list.append(unittest.TestSuite(map(EndedJobWithTransfer, tests)))
  if "MPIParallelJobTest" in selected_job_type:
    suite_list.append(unittest.TestSuite(map(MPIParallelJobTest, tests)))

  alltests = unittest.TestSuite(suite_list)
  unittest.TextTestRunner(verbosity=2).run(alltests)
  
  sys.exit(0)
