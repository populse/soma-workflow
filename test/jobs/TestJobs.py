import unittest
import soma.jobs.jobClient
import soma.jobs.constants as constants
import time
import os
import getpass
import sys
import ConfigParser
import socket
from datetime import datetime
from datetime import timedelta




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
  Each method submit 1 job and return the tuple (job_id, local_ouput_files, std_out_err)
  => pipeline of 4 jobs with file transfer: submitJob1, submitJob2, submitJob3, and 
  submitJob4 methods. 
  => job raising an exception with file transfer: submitExceptionJob
  => local job using user's files only (even for stdout and stderr): localCustomSubmission
  => local job regular submission: localSubmission
  '''
  
  
  
  def __init__(self, jobs, inpath, outpath, python, transfer_timeout = -24, jobs_timeout = 1):
    self.jobs = jobs
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
    
  def setNewConnection(self, jobs):
    '''
    For the disconnection test
    '''
    self.jobs = jobs
  
  def submitJob1(self, time=2):
    self.l_file11 = self.jobs.registerFileTransfer(self.outpath + "file11", self.tr_timeout) 
    self.l_file12 = self.jobs.registerFileTransfer(self.outpath + "file12", self.tr_timeout) 
    
    self.l_file0 = self.jobs.sendFile(self.inpath + "complete/" + "file0", self.tr_timeout) 
    l_script1 = self.jobs.sendFile(self.inpath + "complete/" + "job1.py", self.tr_timeout) 
    l_stdin1 = self.jobs.sendFile(self.inpath + "complete/" + "stdin1", self.tr_timeout) 
    
    job1id = self.jobs.submit( [self.python, l_script1, self.l_file0, self.l_file11, self.l_file12, repr(time)], 
                               [self.l_file0, l_script1, l_stdin1], 
                               [self.l_file11, self.l_file12], 
                               l_stdin1, False, self.jobs_timeout, "job1 with transfers") 
                                    
    return (job1id, [self.l_file11, self.l_file12], None)
  

  def submitJob2(self, time=2):
    self.l_file2 = self.jobs.registerFileTransfer(self.outpath + "file2", self.tr_timeout) 

    l_script2 = self.jobs.sendFile(self.inpath + "complete/" + "job2.py", self.tr_timeout) 
    l_stdin2 = self.jobs.sendFile(self.inpath + "complete/" + "stdin1", self.tr_timeout) 
  
    job2id = self.jobs.submit( [self.python, l_script2, self.l_file11, self.l_file0, self.l_file2, repr(time)], 
                               [self.l_file0, self.l_file11, l_script2, l_stdin2], 
                               [self.l_file2], 
                               l_stdin2, False, self.jobs_timeout, "job2 with transfers") 
    return (job2id, [self.l_file2], None)


  def submitJob3(self, time=2):
    self.l_file3 = self.jobs.registerFileTransfer(self.outpath + "file3", self.tr_timeout) 
    
    l_script3 = self.jobs.sendFile(self.inpath + "complete/" + "job3.py", self.tr_timeout) 
    l_stdin3 = self.jobs.sendFile(self.inpath + "complete/" + "stdin3", self.tr_timeout) 
    
    job3id = self.jobs.submit( [self.python, l_script3, self.l_file12, self.l_file3, repr(time)], 
                               [self.l_file12, l_script3, l_stdin3], 
                               [self.l_file3], 
                               l_stdin3, False,  self.jobs_timeout, "job3 with transfers") 
  
    return (job3id, [self.l_file3], None)
  
  
  def submitJob4(self):
    self.l_file4 = self.jobs.registerFileTransfer(self.outpath + "file4", self.tr_timeout) 
  
    l_script4 = self.jobs.sendFile(self.inpath + "complete/" + "job4.py", self.tr_timeout) 
    l_stdin4 = self.jobs.sendFile(self.inpath + "complete/" + "stdin4", self.tr_timeout) 
  
    job4id = self.jobs.submit( [self.python, l_script4, self.l_file2, self.l_file3, self.l_file4], 
                               [self.l_file2, self.l_file3, l_script4, l_stdin4], 
                               [self.l_file4], 
                               l_stdin4, False, self.jobs_timeout, "job4 with transfers") 
    return (job4id, [self.l_file4], None)
  
  
  def submitExceptionJob(self):
    l_script = self.jobs.sendFile(self.inpath + "simple/exceptionJob.py", self.tr_timeout)
    
    jobid = self.jobs.submit( [self.python, l_script], 
                              [l_script], 
                              [], 
                              None, False, self.jobs_timeout, "job with exception") 
    
    return (jobid, None, None)


  def localCustomSubmission(self):
    stdout = self.outpath + "stdout_local_custom_submission"
    stderr = self.outpath + "stderr_local_custom_submission"
    file11 = self.outpath + "file11"
    file12 = self.outpath + "file12"
    jobId = self.jobs.submit( command = [self.python, 
                                        self.inpath + "complete/" + "job1.py", 
                                        self.inpath + "complete/" + "file0",
                                        file11, 
                                        file12, "2"], 
                              stdin = self.inpath + "complete/" + "stdin1",
                              join_stderrout=False,
                              disposal_timeout = self.jobs_timeout,
                              name_description = "job1 local custom submission",
                              stdout_path=stdout,
                              stderr_path=stderr,
                              working_directory=self.outpath )
      
    return (jobId, 
            [file11, file12],  
            [stdout, stderr])
              
  def localSubmission(self):
    file11 = self.outpath + "file11"
    file12 = self.outpath + "file12"
    jobId = self.jobs.submit(command = [ self.python, 
                                          self.inpath + "complete/" + "job1.py", 
                                          self.inpath + "complete/" + "file0",
                                          file11, 
                                          file12, "2"], 
                              stdin = self.inpath + "complete/" + "stdin1",
                              join_stderrout=False, 
                              disposal_timeout = self.jobs_timeout,
                              name_description = "job1 local submission")
    return (jobId,
            [file11, file12],
            None)
     
  def mpiJobSubmission(self, node_num):
    
    #compilation 
    
    l_source = self.jobs.sendFile(self.inpath + "mpi/simple_mpi.c", self.tr_timeout)
    
    l_object = self.jobs.registerFileTransfer(self.outpath + "simple_mpi.o", self.tr_timeout) 
    #/volatile/laguitton/sge6-2u5/mpich/mpich-1.2.7/bin/
    #/opt/mpich/gnu/bin/
    
    mpibin = self.jobs.config.get(self.jobs.resource_id, constants.OCFG_PARALLEL_ENV_MPI_BIN)
    print "mpibin = " + mpibin
    
    print "l_source = " + l_source
    print "l_object = " + l_object
    compil1jobId = self.jobs.submit( command = [ mpibin+"/mpicc", 
                                                  "-c", l_source, 
                                                  "-o", l_object ], 
                                      referenced_input_files = [l_source],
                                      referenced_output_files = [l_object],
                                      join_stderrout=False, 
                                      disposal_timeout = self.jobs_timeout,
                                      name_description = "job compil1 mpi")
    
    self.jobs.wait([compil1jobId])
    
    l_bin = self.jobs.registerFileTransfer(self.outpath + "simple_mpi", self.tr_timeout) 
    print "l_bin = " + l_bin
    
    compil2jobId = self.jobs.submit( command = [ mpibin+"/mpicc", 
                                                "-o", l_bin, 
                                                l_object ], 
                                    referenced_input_files = [l_object],
                                    referenced_output_files = [l_bin],
                                    join_stderrout=False, 
                                    disposal_timeout = self.jobs_timeout,
                                    name_description = "job compil2 mpi")
    
    self.jobs.wait([compil2jobId])
    self.jobs.cancelTransfer(l_object)

    
    # mpi job submission
    script = self.jobs.sendFile(self.inpath + "mpi/simple_mpi.sh", self.tr_timeout)
    
    jobId = self.jobs.submit( command = [ script, repr(node_num), l_bin], 
                              referenced_input_files = [script, l_bin],
                              join_stderrout=False, 
                              disposal_timeout = self.jobs_timeout,
                              name_description = "parallel job mpi",
                              parallel_job_info = (constants.OCFG_PARALLEL_PC_MPI,node_num))

    self.jobs.dispose(compil1jobId)
    self.jobs.dispose(compil2jobId)
    

    return (jobId, [l_source], None)
          
     
class JobsTest(unittest.TestCase):
  '''
  Abstract class for jobs common tests.
  '''
  @staticmethod
  def setupConnection():
    JobsTest.test_config = ConfigParser.ConfigParser()
    JobsTest.test_config.read('TestJobs.cfg')
    JobsTest.hostname = socket.gethostname()
    
    if JobsTest.test_config.get(JobsTest.hostname, 'mode') == 'remote':
      print "Ressource => " + JobsTest.test_config.get(JobsTest.hostname, 'ressource_id')
      print "login: ",
      JobsTest.login = raw_input()
      JobsTest.password = getpass.getpass()
    else:
      JobsTest.login = None
      JobsTest.password = None
    
    
    JobsTest.jobs = soma.jobs.jobClient.Jobs(os.environ["SOMA_JOBS_CONFIG"],
                                    JobsTest.test_config.get(JobsTest.hostname, 'ressource_id'), 
                                    JobsTest.login, 
                                    JobsTest.password,
                                    log="1")
  
    JobsTest.transfer_timeout = -24 
    JobsTest.jobs_timeout = 1
  
    JobsTest.jobExamples = JobExamples(JobsTest.jobs, 
                              JobsTest.test_config.get(JobsTest.hostname, 'job_examples_dir'), 
                              JobsTest.test_config.get(JobsTest.hostname, 'job_output_dir'), 
                              JobsTest.test_config.get(JobsTest.hostname, 'python'),
                              JobsTest.transfer_timeout, 
                              JobsTest.jobs_timeout)   
  
    JobsTest.outpath = JobsTest.test_config.get(JobsTest.hostname, 'job_output_dir')
                                   
  def setUp(self):
    raise Exception('JobTest is an abstract class. SetUp must be implemented in subclass')
                                   
   
  def tearDown(self):
    for jid in self.myJobs:
      JobsTest.jobs.dispose(jid)
    remainingJobs = frozenset(JobsTest.jobs.jobs())
    self.failUnless(len(remainingJobs.intersection(self.myJobs)) == 0)
     
                                   
  def test_jobs(self):
    res = set(JobsTest.jobs.jobs())
    self.failUnless(res.issuperset(self.myJobs))
     
  def test_wait(self):
    JobsTest.jobs.wait(self.myJobs)
    for jid in self.myJobs:
      status = JobsTest.jobs.status(jid)
      self.failUnless(status == constants.DONE or 
                      status == constants.FAILED,
                      'Job %s status after wait: %s' %(jid, status))
                      
  def test_wait2(self):
    startTime = datetime.now()
    interval = 5
    JobsTest.jobs.wait(self.myJobs, interval)
    delta = datetime.now() - startTime
    if delta < timedelta(seconds=interval):
      for jid in self.myJobs:
        status = JobsTest.jobs.status(jid)
        self.failUnless(status == constants.DONE or 
                        status == constants.FAILED,
                        'Job %s status after wait: %s' %(self.myJobs[0], status))
    else:
      self.failUnless( abs(delta-timedelta(seconds=interval)) < timedelta(seconds=1))
   
  def test_stop(self):
    jobid = self.myJobs[len(self.myJobs)-1]
    JobsTest.jobs.stop(jobid)
    status = JobsTest.jobs.status(jobid)
    self.failUnless(status == constants.DONE or
                    status == constants.FAILED or 
                    status == constants.USER_ON_HOLD or 
                    status == constants.USER_SYSTEM_ON_HOLD or
                    status == constants.USER_SUSPENDED or
                    status == constants.USER_SYSTEM_SUSPENDED,
                    'Job status after stop: %s' %status)
    
  def test_restart(self):
    jobid = self.myJobs[len(self.myJobs)-1]
    JobsTest.jobs.stop(jobid)
    JobsTest.jobs.restart(jobid)
    status = JobsTest.jobs.status(jobid)
    self.failUnless(not status == constants.USER_ON_HOLD and  
                    not status == constants.USER_SYSTEM_ON_HOLD and
                    not status == constants.USER_SUSPENDED and
                    not status == constants.USER_SYSTEM_SUSPENDED,
                    'Job status after restart: %s' %status)
   
  def test_kill(self):
    jobid = self.myJobs[0]
    JobsTest.jobs.kill(jobid)
    exitInformation = JobsTest.jobs.exitInformation(jobid)
    exitStatus = exitInformation[0]
    status = JobsTest.jobs.status(jobid)
    self.failUnless(status == constants.FAILED or status == constants.DONE, 
                    'Job status after kill: %s' %status)
    self.failUnless(exitStatus == constants.USER_KILLED or exitStatus == constants.FINISHED_REGULARLY, 
                    'Job exit status after kill: %s' %exitStatus)
                    
  def testResult(self):
    raise Exception('JobTest is an abstract class. testResult must be implemented in subclass')
 
  def copystdouterr(self, jobid, stdoutPath, stderrPath):
    f = open(stdoutPath, 'w')
    line = JobsTest.jobs.stdoutReadLine(jobid)
    while line:
      f.write(line)
      line = JobsTest.jobs.stdoutReadLine(jobid)
    f.close()
    f = open(stderrPath, 'w')
    line = JobsTest.jobs.stderrReadLine(jobid)
    while line:
      f.write(line)
      line = JobsTest.jobs.stderrReadLine(jobid)
    f.close()
 
 
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
  
  def testResult(self):
    jobid = self.myJobs[0]
    JobsTest.jobs.wait(self.myJobs)
    status = JobsTest.jobs.status(jobid)
    self.failUnless(status == constants.DONE,
                    'Job %s status after wait: %s' %(jobid, status))
    exitInformation = JobsTest.jobs.exitInformation(jobid)
    exitStatus = exitInformation[0]
    self.failUnless(exitStatus == constants.FINISHED_REGULARLY, 
                    'Job %s exit status: %s' %(jobid, exitStatus))
    exitValue = exitInformation[1]
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
      
  def testResult(self):
    jobid = self.myJobs[0]
    JobsTest.jobs.wait(self.myJobs)
    status = JobsTest.jobs.status(jobid)
    self.failUnless(status == constants.DONE,
                    'Job %s status after wait: %s' %(jobid, status))
    exitInformation = JobsTest.jobs.exitInformation(jobid)
    exitStatus = exitInformation[0]
    self.failUnless(exitStatus == constants.FINISHED_REGULARLY, 
                    'Job %s exit status: %s' %(jobid, exitStatus))
    exitValue = exitInformation[1]
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
    self.copystdouterr(self.myJobs[0], stdout, stderr)
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
    self.remoteFiles = []
   
  def tearDown(self):
    JobsTest.tearDown(self)
    for file in self.remoteFiles:
      if os.path.isfile(file): os.remove(file)
      
  def testResult(self):
    jobid = self.myJobs[0]
    JobsTest.jobs.wait(self.myJobs)
    status = JobsTest.jobs.status(jobid)
    self.failUnless(status == constants.DONE,
                    'Job %s status after wait: %s' %(jobid, status))
    exitInformation = JobsTest.jobs.exitInformation(jobid)
    exitStatus = exitInformation[0]
    self.failUnless(exitStatus == constants.FINISHED_REGULARLY, 
                    'Job %s exit status: %s' %(jobid, exitStatus))
    exitValue = exitInformation[1]
    self.failUnless(exitValue == 0,
                    'Job exit value: %d' %exitValue)
    
    # checking output files
    for file in self.outputFiles:
      remote_file = JobsTest.jobs.transferInformation(file)[1]
      self.failUnless(remote_file)
      JobsTest.jobs.retrieveFile(file)
      self.failUnless(os.path.isfile(remote_file), 'File %s doesn t exit' %file)
      self.remoteFiles.append(remote_file)
   
    (correct, msg) = checkFiles(self.remoteFiles, JobsTest.jobExamples.job1OutputFileModels)
    self.failUnless(correct, msg)
    
    # checking stdout and stderr
    remote_stdout = JobsTest.outpath + "/stdout_submit_with_transfer"
    remote_stderr = JobsTest.outpath + "/stderr_submit_with_transfer"
    self.copystdouterr(self.myJobs[0], remote_stdout, remote_stderr)
    self.remoteFiles.append(remote_stdout)
    self.remoteFiles.append(remote_stderr)
  
    (correct, msg) = checkFiles(self.remoteFiles[2:5], JobsTest.jobExamples.job1stdouterrModels)
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
    self.remoteFiles = []

    JobsTest.jobs.wait(self.myJobs)
   
  def tearDown(self):
    JobsTest.tearDown(self)
  
  def testResult(self):
    self.failUnless(True)

    
class JobPipelineWithTransfer(JobsTest):
  '''
  Submission of a job pipeline with transfer
  '''
  def setUp(self):
    self.myJobs = []
    self.myTransfers = []
    self.remoteFiles = []
    self.outputFiles = []
    
    # Job1 
    
    info1 = JobsTest.jobExamples.submitJob1()
    self.myJobs.append(info1[0]) 
    self.outputFiles.extend(info1[1])

    JobsTest.jobs.wait(self.myJobs)
    status = JobsTest.jobs.status(self.myJobs[0])
    self.failUnless(status == constants.DONE,
                    'Job %s status after wait: %s' %(self.myJobs[0], status))
    exitInformation = JobsTest.jobs.exitInformation(self.myJobs[0])
    exitStatus = exitInformation[0]
    self.failUnless(exitStatus == constants.FINISHED_REGULARLY, 
                    'Job %s exit status: %s' %(self.myJobs[0], exitStatus))
    exitValue = exitInformation[1]
    self.failUnless(exitValue == 0,
                    'Job exit value: %d' %exitValue)
                    
    # Job2 & 3
    
    info2 = JobsTest.jobExamples.submitJob2()
    self.myJobs.append(info2[0]) 
    self.outputFiles.extend(info2[1])
    
    info3 = JobsTest.jobExamples.submitJob3()
    self.myJobs.append(info3[0]) 
    self.outputFiles.extend(info3[1])

    JobsTest.jobs.wait(self.myJobs)
    status = JobsTest.jobs.status(self.myJobs[1])
    self.failUnless(status == constants.DONE,
                    'Job %s status after wait: %s' %(self.myJobs[1], status))
    exitInformation = JobsTest.jobs.exitInformation(self.myJobs[1])
    exitStatus = exitInformation[0]
    self.failUnless(exitStatus == constants.FINISHED_REGULARLY, 
                    'Job %s exit status: %s' %(self.myJobs[1], exitStatus))
    exitValue = exitInformation[1]
    self.failUnless(exitValue == 0,
                    'Job exit value: %d' %exitValue)
                    
    status = JobsTest.jobs.status(self.myJobs[2])
    self.failUnless(status == constants.DONE,
                    'Job %s status after wait: %s' %(self.myJobs[2], status))
    exitInformation = JobsTest.jobs.exitInformation(self.myJobs[2])
    exitStatus = exitInformation[0]
    self.failUnless(exitStatus == constants.FINISHED_REGULARLY, 
                    'Job %s exit status: %s' %(self.myJobs[2], exitStatus))
    exitValue = exitInformation[1]
    self.failUnless(exitValue == 0,
                    'Job exit value: %d' %exitValue)
    
    # Job 4
    
    info4 = JobsTest.jobExamples.submitJob4()
    self.myJobs.append(info4[0]) 
    self.outputFiles.extend(info4[1])

                    
                    
   
  def tearDown(self):
    JobsTest.tearDown(self)
    for file in self.remoteFiles:
      if os.path.isfile(file): os.remove(file)
      
  def testResult(self):
    jobid = self.myJobs[len(self.myJobs)-1]
    JobsTest.jobs.wait(self.myJobs)
    status = JobsTest.jobs.status(jobid)
    self.failUnless(status == constants.DONE,
                    'Job %s status after wait: %s' %(jobid, status))
    exitInformation = JobsTest.jobs.exitInformation(jobid)
    exitStatus = exitInformation[0]
    self.failUnless(exitStatus == constants.FINISHED_REGULARLY, 
                    'Job %s exit status: %s' %(jobid, exitStatus))
    exitValue = exitInformation[1]
    self.failUnless(exitValue == 0,
                    'Job exit value: %d' %exitValue)
    
    
    # checking output files
    for file in self.outputFiles:
      remote_file = JobsTest.jobs.transferInformation(file)[1]
      self.failUnless(remote_file)
      JobsTest.jobs.retrieveFile(file)
      self.failUnless(os.path.isfile(remote_file), 'File %s doesn t exit' %file)
      self.remoteFiles.append(remote_file)
    
    models = JobsTest.jobExamples.job1OutputFileModels + JobsTest.jobExamples.job2OutputFileModels + JobsTest.jobExamples.job3OutputFileModels + JobsTest.jobExamples.job4OutputFileModels
    (correct, msg) = checkFiles(self.remoteFiles, models)
    self.failUnless(correct, msg)
    

    # checking stdout and stderr
    remote_stdout = JobsTest.outpath + "/stdout_pipeline_job1"
    remote_stderr = JobsTest.outpath + "/stderr_pipeline_job1"
    self.copystdouterr(self.myJobs[0], remote_stdout, remote_stderr)
    self.remoteFiles.append(remote_stdout)
    self.remoteFiles.append(remote_stderr)
    
    remote_stdout = JobsTest.outpath + "/stdout_pipeline_job2"
    remote_stderr = JobsTest.outpath + "/stderr_pipeline_job2"
    self.copystdouterr(self.myJobs[1], remote_stdout, remote_stderr)
    self.remoteFiles.append(remote_stdout)
    self.remoteFiles.append(remote_stderr)
  
    remote_stdout = JobsTest.outpath + "/stdout_pipeline_job3"
    remote_stderr = JobsTest.outpath + "/stderr_pipeline_job3"
    self.copystdouterr(self.myJobs[2], remote_stdout, remote_stderr)
    self.remoteFiles.append(remote_stdout)
    self.remoteFiles.append(remote_stderr)
    
    remote_stdout = JobsTest.outpath + "/stdout_pipeline_job4"
    remote_stderr = JobsTest.outpath + "/stderr_pipeline_job4"
    self.copystdouterr(self.myJobs[3], remote_stdout, remote_stderr)
    self.remoteFiles.append(remote_stdout)
    self.remoteFiles.append(remote_stderr)
   
    models = JobsTest.jobExamples.job1stdouterrModels + JobsTest.jobExamples.job2stdouterrModels + JobsTest.jobExamples.job3stdouterrModels + JobsTest.jobExamples.job4stdouterrModels
    (correct, msg) = checkFiles(self.remoteFiles[5:13], models)
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
    self.remoteFiles = []
    
  def tearDown(self):
    JobsTest.tearDown(self)
    for file in self.remoteFiles:
      if os.path.isfile(file): os.remove(file)
   
  def testResult(self):
    jobid = self.myJobs[0]
    JobsTest.jobs.wait(self.myJobs)
    status = JobsTest.jobs.status(jobid)
    self.failUnless(status == constants.DONE,
                    'Job %s status after wait: %s' %(jobid, status))
    exitInformation = JobsTest.jobs.exitInformation(jobid)
    exitStatus = exitInformation[0]
    self.failUnless(exitStatus == constants.FINISHED_REGULARLY, 
                    'Job %s exit status: %s' %(jobid, exitStatus))
    exitValue = exitInformation[1]
    self.failUnless(exitValue == 1,
                    'Job exit value: %d' %exitValue)
    # checking stdout and stderr
    remote_stdout = JobsTest.outpath + "/stdout_exception_job"
    remote_stderr = JobsTest.outpath + "/stderr_exception_job"
    self.copystdouterr(jobid, remote_stdout, remote_stderr)
    self.remoteFiles.append(remote_stdout)
    self.remoteFiles.append(remote_stderr)
    
    (identical, msg) = checkFiles(self.remoteFiles, JobsTest.jobExamples.exceptionjobstdouterr,1)
    self.failUnless(identical, msg)

    
class DisconnectionTest(JobsTest):
  '''
  Submission of a job pipeline with transfer
  '''
  def setUp(self):
    self.myJobs = []
    self.myTransfers = []
    self.remoteFiles = []
    self.outputFiles = []
    
    # Job1 
    
    info1 = JobsTest.jobExamples.submitJob1()
    self.myJobs.append(info1[0]) 
    self.outputFiles.extend(info1[1])

    JobsTest.jobs.wait(self.myJobs)
    status = JobsTest.jobs.status(self.myJobs[0])
    self.failUnless(status == constants.DONE,
                    'Job %s status after wait: %s' %(self.myJobs[0], status))
    exitInformation = JobsTest.jobs.exitInformation(self.myJobs[0])
    exitStatus = exitInformation[0]
    self.failUnless(exitStatus == constants.FINISHED_REGULARLY, 
                    'Job %s exit status: %s' %(self.myJobs[0], exitStatus))
    exitValue = exitInformation[1]
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
    JobsTest.jobs.disconnect()
    del JobsTest.jobs
    time.sleep(20)
    print ".... Reconnection"

    JobsTest.jobs = soma.jobs.jobClient.Jobs(os.environ["SOMA_JOBS_CONFIG"],
                                             JobsTest.test_config.get(JobsTest.hostname, 'ressource_id'), 
                                             JobsTest.login, 
                                             JobsTest.password,
                                             log="2")


    JobsTest.jobExamples.setNewConnection(JobsTest.jobs)
    #time.sleep(1)
   
  def tearDown(self):
    #pass
    JobsTest.tearDown(self)
    for file in self.remoteFiles:
      if os.path.isfile(file): os.remove(file)
      
  def testResult(self):

    JobsTest.jobs.wait(self.myJobs)
    status = JobsTest.jobs.status(self.myJobs[1])
    self.failUnless(status == constants.DONE,
                    'Job %s status after wait: %s' %(self.myJobs[1], status))
    exitInformation = JobsTest.jobs.exitInformation(self.myJobs[1])
    exitStatus = exitInformation[0]
    self.failUnless(exitStatus == constants.FINISHED_REGULARLY, 
                    'Job %s exit status: %s' %(self.myJobs[1], exitStatus))
    exitValue = exitInformation[1]
    self.failUnless(exitValue == 0,
                    'Job exit value: %d' %exitValue)
                    
    status = JobsTest.jobs.status(self.myJobs[2])
    self.failUnless(status == constants.DONE,
                    'Job %s status after wait: %s' %(self.myJobs[2], status))
    exitInformation = JobsTest.jobs.exitInformation(self.myJobs[2])
    exitStatus = exitInformation[0]
    self.failUnless(exitStatus == constants.FINISHED_REGULARLY, 
                    'Job %s exit status: %s' %(self.myJobs[2], exitStatus))
    exitValue = exitInformation[1]
    self.failUnless(exitValue == 0,
                    'Job exit value: %d' %exitValue)
    
    # Job 4
    
    info4 = JobsTest.jobExamples.submitJob4()
    self.failUnless(not info4[0] == -1, "The job was not submitted.")
    self.myJobs.append(info4[0]) 
    self.outputFiles.extend(info4[1])

    jobid = self.myJobs[len(self.myJobs)-1]
    JobsTest.jobs.wait(self.myJobs)
    status = JobsTest.jobs.status(jobid)
    self.failUnless(status == constants.DONE,
                    'Job %s status after wait: %s' %(jobid, status))
    exitInformation = JobsTest.jobs.exitInformation(jobid)
    exitStatus = exitInformation[0]
    self.failUnless(exitStatus == constants.FINISHED_REGULARLY, 
                    'Job %s exit status: %s' %(jobid, exitStatus))
    exitValue = exitInformation[1]
    self.failUnless(exitValue == 0,
                    'Job exit value: %d' %exitValue)
    
    
    # checking output files
    for file in self.outputFiles:
      remote_file = JobsTest.jobs.transferInformation(file)[1]
      self.failUnless(remote_file)
      JobsTest.jobs.retrieveFile(file)
      self.failUnless(os.path.isfile(remote_file), 'File %s doesn t exit' %file)
      self.remoteFiles.append(remote_file)
    
    models = JobsTest.jobExamples.job1OutputFileModels + JobsTest.jobExamples.job2OutputFileModels + JobsTest.jobExamples.job3OutputFileModels + JobsTest.jobExamples.job4OutputFileModels
    (correct, msg) = checkFiles(self.remoteFiles, models)
    self.failUnless(correct, msg)
    

    # checking stdout and stderr
    remote_stdout = JobsTest.outpath + "/stdout_pipeline_job1"
    remote_stderr = JobsTest.outpath + "/stderr_pipeline_job1"
    self.copystdouterr(self.myJobs[0], remote_stdout, remote_stderr)
    self.remoteFiles.append(remote_stdout)
    self.remoteFiles.append(remote_stderr)
    
    remote_stdout = JobsTest.outpath + "/stdout_pipeline_job2"
    remote_stderr = JobsTest.outpath + "/stderr_pipeline_job2"
    self.copystdouterr(self.myJobs[1], remote_stdout, remote_stderr)
    self.remoteFiles.append(remote_stdout)
    self.remoteFiles.append(remote_stderr)
  
    remote_stdout = JobsTest.outpath + "/stdout_pipeline_job3"
    remote_stderr = JobsTest.outpath + "/stderr_pipeline_job3"
    self.copystdouterr(self.myJobs[2], remote_stdout, remote_stderr)
    self.remoteFiles.append(remote_stdout)
    self.remoteFiles.append(remote_stderr)
    
    remote_stdout = JobsTest.outpath + "/stdout_pipeline_job4"
    remote_stderr = JobsTest.outpath + "/stderr_pipeline_job4"
    self.copystdouterr(self.myJobs[3], remote_stdout, remote_stderr)
    self.remoteFiles.append(remote_stdout)
    self.remoteFiles.append(remote_stderr)
   
    models = JobsTest.jobExamples.job1stdouterrModels + JobsTest.jobExamples.job2stdouterrModels + JobsTest.jobExamples.job3stdouterrModels + JobsTest.jobExamples.job4stdouterrModels
    (correct, msg) = checkFiles(self.remoteFiles[5:13], models,1)
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
      
  def testResult(self):
    jobid = self.myJobs[0]
    JobsTest.jobs.wait(self.myJobs)
    
    status = JobsTest.jobs.status(jobid)
    self.failUnless(status == constants.DONE,
                    'Job %s status after wait: %s' %(jobid, status))
    exitInformation = JobsTest.jobs.exitInformation(jobid)
    exitStatus = exitInformation[0]
    self.failUnless(exitStatus == constants.FINISHED_REGULARLY, 
                    'Job %s exit status: %s' %(jobid, exitStatus))
    exitValue = exitInformation[1]
    self.failUnless(exitValue == 0,
                    'Job exit value: %d' %exitValue)
                    

    print "stdout: "
    line = JobsTest.jobs.stdoutReadLine(jobid)
    process_num = 1
    while line:
      splitted_line = line.split()
      if splitted_line[0] == "Grettings":
        self.failUnless(line.rstrip() == "Grettings from process %d!" %(process_num), 
                        "stdout line:  %sinstead of  : 'Grettings from process %d!'" %(line, process_num))
        process_num = process_num +1
      line = JobsTest.jobs.stdoutReadLine(jobid)

    self.failUnless(process_num==self.node_num, 
                    "%d process(es) run instead of %d." %(process_num-1, self.node_num))

      



if __name__ == '__main__':
  
  #all = False
  all = True
  
  JobsTest.setupConnection()
  
  suite_list = []
  if all:
    #suite_list.append(unittest.TestLoader().loadTestsFromTestCase(LocalCustomSubmission))
    #suite_list.append(unittest.TestLoader().loadTestsFromTestCase(LocalSubmission))
    #suite_list.append(unittest.TestLoader().loadTestsFromTestCase(SubmissionWithTransfer))
    #suite_list.append(unittest.TestLoader().loadTestsFromTestCase(ExceptionJobTest))
    #suite_list.append(unittest.TestLoader().loadTestsFromTestCase(JobPipelineWithTransfer))
    #suite_list.append(unittest.TestLoader().loadTestsFromTestCase(DisconnectionTest))
    #suite_list.append(unittest.TestLoader().loadTestsFromTestCase(EndedJobWithTransfer))
    suite_list.append(unittest.TestLoader().loadTestsFromTestCase(MPIParallelJobTest))

  else:
    
    minimal = ['testResult']#'test_wait2'] #, 'test_wait' ]

    tests = minimal
    
    #suite_list.append(unittest.TestSuite(map(LocalCustomSubmission, tests)))
    #suite_list.append(unittest.TestSuite(map(LocalSubmission, tests)))
    #suite_list.append(unittest.TestSuite(map(SubmissionWithTransfer, tests)))
    #suite_list.append(unittest.TestSuite(map(ExceptionJobTest, tests)))
    #suite_list.append(unittest.TestSuite(map(JobPipelineWithTransfer, tests)))
    #suite_list.append(unittest.TestSuite(map(DisconnectionTest, tests)))
    #suite_list.append(unittest.TestSuite(map(EndedJobWithTransfer, tests)))
    suite_list.append(unittest.TestSuite(map(MPIParallelJobTest, tests)))


  alltests = unittest.TestSuite(suite_list)
  unittest.TextTestRunner(verbosity=2).run(alltests)
  
  sys.exit(0)
