import sys
from datetime import datetime
import Pyro.naming, Pyro.core
from Pyro.errors import NamingError
import time
import os

'''
2 modes:
@type  mode: 'local' or 'remote'
@param mode: 
  'local': (default) if run on a submitting machine of the pool
  'remote': if run from a machine whichis not a submitting machine of the pool 
  and doesn't share a file system with these machines
'''

mode = 'remote'
if len(sys.argv) < 2:
  TestNum = 1
else:
  TestNum = sys.argv[4]

if mode == 'local':
  inpath = "/home/sl225510/svn/brainvisa/soma/soma-pipeline/trunk/test/jobExamples/"
  outpath = "/home/sl225510/output/"
  srcLocalJobProcess = "/home/sl225510/svn/brainvisa/soma/soma-pipeline/trunk/python/soma/jobs/localJobProcess.py"
 
  from soma.jobs.fileTransfer import LocalFileTransfer
  from soma.jobs.jobLocalConnection import JobLocalConnection 
  connection = JobLocalConnection(srcLocalJobProcess)
  jsc = connection.getJobScheduler()
  #from soma.jobs.jobScheduler import JobScheduler
  #jsc = JobScheduler()
  ft = LocalFileTransfer(jsc)
  
  import soma.jobs.jobDatabase
  def printTables():
    soma.jobs.jobDatabase.printTables("/volatile/laguitton/job.db")
  

if mode == 'remote':
  from jobRemoteConnection import JobRemoteConnection 
  from fileTransfer import RemoteFileTransfer
  import sys
  import getpass
  
  sys.stdout.write("login: ")
  _login = "sl225510" 
  print _login
  _password = getpass.getpass()
  
  connection = JobRemoteConnection(_login, _password)
  jsc = connection.getJobScheduler()
  ft = RemoteFileTransfer(jsc)
  inpath = "/home/soizic/jobExamples/"
  outpath = "/home/soizic/output/"


#### COMPLETE JOB EXAMPLE ###################################

#job1########################################################

#input files
script1 = inpath + "complete/" + "job1.py"
stdin1  = inpath + "complete/" + "stdin1"
file0   = inpath + "complete/" + "file0"

#output files
file11 = outpath + "file11"
file12 = outpath + "file12"


#job2########################################################
#input files
script2 = inpath + "complete/" + "job2.py"
stdin2  = inpath + "complete/" + "stdin2"
#file0
#file11

#output files
file2 = outpath + "file2"

#job3########################################################
#input files
script3 = inpath + "complete/" + "job3.py"
stdin3  = inpath + "complete/" + "stdin3"
#file12

#output files
file3 = outpath + "file3"

#job4########################################################
#input files
script4 = inpath + "complete/" + "job4.py"
stdin4  = inpath + "complete/" + "stdin4"
#file2
#file4

#output files
file4 = outpath + "file4"


#python = "/i2bm/research/Mandriva-2008.0-i686/bin/python" #condor
python = "python" #SGE

#CUSTOM SUBMISSION#############################################

def customSubmission():
  jobId = jsc.customSubmit([python, 
                            script1, 
                            file0, 
                            outpath + "file11_custom", 
                            outpath + "file12_custom", "10"], 
                            outpath, 
                            outpath + "stdoutjob1", 
                            outpath + "stderrjob1",
                            stdin1,
                            1,
                            "job1 custom submission")
  return jobId
                            

#REGULAR SUBMISSION###########################################

def regularSubmission():
  jobId = jsc.submit( [ python, 
                        script1, 
                        file0, 
                        outpath + "file11_regular", 
                        outpath + "file12_regular"], 
                        None, 
                        False, 
                        stdin1,
                        1,
                        "job1 regular submission")
  return jobId

#SUBMISSION WITH TRANSFER#####################################


tr_time_out = -24
jobs_time_out = 1

def submitWTjob1():
  global script1, stdin1, file0, file11, file12
  global l_file0, l_file11, l_file12, l_script1, l_stdin1, job1id

  l_file0 = ft.transferInputFile(file0, tr_time_out) 
  
  l_file11 = jsc.registerTransfer(file11, tr_time_out) 
  l_file12 = jsc.registerTransfer(file12, tr_time_out) 


  # .....
  
  
  l_script1 = ft.transferInputFile(script1, tr_time_out) 
  l_stdin1 = ft.transferInputFile(stdin1, tr_time_out) 
  
  job1id = jsc.submitWithTransfer( [python, l_script1, l_file0, l_file11, l_file12, "30"], 
                                   [l_file0, l_script1, l_stdin1], 
                                   [l_file11, l_file12], 
                                   False, l_stdin1, jobs_time_out, "job1") 

  return job1id

def submitWTjob2():
  global script2, stdin2, file2
  global l_file0, l_file11
  global l_file2, l_script2, l_stdin2, job2id
  
  l_file2 = jsc.registerTransfer(file2, tr_time_out) 

  l_script2 = ft.transferInputFile(script2, tr_time_out) 
  l_stdin2 = ft.transferInputFile(stdin2, tr_time_out) 

  job2id = jsc.submitWithTransfer( [python, l_script2, l_file11, l_file0, l_file2, "2"], 
                                   [l_file0, l_file11, l_script2, l_stdin2], 
                                   [l_file2], 
                                   False, l_stdin2, jobs_time_out, "job2") 

  return job2id

def submitWTjob3( ):
  global script3, stdin3, file3
  global l_file12
  global l_file3, l_script3, l_stdin3, job3id
  
  l_file3 = jsc.registerTransfer(file3, tr_time_out) 

  l_script3 = ft.transferInputFile(script3, tr_time_out) 
  l_stdin3 = ft.transferInputFile(stdin3, tr_time_out) 
  
  job3id = jsc.submitWithTransfer( [python, l_script3, l_file12, l_file3, "2"], 
                                   [l_file12, l_script3, l_stdin3], 
                                   [l_file3], 
                                   False, l_stdin3, jobs_time_out, "job3") 

  return job3id

def submitWTjob4( ):
  global script4, stdin4, file4
  global l_file2, l_file3
  global l_file4, l_script4, l_stdin4, job4id
  
  l_file4 = jsc.registerTransfer(file4, tr_time_out) 

  l_script4 = ft.transferInputFile(script4, tr_time_out) 
  l_stdin4 = ft.transferInputFile(stdin4, tr_time_out) 

  job4id = jsc.submitWithTransfer( [python, l_script4, l_file2, l_file3, l_file4], 
                                   [l_file2, l_file3, l_script4, l_stdin4], 
                                   [l_file4], 
                                   False, l_stdin4, jobs_time_out, "job4") 
  
  return job4id


def checkFile(filename):
  if os.path.isfile(filename): return " exists"
  else: return " doesn't exist !!!!"

############################################

if mode == 'local':
  print "######### CUSTOM SUBMISSION TEST #############"
  jobid = customSubmission()
  print "job id = " + repr(jobid)
  print "job information : " + repr(jsc.generalInformation(jobid))
  jsc.wait([jobid], 2)
  jsc.stop(jobid)
  time.sleep(1)
  print "stopped, status: " + jsc.status(jobid)
  time.sleep(1)
  jsc.restart(jobid)
  time.sleep(1)
  print "restarted, status: " + jsc.status(jobid)
  print "waiting..."
  jsc.wait([jobid])
  time.sleep(1)
  print "end, status: " + jsc.status(jobid)
  print "exit info: " + repr(jsc.exitInformation(jobid))
  print "checking that files exist"
  print outpath + "stdoutjob1" + checkFile(outpath + "stdoutjob1")
  print outpath + "stderrjob1" + checkFile(outpath + "stdoutjob1")
  print file11 + checkFile(file11)
  print file12 + checkFile(file12)
  jsc.dispose(jobid)

  print "######### REGULAR SUBMISSION TEST #############"
  jobid = regularSubmission()
  print "job id = " + repr(jobid)
  print "job information : " + repr(jsc.generalInformation(jobid))
  jsc.wait([jobid], 2)
  jsc.stop(jobid)
  time.sleep(1)
  print "stopped, status: " + jsc.status(jobid)
  time.sleep(1)
  jsc.restart(jobid)
  time.sleep(1)
  print "restarted, status: " + jsc.status(jobid)
  print "waiting..."
  jsc.wait([jobid])
  time.sleep(1)
  print "end, status: " + jsc.status(jobid)
  print "exit info: " + repr(jsc.exitInformation(jobid))
  print "checking that files exist"
  print file11 + checkFile(file11)
  print file12 + checkFile(file12)
  print "stdout :"
  line = jsc.stdoutReadLine(jobid)
  while line:
    print line,
    line = jsc.stdoutReadLine(jobid)
  
  
#print "########## JOB WITH EXCEPTION TEST #############"

#l_exceptionScript = ft.transferInputFile(inpath + "simple/exceptionJob.py", -24)
  
#jobid = jsc.submitWithTransfer( [python, l_exceptionScript], 
                                  #[l_exceptionScript], 
                                  #[], 
                                  #False, None, jobs_time_out, "job with exception") 
#print "job id = " + repr(jobid)
#print "job information : " + repr(jsc.generalInformation(jobid))
#jsc.wait([jobid], 2)
#jsc.stop(jobid)
#time.sleep(1)
#print "stopped, status: " + jsc.status(jobid)
#time.sleep(1)
#jsc.restart(jobid)
#time.sleep(1)
#print "restarted, status: " + jsc.status(jobid)
#print "waiting..."
#jsc.wait([jobid])
#time.sleep(1)
#print "end, status: " + jsc.status(jobid)
#print "exit info: " + repr(jsc.exitInformation(jobid))
#print "stderr :"
#line = jsc.stderrReadLine(jobid)
#while line:
  #print line,
  #line = jsc.stderrReadLine(jobid)



print "######### COMPLETE SUBMISSION TEST #############"

startTime = datetime.now()

job1id = submitWTjob1()
print "submission"
print "job1 id = " + repr(job1id)
print "job1 information : " + repr(jsc.generalInformation(job1id))
print "waiting..."
jsc.wait([job1id])
time.sleep(1)
print "end job1, status: " + jsc.status(job1id)
print "exit info job1: " + repr(jsc.exitInformation(job1id))
print "stderr job1:"
line = jsc.stderrReadLine(job1id)
while line:
  print line,
  line = jsc.stderrReadLine(job1id)


job2id = submitWTjob2()
print "submission"
print "job2 id = " + repr(job2id)
print "job2 information : " + repr(jsc.generalInformation(job2id))
job3id = submitWTjob3()
print "submission"
print "job3 id = " + repr(job3id)
print "job3 information : " + repr(jsc.generalInformation(job3id))
time.sleep(3)
jsc.kill(job2id)
time.sleep(1)
print "kill job2, status: " + jsc.status(job2id)
print "exit info: " + repr(jsc.exitInformation(job2id))
print "stderr :"
line = jsc.stderrReadLine(job2id)
while line:
  print line,
  line = jsc.stderrReadLine(job2id)
print "stdout :"
line = jsc.stdoutReadLine(job2id)
while line:
  print line,
  line = jsc.stdoutReadLine(job2id)
jsc.dispose(job2id)
jsc.stop(job3id)
time.sleep(1)
print "stop job3, status: " + jsc.status(job3id)
jsc.restart(job3id)
time.sleep(1)
print "restart job3, status: " + jsc.status(job3id)
job2id = submitWTjob2()
print "submission"
print "job2 id = " + repr(job2id)
print "job2 information : " + repr(jsc.generalInformation(job2id))
print "waiting..."
jsc.wait([job2id, job3id])

time.sleep(1)
print "end job2, status: " + jsc.status(job2id)
print "exit info job2: " + repr(jsc.exitInformation(job2id))
print "stderr job2:"
line = jsc.stderrReadLine(job2id)
while line:
  print line,
  line = jsc.stderrReadLine(job2id)

time.sleep(1)
print "end job3, status: " + jsc.status(job3id)
print "exit info job3: " + repr(jsc.exitInformation(job3id))
print "stderr job3:"
line = jsc.stderrReadLine(job3id)
while line:
  print line,
  line = jsc.stderrReadLine(job3id)





job4id = submitWTjob4()
print "submission"
print "job4 id = " + repr(job4id)
print "job4 information : " + repr(jsc.generalInformation(job4id))
print "waiting..."
jsc.wait([job4id])
time.sleep(1)
print "end job4, status: " + jsc.status(job4id)
print "exit info job4: " + repr(jsc.exitInformation(job4id))
print "stderr job4:"
line = jsc.stderrReadLine(job4id)
while line:
  print line,
  line = jsc.stderrReadLine(job4id)



delta = datetime.now()-startTime
print "time: " + repr(delta.seconds) + " seconds."

#transfers = jsc.getTransfers()
#print "transfers " + repr(transfers)
#print "information for " + transfers[1] + " : " + repr(jsc.getTransferInformation(transfers[1]))

print "transfer output file"
ft.transferOutputFile(l_file4)
print l_file4 + checkFile(l_file4)

#print "jobs owned by user " + repr(jsc.jobs())
#print "dispose jobs"
#jsc.dispose(job1id)
#jsc.dispose(job2id)
#jsc.dispose(job3id)
#jsc.dispose(job4id)
##for i in jsc.jobs(): jsc.dispose(i)
#print "jobs owned by user " + repr(jsc.jobs())

