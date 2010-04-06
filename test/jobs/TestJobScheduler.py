import sys
from datetime import datetime
import Pyro.naming, Pyro.core
from Pyro.errors import NamingError
import time
import os

#import soma.jobs.jobDatabase

'''
2 modes:
@type  mode: 'local' or 'remote'
@param mode: 
  'local': (default) if run on a submitting machine of the pool
  'remote': if run from a machine whichis not a submitting machine of the pool 
  and doesn't share a file system with these machines
'''

mode = 'remote'
testNum = 3

if mode == 'local':
  from soma.jobs.newJobScheduler import JobScheduler
  from soma.jobs.fileTransfer import LocalFileTransfer
  jsc = JobScheduler()
  ft = LocalFileTransfer(jsc)
  inpath = "/home/sl225510/projets/jobExamples/complete/"
  outpath = "/home/sl225510/"
  
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
  inpath = "/home/soizic/jobExamples/complete/"
  outpath = "/home/soizic/"


if mode == 'test':
  from soma.jobs.fileTransfer import RemoteFileTransfer
  from soma.jobs.jobScheduler import JobScheduler
  jsc = JobScheduler()
  ft = RemoteFileTransfer(jsc)
  inpath = "/home/sl225510/projets/jobExamples/complete/"
  outpath = "/home/sl225510/"


if mode == 'test2':
  from soma.jobs.fileTransfer import LocalFileTransfer
  from soma.jobs.connectionCheck import ConnectionHolder
  
  Pyro.core.initClient()
  
  locator = Pyro.naming.NameServerLocator()
  ns = locator.getNS(host='is143016')
  try:
      job_scheduler_URI=ns.resolve('toto')
      print "The job scheduler object was found"
  except NamingError,x:
      print 'Couldn\'t find job scheduler, nameserver says:',x
      raise SystemExit
  
  jsc = Pyro.core.getProxyForURI( job_scheduler_URI )
  
  try:
      connection_checker_URI=ns.resolve('connectionChecker')
      print "The ConnectionChecker object was found"
  except NamingError,x:
      print 'Couldn\'t find ConnectionChecker, nameserver says:',x
      raise SystemExit
  
  connectionHolder = ConnectionHolder( Pyro.core.getAttrProxyForURI( connection_checker_URI))
  connectionHolder.start()
  
  ft = LocalFileTransfer(jsc)
  inpath = "/home/sl225510/projets/jobExamples/complete/"
  outpath = "/home/sl225510/"
  
  
#def printTables():
#  soma.jobs.jobDatabase.printTables("/volatile/laguitton/job.db")


#job1########################################################

#input files
script1 = inpath + "job1.py"
stdin1  = inpath + "stdin1"
file0   = inpath + "file0"

#output files
file11 = outpath + "file11"
file12 = outpath + "file12"


#job2########################################################
#input files
script2 = inpath + "job2.py"
stdin2  = inpath + "stdin2"
#file0
#file11

#output files
file2 = outpath + "file2"

#job3########################################################
#input files
script3 = inpath + "job3.py"
stdin3  = inpath + "stdin3"
#file12

#output files
file3 = outpath + "file3"

#job4########################################################
#input files
script4 = inpath + "job4.py"
stdin4  = inpath + "stdin4"
#file2
#file4

#output files
file4 = outpath + "file4"


#CUSTOM SUBMISSION#############################################
#jobId = jsc.customSubmit(["python", script1, file0, file11, file12], 
                          #"/home/sl225510/", 
                          #"/home/sl225510/stdoutjob1", 
                          #None,
                          #l_stdin1,
                          #1)
                          
#path2 = "/home/sl225510/projets/jobExamples/simple/"
#jobId = jsc.customSubmit( [path2+"sh_loop", "30"], 
                          #"/home/sl225510/", 
                          #"/home/sl225510/stdoutSh_loop", 
                          #None,
                          #None,
                          #1)


#REGULAR SUBMISSION###########################################
#jobId = jsc.submit( [path2+"sh_loop", "30"], 
                    #None, 
                    #False, 
                    #None,
                    #-24)

#SUBMISSION WITH TRANSFER#####################################

#python = "/i2bm/research/Mandriva-2008.0-i686/bin/python" #condor
python = "python" #SGE
tr_time_out = 1
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
  
  job1id = jsc.submitWithTransfer( [python, l_script1, l_file0, l_file11, l_file12, "1"], 
                                   [l_file0, l_script1, l_stdin1], 
                                   [l_file11, l_file12], 
                                   True, l_stdin1, jobs_time_out) 

  return job1id

def submitWTjob2():
  global script2, stdin2, file2
  global l_file0, l_file11
  global l_file2, l_script2, l_stdin2, job2id
  
  l_file2 = jsc.registerTransfer(file2, tr_time_out) 

  l_script2 = ft.transferInputFile(script2, tr_time_out) 
  l_stdin2 = ft.transferInputFile(stdin2, tr_time_out) 
  
  #control 
  #if not os.path.isfile(l_script2):
    #print l_script2 + " was not transfered."
    #sys.exit()
  #if not os.path.isfile(l_stdin2):
    #print l_stdin2 + " was not transfered." 
    #sys.exit()
  #########

  job2id = jsc.submitWithTransfer( [python, l_script2, l_file11, l_file0, l_file2, "2"], 
                                   [l_file0, l_file11, l_script2, l_stdin2], 
                                   [l_file2], 
                                   True, l_stdin2, jobs_time_out) 

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
                                   True, l_stdin3, jobs_time_out) 

  return job3id

def submitWTjob4( ):
  global script4, stdin4, file4
  global l_file2, l_file3
  global l_file4, l_script4, l_stdin4, job4id
  
  l_file4 = jsc.registerTransfer(file4, tr_time_out) 

  l_script4 = ft.transferInputFile(script4, tr_time_out) 
  l_stdin4 = ft.transferInputFile(stdin4, tr_time_out) 
  
  #control 
  #if not os.path.isfile(l_script4):
    #print l_script4 + " was not transfered."
    #sys.exit()
  #if not os.path.isfile(l_stdin4):
    #print l_stdin4 + " was not transfered." 
    #sys.exit()
  #########

  job4id = jsc.submitWithTransfer( [python, l_script4, l_file2, l_file3, l_file4], 
                                   [l_file2, l_file3, l_script4, l_stdin4], 
                                   [l_file4], 
                                   True, l_stdin4, jobs_time_out) 
  
  return job4id


def wait(jobid):
  status = jsc.status(jobid)
  while status == "undetermined" or status == "queued_active" or status == "running":
      print "job " + repr(jobid) + " : " + status 
      time.sleep(1)
      status = jsc.status(jobid)

############################################

#startTime = datetime.now()

#job1id = submitWTjob1()
##jsc.wait(job1id)
#wait(job1id)

#job2id = submitWTjob2()
#job3id = submitWTjob3()
##jsc.wait(job2id)
#wait(job2id)
##jsc.wait(job3id)
#wait(job3id)

#job4id = submitWTjob4()
##jsc.wait(job4id)
#wait(job4id)

#delta = datetime.now()-startTime
#print "time: " + repr(delta.seconds) + " seconds."

#ft.transferOutputFile(l_file4)

#jsc.dispose(job1id)
#jsc.dispose(job2id)
#jsc.dispose(job3id)
#jsc.dispose(job4id)


#########################################

for i in range(1, 3):
  
 
  startTime = datetime.now()

  file4 =  outpath + "file4_" + repr(testNum) +"_" + repr(i) 


  job1id = submitWTjob1()
  print "job1 submitted \n"

  #jsc.wait(job1id)
  status = jsc.status(job1id)
  print "job " + repr(job1id) + " : " + jsc.status(job1id) 
  while status == "undetermined" or status == "queued_active" or status == "running":
    print "job " + repr(job1id) + " : " + jsc.status(job1id) 
    time.sleep(1)
    status = jsc.status(job1id)
  print "job " + repr(job1id) + " : " + jsc.status(job1id) 

  if status == "failed":
      sys.exit()


  job2id = submitWTjob2()
  print "job2 submitted \n"
  job3id = submitWTjob3()
  print "job3 submitted \n"

  #jsc.wait(job2id)
  status = jsc.status(job2id)
  while  status == "undetermined" or status == "queued_active" or status == "running":
    time.sleep(1)
    status = jsc.status(job2id)
  print "job " + repr(job2id) + " : " + jsc.status(job2id) 
  
  if status == "failed":
    sys.exit()
    
  #jsc.wait(job3id)
  status = jsc.status(job3id)
  while  status == "undetermined" or status == "queued_active" or status == "running":
    time.sleep(1)
    status = jsc.status(job3id)
  print "job " + repr(job3id) + " : " + jsc.status(job3id) 

  if status == "failed":
    sys.exit()


  job4id = submitWTjob4()

  #jsc.wait(job4id)
  status = jsc.status(job4id)
  while  status == "undetermined" or status == "queued_active" or status == "running":
    time.sleep(1)
    status = jsc.status(job4id)
  print "job " + repr(job4id) + " : " + jsc.status(job4id) 

  if status == "failed":
    sys.exit()


  delta = datetime.now()-startTime
  print "time: " + repr(delta.seconds) + " seconds."
  print "jobs : " + repr(jsc.jobs())

  #job_ids = jsc.jobs()
  #for job_id in job_ids:
    #print "job " + repr(job_id) + " : " + jsc.status(job_id) => pb because the jobs can be delete by other processes
  ft.transferOutputFile(l_file4)

  jsc.dispose(job1id)
  jsc.dispose(job2id)
  jsc.dispose(job3id)
  jsc.dispose(job4id)
  
  jsc.cancelTransfer(l_file0)
  jsc.cancelTransfer(l_file11) 
  jsc.cancelTransfer(l_file12)
  jsc.cancelTransfer(l_script1) 
  jsc.cancelTransfer(l_stdin1) 
  jsc.cancelTransfer(l_file2)  
  jsc.cancelTransfer(l_script2)
  jsc.cancelTransfer(l_stdin2)
  jsc.cancelTransfer(l_file3)
  jsc.cancelTransfer(l_script3)
  jsc.cancelTransfer(l_stdin3)
  jsc.cancelTransfer(l_file4)
  jsc.cancelTransfer(l_script4)
  jsc.cancelTransfer(l_stdin4)

  time.sleep(1)




##############################################################