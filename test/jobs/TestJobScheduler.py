import sys
from datetime import datetime
#import Pyro.naming, Pyro.core
#from Pyro.errors import NamingError
'''
2 modes:
@type  mode: 'local' or 'remote'
@param mode: 
  'local': (default) if run on a submitting machine of the pool
  'remote': if run from a machine whichis not a submitting machine of the pool 
  and doesn't share a file system with these machines
'''

mode = 'test'

if mode == 'local':
  from soma.jobs.jobScheduler import JobScheduler
  from soma.jobs.fileTransfer import LocalFileTransfer
  jsc = JobScheduler()
  ft = LocalFileTransfer(jsc)
  inpath = "/home/sl225510/projets/jobExamples/complete/"
  outpath = "/home/sl225510/"
  
if mode == 'remote':
  import remoteJobScheduler 
  from fileTransfer import RemoteFileTransfer
  import sys
  import getpass
  
  sys.stdout.write("login: ")
  _login = "sl225510" 
  print _login
  _password = getpass.getpass()
  
  jsc = remoteJobScheduler.getJobScheduler(_login, _password)
  ft = RemoteFileTransfer(jsc)
  inpath = "/home/laguitton/jobExamples/complete/"
  outpath = "/home/laguitton/"


if mode == 'test':
  from soma.jobs.fileTransfer import RemoteFileTransfer
  from soma.jobs.jobScheduler import JobScheduler
  jsc = JobScheduler()
  ft = RemoteFileTransfer(jsc)
  inpath = "/home/sl225510/projets/jobExamples/complete/"
  outpath = "/home/sl225510/"


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

def submitWTjob1(jobScheduler, fileTransfer):
  global script1, stdin1, file0, file11, file12
  global l_file0, l_file11, l_file12, l_script1, l_stdin1, job1id
  
  l_file0 = fileTransfer.transferInputFile(file0, -24) 

  l_file11 = jobScheduler.registerTransfer(file11, -24) 
  l_file12 = jobScheduler.registerTransfer(file12, -24) 

  l_script1 = fileTransfer.transferInputFile(script1, -24) 
  l_stdin1 = fileTransfer.transferInputFile(stdin1, -24) 

  job1id = jobScheduler.submitWithTransfer( [python, l_script1, l_file0, l_file11, l_file12], 
                                   [l_file0, l_script1, l_stdin1], 
                                   [l_file11, l_file12], 
                                   True, l_stdin1, 1) 

  return job1id

def submitWTjob2(jobScheduler, fileTransfer):
  global script2, stdin2, file2
  global l_file0, l_file11
  global l_file2, l_script2, l_stdin2, job2id
  
  l_file2 = jobScheduler.registerTransfer(file2, -24) 

  print "l_file2: " + l_file2

  l_script2 = fileTransfer.transferInputFile(script2, -24) 
  l_stdin2 = fileTransfer.transferInputFile(stdin2, -24) 

  job2id = jobScheduler.submitWithTransfer( [python, l_script2, l_file11, l_file0, l_file2, "15"], 
                                   [l_file0, l_file11, l_script2, l_stdin2], 
                                   [l_file2], 
                                   True, l_stdin2, 1) 

  return job2id

def submitWTjob3(jobScheduler, fileTransfer):
  global script3, stdin3, file3
  global l_file12
  global l_file3, l_script3, l_stdin3, job3id
  
  l_file3 = jobScheduler.registerTransfer(file3, -24) 

  l_script3 = fileTransfer.transferInputFile(script3, -24) 
  l_stdin3 = fileTransfer.transferInputFile(stdin3, -24) 

  job3id = jobScheduler.submitWithTransfer( [python, l_script3, l_file12, l_file3, "15"], 
                                   [l_file12, l_script3, l_stdin3], 
                                   [l_file3], 
                                   True, l_stdin3, 1) 

  return job3id

def submitWTjob4(jobScheduler, fileTransfer):
  global script4, stdin4, file4
  global l_file2, l_file3
  global l_file4, l_script4, l_stdin4, job4id
  
  l_file4 = jobScheduler.registerTransfer(file4, -24) 

  l_script4 = fileTransfer.transferInputFile(script4, -24) 
  l_stdin4 = fileTransfer.transferInputFile(stdin4, -24) 

  job4id = jobScheduler.submitWithTransfer( [python, l_script4, l_file2, l_file3, l_file4], 
                                   [l_file2, l_file3, l_script4, l_stdin4], 
                                   [l_file4], 
                                   True, l_stdin4, 1) 

  return job4id





#startTime = datetime.now()

#job1id = submitWTjob1()
#jsc.wait(job1id)

#job2id = submitWTjob2()
#job3id = submitWTjob3()
#jsc.wait(job2id)
#jsc.wait(job3id)

#job4id = submitWTjob4()
#jsc.wait(job4id)

#delta = datetime.now()-startTime
#print "time: " + repr(delta.seconds) + " seconds."


#jsc.dispose(job1id)
#jsc.dispose(job2id)
#jsc.dispose(job3id)
#jsc.dispose(job4id)





##############################################################