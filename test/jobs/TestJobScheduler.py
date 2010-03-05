from soma.jobs.jobScheduler import JobScheduler
from datetime import datetime

path = "/home/sl225510/projets/jobExamples/complete/"
#job1########################################################
global script1, stdin1, file0, file11, file12
global l_script1, l_stdin1, l_file0, l_file11, l_file12, job1id

#input files
script1 = path + "job1.py"
stdin1  = path + "stdin1"
file0   = path + "file0"

#output files
file11 = path + "file11"
file12 = path + "file12"


#job2########################################################
global script2, stdin2, file2
global l_script2, l_stdin2, l_file2, job2id
#input files
script2 = path + "job2.py"
stdin2  = path + "stdin2"
#file0
#file11

#output files
file2 = path + "file2"

#job3########################################################
global script3, stdin3, file3
global l_script3, l_stdin3, l_file3, job3id
#input files
script3 = path + "job3.py"
stdin3  = path + "stdin3"
#file12

#output files
file3 = path + "file3"

#job4########################################################
global script4, stdin4, file4
global l_script4, l_stdin4, l_file4, job4id
#input files
script4 = path + "job4.py"
stdin4  = path + "stdin4"
#file2
#file4

#output files
file4 = path + "file4"


jsc = JobScheduler()

#CUSTOM SUBMISSION#############################################
#jobId = jsc.customSubmit(["python", script1, file0, file11, file12], 
                          #"/home/sl225510/", 
                          #"/home/sl225510/stdoutjob1", 
                          #None,
                          #l_stdin1,
                          #1)
                          
path2 = "/home/sl225510/projets/jobExamples/simple/"
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

def submitWTjob1():
  global script1, stdin1, file0, file11, file12
  global l_file0, l_file11, l_file12, l_script1, l_stdin1, job1id
  
  l_file0 = jsc.transferInputFile(file0, -24) 

  l_file11 = jsc.allocateLocalOutputFile(file11, -24) 
  l_file12 = jsc.allocateLocalOutputFile(file12, -24) 

  l_script1 = jsc.transferInputFile(script1, -24) 
  l_stdin1 = jsc.transferInputFile(stdin1, -24) 

  job1id = jsc.submitWithTransfer( ["python", l_script1, l_file0, l_file11, l_file12], 
                                   [l_file0, l_script1, l_stdin1], 
                                   [l_file11, l_file12], 
                                   True, l_stdin1, 1) 

  return job1id

def submitWTjob2():
  global script2, stdin2, file2
  global l_file0, l_file11
  global l_file2, l_script2, l_stdin2, job2id
  
  l_file2 = jsc.allocateLocalOutputFile(file2, -24) 

  print "l_file2: " + l_file2

  l_script2 = jsc.transferInputFile(script2, -24) 
  l_stdin2 = jsc.transferInputFile(stdin2, -24) 

  job2id = jsc.submitWithTransfer( ["python", l_script2, l_file11, l_file0, l_file2, "15"], 
                                   [l_file0, l_file11, l_script2, l_stdin2], 
                                   [l_file2], 
                                   True, l_stdin2, 1) 

  return job2id

def submitWTjob3():
  global script3, stdin3, file3
  global l_file12
  global l_file3, l_script3, l_stdin3, job3id
  
  l_file3 = jsc.allocateLocalOutputFile(file3, -24) 

  l_script3 = jsc.transferInputFile(script3, -24) 
  l_stdin3 = jsc.transferInputFile(stdin3, -24) 

  job3id = jsc.submitWithTransfer( ["python", l_script3, l_file12, l_file3, "15"], 
                                   [l_file12, l_script3, l_stdin3], 
                                   [l_file3], 
                                   True, l_stdin3, 1) 

  return job3id

def submitWTjob4():
  global script4, stdin4, file4
  global l_file2, l_file3
  global l_file4, l_script4, l_stdin4, job4id
  
  l_file4 = jsc.allocateLocalOutputFile(file4, -24) 

  l_script4 = jsc.transferInputFile(script4, -24) 
  l_stdin4 = jsc.transferInputFile(stdin4, -24) 

  job4id = jsc.submitWithTransfer( ["python", l_script4, l_file2, l_file3, l_file4], 
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