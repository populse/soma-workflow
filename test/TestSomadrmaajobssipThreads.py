from soma.pipeline.somadrmaajobssip import DrmaaJobs
import time
import threading
from datetime import datetime

drmaajobs = DrmaaJobs()

path = "/home/soizic/projets/jobExamples/complete/"
outputPath = "/home/soizic/output/"

jobTemplateId = drmaajobs.allocateJobTemplate()
drmaajobs.setCommand(jobTemplateId, 
                     "python", 
                     [path + "job1.py", 
                      path + "file0",
                      outputPath + "file11",
                      outputPath + "file12",
                     "15"])
drmaajobs.setAttribute(jobTemplateId,"drmaa_input_path", "[void]:" + path + "stdin1")
drmaajobs.setAttribute(jobTemplateId,"drmaa_output_path", "[void]:" + outputPath + "stdoutjob1")
drmaajobs.setAttribute(jobTemplateId,"drmaa_error_path", "[void]:" + outputPath + "stderrjob1")
drmaajobs.setAttribute(jobTemplateId,"drmaa_join_files", "y")

jobIds = []
jobId = drmaajobs.runJob(jobTemplateId)
jobIds.append(jobId)
time.sleep(4)
jobId = drmaajobs.runJob(jobTemplateId)
jobIds.append(jobId)
time.sleep(4)
jobId = drmaajobs.runJob(jobTemplateId)
jobIds.append(jobId)
print jobIds

def printJobStatus():
  for jobid in jobIds:
    status = drmaajobs.jobStatus(jobid);
    print('job: ', jobid, 'status', status);
    
    
def printJobInfo(jobInfoTuple):
  exitStatus, returned_value, term_sig, ressource_usage = jobInfoTuple
  print "~~~~~~~~~~~~~~~~~~~~~~~~~~~"
  print "exitStatus = " + exitStatus
  print "returnedValue = " + repr(returned_value)
  print "term_sig = " + repr(term_sig)
  print "ressource_usage " + repr(ressource_usage)


def waitThread():
  startTime = datetime.now()
  #printJobStatus()
  #print "   "
  #print "   "
  #time.sleep(2)
  #jobInfoTuple = drmaajobs.wait(jobIds[0], 0)
  #print "   "
  #print "   "
  #printJobInfo(jobInfoTuple)
  #print "   "
  #print "   "
  #printJobStatus()
  #print "   "
  #print "   "
 
  jobInfoTuple = drmaajobs.wait(jobIds[1])
  print "   "
  print "   "
  printJobInfo(jobInfoTuple)
  delta = datetime.now()-startTime
  print "END WAIT !!!! time: " + repr(delta.seconds) + " seconds."
  
  

  ##print "jobs : " + repr(jsc.jobs())


#waitThread = threading.Thread(name = "wait thread",
                              #target = waitThread)
#waitThread.setDaemon(True)
#waitThread.start()

#printJobStatus()
#startTime = datetime.now()
#drmaajobs.synchronize(jobIds)
#delta = datetime.now()-startTime
#print "END SYNCHRONIZE !!! time: " + repr(delta.seconds) + " seconds."
#printJobStatus()

time.sleep(2)
drmaajobs.terminate(jobIds[1])
printJobStatus()

time.sleep(2)
drmaajobs.terminate(jobIds[1])
printJobStatus()




