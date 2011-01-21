from soma.workflow.somadrmaajobssip import DrmaaJobs
import time

drmaajobs = DrmaaJobs()


jobTemplateId = drmaajobs.allocateJobTemplate()
drmaajobs.setCommand(jobTemplateId, 
                     "/i2bm/research/Mandriva-2008.0-i686/bin/python", 
                     ["/home/sl225510/projets/jobExamples/complete/job1.py", 
                     "/home/sl225510/projets/jobExamples/complete/file0",
                     "/home/sl225510/projets/jobExamples/complete/file11",
                     "/home/sl225510/projets/jobExamples/complete/file12",
                     "15"])
drmaajobs.setAttribute(jobTemplateId,"drmaa_input_path", "[void]:/home/sl225510/projets/jobExamples/complete/stdin1")
drmaajobs.setAttribute(jobTemplateId,"drmaa_output_path", "[void]:/home/sl225510/stdoutjob1")
drmaajobs.setAttribute(jobTemplateId,"drmaa_error_path", "[void]:/home/sl225510/stderrjob1")
drmaajobs.setAttribute(jobTemplateId,"drmaa_join_files", "n")

jobIds = []
jobId = drmaajobs.runJob(jobTemplateId)
jobIds.append(jobId)
print jobIds

def printJobStatus():
	for jobid in jobIds:
		status = drmaajobs.jobStatus(jobid);
		print('job: ', jobid, 'status', status);

printJobStatus()

time.sleep(2)

printJobStatus()

time.sleep(10)

printJobStatus()

exitStatus, returned_value, term_sig, resource_usage = drmaajobs.wait(jobIds[0], 0)

print "~~~~~~~~~~~~~~~~~~~~~~~~~~~"
print "exitStatus = " + exitStatus
print "returnedValue = " + repr(returned_value)
print "term_sig = " + repr(term_sig)
print "resource_usage " + repr(resource_usage)

printJobStatus()

exitStatus, returned_value, term_sig, resource_usage = drmaajobs.wait(jobIds[0])

print "~~~~~~~~~~~~~~~~~~~~~~~~~~~"
print "exitStatus = " + exitStatus
print "returnedValue = " + repr(returned_value)
print "term_sig = " + repr(term_sig)
print "resource_usage " + repr(resource_usage)

##drmaajobs.synchronize(jobIds)

#printJobStatus()
