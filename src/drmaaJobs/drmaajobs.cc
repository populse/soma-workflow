
#include <stdlib.h>
#include "drmaajobs.h"


DrmaaJobs::DrmaaJobs() {  
      
     m_currentId = 1;
//     mLogPath = NULL;
//     const char * pPath = getenv ("SOMA_JOBS_DRMAA_LOG");
//     if (pPath!=NULL) mLogPath = new std::string(pPath);    

}

DrmaaJobs::~DrmaaJobs() {

    char error[DRMAA_ERROR_STRING_BUFFER];
    int errnum = 0;

    for(std::map<int, drmaa_job_template_t *, ltint>::const_iterator i = mJobTemplatesMap.begin();i!=mJobTemplatesMap.end(); ++i) {
        //log("> before drmaa_delete_job_template");
        errnum = drmaa_delete_job_template(i->second, error, DRMAA_ERROR_STRING_BUFFER);
        //log("< after drmaa_delete_job_template");
        //if (errnum != DRMAA_ERRNO_SUCCESS) {
            //return 
            //fprintf (stderr, "Could not delete job template: %s\n", error);
            //log("Could not delete job template:" + std::string(error) + "\n");
        //}
    }
    mJobTemplatesMap.clear();

    exitSession();
    //log("<= Session ended");

}



void DrmaaJobs::displayJobTemplateAttributeValues(int jobTemplateId) {
    if(!isJobTemplateIdValid(jobTemplateId)) return;

    char error[DRMAA_ERROR_STRING_BUFFER];
    int errnum = 0;

    drmaa_attr_names_t * attributeNames;

    errnum = drmaa_get_attribute_names(&attributeNames,
                                       error,
                                       DRMAA_ERROR_STRING_BUFFER);

    if(errnum != DRMAA_ERRNO_SUCCESS ) {
        fprintf (stderr, "Could not get attribute names : %s\n", error);
        //log("Could not get attribute names :" + std::string(error) + "\n");
        return;
    }

    drmaa_job_template_t * drmaaJT = mJobTemplatesMap[jobTemplateId];

    printf("**** Job attributes for job template %d ****\n", jobTemplateId);
    char attributName[DRMAA_ERROR_STRING_BUFFER];
    while (drmaa_get_next_attr_name(attributeNames, attributName, DRMAA_ERROR_STRING_BUFFER) == DRMAA_ERRNO_SUCCESS)
    {
        char attributeValue [DRMAA_ERROR_STRING_BUFFER];
        errnum = drmaa_get_attribute (drmaaJT,
                                      attributName,
                                      attributeValue,
                                      DRMAA_ERROR_STRING_BUFFER,
                                      error,
                                      DRMAA_ERROR_STRING_BUFFER);
        if(errnum != DRMAA_ERRNO_SUCCESS ) {
            fprintf (stderr, "Job attribute \"%s\" : %s\n", attributName, error);
            //log("Job attribute :" + std::string(attributName) + " : " + std::string(error) + "\n");
        } else {
            printf ("Job attribute \"%s\" : \"%s\" \n", attributName, attributeValue);
        }
    }

    drmaa_release_attr_names(attributeNames);
    printf("************************\n");
}


void DrmaaJobs::displaySupportedAttributeNames() {
    char error[DRMAA_ERROR_STRING_BUFFER];
    int errnum = 0;

    drmaa_attr_names_t * attributeNames;

    errnum = drmaa_get_attribute_names(&attributeNames,
                                       error,
                                       DRMAA_ERROR_STRING_BUFFER);

    if(errnum != DRMAA_ERRNO_SUCCESS ) {
        fprintf (stderr, "Could not get attribute names : %s\n", error);
        //log("Could not get attribute names : " + std::string(error) + "\n");
        return;
    }

    printf("**** Supported job attributes ****\n");
    char attributName[DRMAA_ERROR_STRING_BUFFER];
    while (drmaa_get_next_attr_name(attributeNames, attributName, DRMAA_ERROR_STRING_BUFFER) == DRMAA_ERRNO_SUCCESS)
    {
        printf(" \"%s\" \n", attributName );
    }

    drmaa_release_attr_names(attributeNames);
    printf("************************\n");
}


void DrmaaJobs::initSession(const char * contactString) {
    char error[DRMAA_ERROR_STRING_BUFFER];
    int errnum = 0;

    if(std::string(contactString) == "NULL") {
        //log("> before drmaa_init");
        errnum = drmaa_init (NULL, error, DRMAA_ERROR_STRING_BUFFER);
        //log("< after drmaa_init");
    } else {
        char contact[DRMAA_CONTACT_BUFFER];
        //log("> before drmaa_init");
        errnum = drmaa_init (contact, error, DRMAA_ERROR_STRING_BUFFER);
        //log("< after drmaa_init");
    }

    if (errnum != DRMAA_ERRNO_SUCCESS) {
      throw DrmaaError(std::string(error)); 
    }
    
//     char contact[DRMAA_CONTACT_BUFFER];
//     log("> before drmaa_get_contact");
//     errnum = drmaa_get_contact(contact, DRMAA_CONTACT_BUFFER, error, DRMAA_ERROR_STRING_BUFFER);
//     log("> after drmaa_get_contact");
// 
//     if(errnum != DRMAA_ERRNO_SUCCESS) {
//         fprintf(stderr, "Could not get the contact information string: %s\n", error);
//         log("Could not get the contact information string:" + std::string(error) + "\n");
//         return;
//     }
//     
//     log("=> New session");

    //printf("Contact information: %s\n", contact);
}


void DrmaaJobs::exitSession() {
    char error[DRMAA_ERROR_STRING_BUFFER];
    int errnum = 0;

    //log("> before drmaa_exit");
    errnum = drmaa_exit (error, DRMAA_ERROR_STRING_BUFFER);
    //log("< after drmaa_exit");

    if (errnum != DRMAA_ERRNO_SUCCESS) {
       fprintf (stderr, "Could not shut down the DRMAA library: %s\n", error);
       //log("Could not shut down the DRMAA library: " + std::string(error) + "\n");
       return;
    }

    //printf("The DRMAA library was shut down \n");

}

int DrmaaJobs::allocateJobTemplate() {
    char error[DRMAA_ERROR_STRING_BUFFER];
    int errnum = 0;
    drmaa_job_template_t * drmaaJT;

    //log("> before drmaa_allocate_job_template");
    errnum = drmaa_allocate_job_template (&drmaaJT, error, DRMAA_ERROR_STRING_BUFFER);
    //log("< after drmaa_allocate_job_template");

    if (errnum != DRMAA_ERRNO_SUCCESS) {
       //fprintf (stderr, "Could not create job template: %s\n", error);
       //log("Could not create job template: " + std::string(error) + "\n");
       //return undefinedId;
       throw DrmaaError("Could not create job template: " + std::string(error));
    }

    int jobTemplateId = getNextId();
    mJobTemplatesMap[jobTemplateId] = drmaaJT;
    return jobTemplateId;
}

void DrmaaJobs::deleteJobTemplate(int jobTemplateId) {
    if(!isJobTemplateIdValid(jobTemplateId)) return;

   char error[DRMAA_ERROR_STRING_BUFFER];
   int errnum = 0;
   drmaa_job_template_t * drmaaJT = mJobTemplatesMap[jobTemplateId];

   //log("> before drmaa_delete_job_template");
   errnum = drmaa_delete_job_template (drmaaJT, error, DRMAA_ERROR_STRING_BUFFER);
   //log("< after drmaa_delete_job_template");

//    if (errnum != DRMAA_ERRNO_SUCCESS) {
//        //fprintf (stderr, "Could not delete job template: %s\n", error);
//        //log("Could not create job template: " + std::string(error) + "\n");
//    
//    }
//    else {
        mJobTemplatesMap.erase(jobTemplateId);
//    }

}


void DrmaaJobs::setAttribute(int jobTemplateId, const char *name, const char *value) {
    if(!isJobTemplateIdValid(jobTemplateId)) return;

    char error[DRMAA_ERROR_STRING_BUFFER];
    int errnum = 0;
    drmaa_job_template_t * drmaaJT = mJobTemplatesMap[jobTemplateId];

    //log("> before drmaa_set_attribute");
    errnum = drmaa_set_attribute (drmaaJT, name, value,
                                  error, DRMAA_ERROR_STRING_BUFFER);
    //log("> after drmaa_set_attribute");

    if (errnum != DRMAA_ERRNO_SUCCESS) {
        //fprintf (stderr, "Could not set attribute \"%s\": %s\n",
        //         name, error);
        //log("Could not set attribute " + std::string(name) + " : " + std::string(error) + "\n");
        throw DrmaaError("Could not set the attribute " + std::string(name) + " : " + std::string(error));
    }
}


void DrmaaJobs::setVectorAttribute(int jobTemplateId, const char* name, int nbArguments, const char ** arguments) {
    if(!isJobTemplateIdValid(jobTemplateId)) return;

    char error[DRMAA_ERROR_STRING_BUFFER];
    int errnum = 0;
    drmaa_job_template_t * drmaaJT = mJobTemplatesMap[jobTemplateId];


    const char ** args = new const char* [nbArguments+1];

    //printf("DrmaaJobs::setVectorAttribute: nbArguments = %d \n", nbArguments);
    //log("DrmaaJobs::setVectorAttribute " + std::string(name) + " : \n");
    for(int i = 0 ; i < nbArguments ; i++) {
        args[i] = arguments[i];
        //log(std::string(args[i]));
    }
    args[nbArguments] = NULL;

    //log("> before drmaa_set_vector_attribute");
    errnum = drmaa_set_vector_attribute (drmaaJT, name, args,
                                   error, DRMAA_ERROR_STRING_BUFFER);
    //log("< after drmaa_set_vector_attribute");

    if (errnum != DRMAA_ERRNO_SUCCESS) {
        //fprintf (stderr, "Could not set attribute \"%s\": %s\n",
        //        name, error);
        //log("Could not set attribute " + std::string(name) + " : " + std::string(error) + "\n");
        throw DrmaaError("Could not set the vector attribute " + std::string(name) + " : " + std::string(error));
    }

    delete[] args;

}


void DrmaaJobs::setCommand(int jobTemplateId, const char * remote_command, int nbArguments, const char ** arguments) {
    if(!isJobTemplateIdValid(jobTemplateId)) return;

    //printf("DrmaaJobs::setCommand remote_command = %s, nbArguments = %d \n", remote_command, nbArguments);
    //for(int i = 0; i < nbArguments ; i++) {
        //printf("DrmaaJobs::setCommand: argument num %d = %s \n", i, arguments[i]);
    //}

    setAttribute(jobTemplateId, DRMAA_REMOTE_COMMAND, remote_command);

    //const char * args[100];
    const char ** args = new const char* [nbArguments+1];

    for(int i = 0 ; i < nbArguments ; i++) {
        args[i] = arguments[i];
    }
    args[nbArguments] = NULL;

    char error[DRMAA_ERROR_STRING_BUFFER];
    int errnum = 0;
    drmaa_job_template_t * drmaaJT = mJobTemplatesMap[jobTemplateId];

    //log("> before drmaa_set_vector_attribute");
    errnum = drmaa_set_vector_attribute (drmaaJT, DRMAA_V_ARGV, args,
                                   error, DRMAA_ERROR_STRING_BUFFER);
    //log("< after drmaa_set_vector_attribute");

    if (errnum != DRMAA_ERRNO_SUCCESS) {
        //fprintf (stderr, "Could not set attribute \"%s\": %s\n",
        //         DRMAA_V_ARGV, error);
        //log("Could not set attribute " + std::string(DRMAA_V_ARGV) + " : " + std::string(error) + "\n");
        throw DrmaaError("Could not set the command: " + std::string(error));
    }

    delete[] args;
}


const std::string DrmaaJobs::runJob(int jobTemplateId) {

    //log("1");
    std::string submittedJobId = "";
    if(!isJobTemplateIdValid(jobTemplateId)) return submittedJobId;
    //log("2");
    char error[DRMAA_ERROR_STRING_BUFFER];
    int errnum = 0;
    drmaa_job_template_t * drmaaJT = mJobTemplatesMap[jobTemplateId];
    char jobid[DRMAA_JOBNAME_BUFFER];

    //log("> before drmaa_run_job");
    errnum = drmaa_run_job (jobid,
                            DRMAA_JOBNAME_BUFFER,
                            drmaaJT,
                            error,
                            DRMAA_ERROR_STRING_BUFFER);
    //log("< after drmaa_run_job");
    //log("3");

    if (errnum != DRMAA_ERRNO_SUCCESS) {
        //fprintf (stderr, "Could not submit job: %s\n", error);
        //log("Could not submit job: " + std::string(error) + "\n");
        throw DrmaaError("Could not submit job: " + std::string(error));
    }

    //log("4");
    submittedJobId = jobid;
    //printf("Your job has been submitted with id %s\n", jobid);
    //log("Your job has been submitted with id "+ std::string(jobid));
    return submittedJobId;
}

void DrmaaJobs::runBulkJobs(int jobTemplateId, int nbJobs, std::list<std::string> & submittedJobIds_out) {
    if(!isJobTemplateIdValid(jobTemplateId)) return;

    char error[DRMAA_ERROR_STRING_BUFFER];
    int errnum = 0;
    drmaa_job_template_t * drmaaJT = mJobTemplatesMap[jobTemplateId];

    drmaa_job_ids_t *ids = NULL;

    //log("> before drmaa_run_bulk_jobs");
    errnum = drmaa_run_bulk_jobs (&ids,
                                  drmaaJT,
                                  1,
                                  nbJobs,
                                  1,
                                  error,
                                  DRMAA_ERROR_STRING_BUFFER);
    //log("< after drmaa_run_bulk_jobs");

    if (errnum != DRMAA_ERRNO_SUCCESS) {
        //fprintf (stderr, "Could not submit job: %s\n", error);
        //log("Could not submit job: " + std::string(error) + "\n");
        drmaa_release_job_ids(ids);
        //return;
        throw DrmaaError("Could not suspend job: " + std::string(error));
    }

    submittedJobIds_out.clear();
    char jobid[DRMAA_JOBNAME_BUFFER];
    while (drmaa_get_next_job_id(ids, jobid, DRMAA_JOBNAME_BUFFER) == DRMAA_ERRNO_SUCCESS) {
        submittedJobIds_out.push_back(jobid);
        printf ("A job has been submitted with id %s\n", jobid);
    }
    drmaa_release_job_ids (ids);
}

DrmaaJobs::ExitJobInfo DrmaaJobs::wait(const std::string & submittedJobId, int timeout) {

    //log(" => => wait...");

    ExitJobInfo jobInfo;

    char error[DRMAA_ERROR_STRING_BUFFER];
    int errnum = 0;

    const char* jobid = submittedJobId.c_str();

    char jobid_out[DRMAA_JOBNAME_BUFFER];
    int status = 0;
    drmaa_attr_values_t *rusage = NULL;

    if ( timeout < 0 ) 
      timeout = DRMAA_TIMEOUT_WAIT_FOREVER;
    else if (timeout == 0 )
      timeout = DRMAA_TIMEOUT_NO_WAIT;
    
    //log("> before drmaa_wait job " + submittedJobId);
    errnum = drmaa_wait (jobid,
                          jobid_out,
                          DRMAA_JOBNAME_BUFFER,
                          &status,
                          timeout,
                          &rusage,
                          error,
                          DRMAA_ERROR_STRING_BUFFER);
    //log("< after drmaa_wait job " + submittedJobId);

    if (errnum != DRMAA_ERRNO_SUCCESS) {
        //fprintf (stderr, "Could not wait for job: %s\n", error);
        //log("Could not wait for job: " + std::string(error) + "\n");
        jobInfo.status = EXIT_UNDETERMINED;
        return jobInfo;
    }

    ////////////////////////////////////////////////////////
    //printf(" Job %s status: ", jobid);
    //log(" Job " + submittedJobId + " status :");

    int aborted = 0;
    //log("   >>Before drmaa_wifaborted");
    drmaa_wifaborted(&aborted, status, NULL, 0);
    //log("   <<After drmaa_wifaborted");

    if (aborted == 1) {
        //printf("Job never ran\n");
        //log("Job never ran");
        jobInfo.status = EXIT_ABORTED;
    } else {
      int exited = 0;
      //log("   >>Before drmaa_wifexited");
      drmaa_wifexited(&exited, status, NULL, 0);
      //log("   <<After drmaa_wifexited");
      if (exited == 1) {
        int exit_status = 0;
        //log("   >>Before drmaa_wexitstatus");
        drmaa_wexitstatus(&exit_status, status, NULL, 0);
        //log("   <<After drmaa_wexitstatus");
        //printf("Job finished regularly with exit status %d\n", exit_status);
        //log("Job finished regularly");
        jobInfo.status = FINISHED_REGULARLY;
        //log("111");
        jobInfo.exitValue = exit_status; 
        //log("222");
      } else {  
        int signaled = 0;
        drmaa_wifsignaled(&signaled, status, NULL, 0);
        if (signaled == 1) {
            char termsig[DRMAA_SIGNAL_BUFFER+1];
            //log("   >>Before drmaa_wtermsig");
            drmaa_wtermsig(termsig, DRMAA_SIGNAL_BUFFER, status, NULL, 0);
            //log("   <<After drmaa_wtermsig");
            //printf("Job finished due to signal %s\n", termsig);
            //log("Job finished due to signal");
            jobInfo.status = FINISHED_TERM_SIG;
            jobInfo.termSignal = termsig;
        } else {  
          //printf("Job finished with unclear conditions\n");
          //log("Job finished with unclear conditions");
          jobInfo.status = FINISHED_UNCLEAR_CONDITIONS;
        }
      }
    }

    ////////////////////////////////////////////////////////////
    //printf("~~~ Job %s Usage: ~~~~~~~~ \n", jobid);
    char usage[DRMAA_ERROR_STRING_BUFFER];
    while (drmaa_get_next_attr_value (rusage, usage, DRMAA_ERROR_STRING_BUFFER) == DRMAA_ERRNO_SUCCESS) {
        //printf ("  %s\n", usage);
        jobInfo.resourceUsage.push_back(usage);
    }
    //printf("~~~~~~~~~~~~~~~~~~~~~~~~~ \n");

    //log("   >>Before drmaa_release_attr_values");
    drmaa_release_attr_values (rusage);
    //log("   <<After drmaa_release_attr_values");

    //log(" <= <= End of wait" );
    return jobInfo;
}


void DrmaaJobs::synchronize(const std::list<std::string> & submittedJobIds, int timeout) {
    char error[DRMAA_ERROR_STRING_BUFFER];
    int errnum = 0;

    //const char *jobids[100];
    const char ** jobids = new const char* [submittedJobIds.size()+1];
    int index=0;
    for(std::list<std::string>::const_iterator i = submittedJobIds.begin();i!=submittedJobIds.end(); ++i) {
        jobids[index] = (*i).c_str();
        index++;
    }
    jobids[submittedJobIds.size()] = NULL;
    
    if ( timeout < 0 ) 
      timeout = DRMAA_TIMEOUT_WAIT_FOREVER;
    else if (timeout == 0 )
      timeout = DRMAA_TIMEOUT_NO_WAIT;
    
    //log("> before drmaa_synchronize");
    errnum = drmaa_synchronize (jobids,
                                timeout,
                                0,//means that we keep the jobs information available and will get and delete it later (inside wait())
                                error,
                                DRMAA_ERROR_STRING_BUFFER);
    //log("< after drmaa_synchronize");

    delete[] jobids;

    if (errnum != DRMAA_ERRNO_SUCCESS) {
        fprintf (stderr, "Could not wait for jobs: %s\n", error);
        //log("Could not wait for job: " + std::string(error) + "\n");
    }
    //else 
        //printf ("Job tasks have finished.\n");

}






void DrmaaJobs::control(const std::string & submittedJobId, Action action) {

    int drmaa_action = -1;
    switch (action) {
    case SUSPEND:
        drmaa_action = DRMAA_CONTROL_SUSPEND;
        break;
    case RESUME:
        drmaa_action = DRMAA_CONTROL_RESUME;
        break;
    case HOLD:
        drmaa_action = DRMAA_CONTROL_HOLD;
        break;
    case RELEASE:
        drmaa_action = DRMAA_CONTROL_RELEASE;
        break;
    case TERMINATE:
        drmaa_action = DRMAA_CONTROL_TERMINATE;
        //log("terminate job " + std::string(submittedJobId));
        break;
    }

    char error[DRMAA_ERROR_STRING_BUFFER];
    int errnum = 0;

    const char * jobId = submittedJobId.c_str();
    //log("> before drmaa_control");
    errnum = drmaa_control(jobId,
                           drmaa_action,
                           error,
                           DRMAA_ERROR_STRING_BUFFER);
    //log("< after drmaa_control");

    if(errnum != DRMAA_ERRNO_SUCCESS) {
        //fprintf (stderr, "Could not control the job: %s\n", error);
        switch (action) {
        case SUSPEND:
          throw DrmaaError("Could not suspend job: " + std::string(error));
        case RESUME:
          throw DrmaaError("Could not resume job: " + std::string(error));
        case HOLD:
          throw DrmaaError("Could not hold job: " + std::string(error));
        case RELEASE:
          throw DrmaaError("Could not release job: " + std::string(error));
        case TERMINATE:
          throw DrmaaError("Could not terminate job: " + std::string(error));
        }
   }

}


bool DrmaaJobs::isJobTemplateIdValid(int jobTemplateId) {
    if(mJobTemplatesMap.count(jobTemplateId) == 0) {
        fprintf (stderr, "Job template with id %d doesn't exit\n", jobTemplateId);
        return false;
    } else return true;
}



int DrmaaJobs::getNextId() {
    return m_currentId++;
}


DrmaaJobs::JobStatus DrmaaJobs::jobStatus(const std::string & submittedJobId) {

    //printf("drmaa job id %s \n", submittedJobId.c_str());

    char error[DRMAA_ERROR_STRING_BUFFER];
    int errnum = 0;

    int status = 0;
    const char* jobid = submittedJobId.c_str();
    //log("=> before drmaa_job_ps");
    errnum = drmaa_job_ps(jobid,
                          &status,
                          error,
                          DRMAA_ERROR_STRING_BUFFER);
    //log("<= after drmaa_job_ps");

    if (errnum != DRMAA_ERRNO_SUCCESS) {
        //log("Could not get job status");
        //fprintf (stderr, "Could not get job' status: %s\n", error);
        //return UNDETERMINED;
      throw DrmaaError("Could not get the job status: " + std::string(error));  
    }

    //printf("job %s ", submittedJobId.c_str());

    switch (status) {
    case DRMAA_PS_UNDETERMINED:
        //printf ("Job status cannot be determined\n");
        return UNDETERMINED;
    case DRMAA_PS_QUEUED_ACTIVE:
        //printf ("Job is queued and active\n");
        return QUEUED_ACTIVE;
    case DRMAA_PS_SYSTEM_ON_HOLD:
        //printf ("Job is queued and in system hold\n");
        return SYSTEM_ON_HOLD;
    case DRMAA_PS_USER_ON_HOLD:
        //printf ("Job is queued and in user hold\n");
        return USER_ON_HOLD;
    case DRMAA_PS_USER_SYSTEM_ON_HOLD:
        //printf ("Job is queued and in user and system hold\n");
        return USER_SYSTEM_ON_HOLD;
    case DRMAA_PS_RUNNING:
        //printf ("Job is running\n");
        return RUNNING;
    case DRMAA_PS_SYSTEM_SUSPENDED:
        //printf ("Job is system suspended\n");
        return SYSTEM_SUSPENDED;
    case DRMAA_PS_USER_SUSPENDED:
        //printf ("Job is user suspended\n");
        return USER_SUSPENDED;
    case DRMAA_PS_USER_SYSTEM_SUSPENDED:
        //printf ("Job is user and system suspended\n");
        return USER_SYSTEM_SUSPENDED;
    case DRMAA_PS_DONE:
        //printf ("Job finished normally\n");
        return DONE;
    case DRMAA_PS_FAILED:
        //printf ("Job finished, but failed\n");
        return FAILED;
    }

}


// void DrmaaJobs::log(std::string msg) {
//    
//     const char * logPath = getenv ("SOMA_JOBS_DRMAA_LOG");
//     if(logPath != NULL) {
//       std::ofstream logFile;
//       logFile.open(logPath, std::ios::out | std::ios::app);
//       logFile << msg << std::endl;
//       logFile.flush();
//       logFile.close();
//     }


//     if(mLogPath == NULL) return;
// 
//     std::ofstream logFile;
//     logFile.open(mLogPath->c_str(), std::ios::out | std::ios::app);
//     logFile << msg << std::endl;
//     logFile.flush();
//     logFile.close();

//}


