

#include "drmaajobs.h"


DrmaaJobs::DrmaaJobs(const char * contactString)
    : m_currentId(1) {
    initSession(contactString);
}

DrmaaJobs::~DrmaaJobs() {

    char error[DRMAA_ERROR_STRING_BUFFER];
    int errnum = 0;

    for(std::map<int, drmaa_job_template_t *, ltint>::const_iterator i = mJobTemplatesMap.begin();i!=mJobTemplatesMap.end(); ++i) {
        errnum = drmaa_delete_job_template (i->second, error, DRMAA_ERROR_STRING_BUFFER);
        if (errnum != DRMAA_ERRNO_SUCCESS) {
            fprintf (stderr, "Could not delete job template: %s\n", error);
        }
    }
    mJobTemplatesMap.clear();

    exitSession();
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
        //printf("case 1 \n");
        errnum = drmaa_init (NULL, error, DRMAA_ERROR_STRING_BUFFER);
    } else {
        //printf("case 2 \n");
        char contact[DRMAA_CONTACT_BUFFER];
        errnum = drmaa_init (contact, error, DRMAA_ERROR_STRING_BUFFER);
    }

    if (errnum != DRMAA_ERRNO_SUCCESS) {
        fprintf (stderr, "Could not initialize the DRMAA library: %s\n", error);
        return;
    }

    printf("DRMAA library was started successfully\n");

    char contact[DRMAA_CONTACT_BUFFER];
    errnum = drmaa_get_contact(contact, DRMAA_CONTACT_BUFFER, error, DRMAA_ERROR_STRING_BUFFER);

    if(errnum != DRMAA_ERRNO_SUCCESS) {
        fprintf(stderr, "Could not get the contact information string: %s\n", error);
        return;
    }

    printf("Contact information: %s\n", contact);
}


void DrmaaJobs::exitSession() {
    char error[DRMAA_ERROR_STRING_BUFFER];
    int errnum = 0;

    errnum = drmaa_exit (error, DRMAA_ERROR_STRING_BUFFER);

    if (errnum != DRMAA_ERRNO_SUCCESS) {
       fprintf (stderr, "Could not shut down the DRMAA library: %s\n", error);
       return;
    }

    printf("The DRMAA library was shut down \n");

}

int DrmaaJobs::allocateJobTemplate() {
    char error[DRMAA_ERROR_STRING_BUFFER];
    int errnum = 0;
    drmaa_job_template_t * drmaaJT;

    errnum = drmaa_allocate_job_template (&drmaaJT, error, DRMAA_ERROR_STRING_BUFFER);

    if (errnum != DRMAA_ERRNO_SUCCESS) {
       fprintf (stderr, "Could not create job template: %s\n", error);
       return undefinedId;
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

   errnum = drmaa_delete_job_template (drmaaJT, error, DRMAA_ERROR_STRING_BUFFER);

   if (errnum != DRMAA_ERRNO_SUCCESS) {
       fprintf (stderr, "Could not delete job template: %s\n", error);
   }
   else {
        mJobTemplatesMap.erase(jobTemplateId);
   }

}


void DrmaaJobs::setAttribute(int jobTemplateId, const char *name, const char *value) {
    if(!isJobTemplateIdValid(jobTemplateId)) return;

    char error[DRMAA_ERROR_STRING_BUFFER];
    int errnum = 0;
    drmaa_job_template_t * drmaaJT = mJobTemplatesMap[jobTemplateId];

    errnum = drmaa_set_attribute (drmaaJT, name, value,
                                  error, DRMAA_ERROR_STRING_BUFFER);

    if (errnum != DRMAA_ERRNO_SUCCESS) {
        fprintf (stderr, "Could not set attribute \"%s\": %s\n",
                 name, error);
        return;
    }
}


void DrmaaJobs::setVectorAttribute(int jobTemplateId, const char* name, int nbArguments, const char ** arguments) {
    if(!isJobTemplateIdValid(jobTemplateId)) return;

    char error[DRMAA_ERROR_STRING_BUFFER];
    int errnum = 0;
    drmaa_job_template_t * drmaaJT = mJobTemplatesMap[jobTemplateId];


    const char ** args = new const char* [nbArguments+1];

    //printf("DrmaaJobs::setVectorAttribute: nbArguments = %d \n", nbArguments);
    for(int i = 0 ; i < nbArguments ; i++) {
        args[i] = arguments[i];
        //printf("DrmaaJobs::setVectorAttribute: argument num %d = %s \n", i, args[i]);
    }
    args[nbArguments] = NULL;

    errnum = drmaa_set_vector_attribute (drmaaJT, name, args,
                                   error, DRMAA_ERROR_STRING_BUFFER);

    if (errnum != DRMAA_ERRNO_SUCCESS) {
        fprintf (stderr, "Could not set attribute \"%s\": %s\n",
                 name, error);
        return;
    }

    delete[] args;

}


void DrmaaJobs::setCommand(int jobTemplateId, const char * remote_command, int nbArguments, const char ** arguments) {
    if(!isJobTemplateIdValid(jobTemplateId)) return;

    //printf("DrmaaJobs::setCommand remote_command = %s, nbArguments = %d \n", remote_command, nbArguments);
    for(int i = 0; i < nbArguments ; i++) {
        //printf("DrmaaJobs::setCommand: argument num %d = %s \n", i, arguments[i]);
    }

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

    errnum = drmaa_set_vector_attribute (drmaaJT, DRMAA_V_ARGV, args,
                                   error, DRMAA_ERROR_STRING_BUFFER);

    if (errnum != DRMAA_ERRNO_SUCCESS) {
        fprintf (stderr, "Could not set attribute \"%s\": %s\n",
                 DRMAA_V_ARGV, error);
        return;
    }

    delete[] args;
}


const std::string DrmaaJobs::runJob(int jobTemplateId) {

    std::string runningJobId = "";
    if(!isJobTemplateIdValid(jobTemplateId)) return runningJobId;

    char error[DRMAA_ERROR_STRING_BUFFER];
    int errnum = 0;
    drmaa_job_template_t * drmaaJT = mJobTemplatesMap[jobTemplateId];
    char jobid[DRMAA_JOBNAME_BUFFER];

    errnum = drmaa_run_job (jobid,
                            DRMAA_JOBNAME_BUFFER,
                            drmaaJT,
                            error,
                            DRMAA_ERROR_STRING_BUFFER);

    if (errnum != DRMAA_ERRNO_SUCCESS) {
        fprintf (stderr, "Could not submit job: %s\n", error);
        return runningJobId;
    }

    runningJobId = jobid;
    printf ("Your job has been submitted with id %s\n", jobid);
    return runningJobId;
}

void DrmaaJobs::runBulkJobs(int jobTemplateId, int nbJobs, std::list<std::string> & runningJobIds_out) {
    if(!isJobTemplateIdValid(jobTemplateId)) return;

    char error[DRMAA_ERROR_STRING_BUFFER];
    int errnum = 0;
    drmaa_job_template_t * drmaaJT = mJobTemplatesMap[jobTemplateId];

    drmaa_job_ids_t *ids = NULL;

    errnum = drmaa_run_bulk_jobs (&ids,
                                  drmaaJT,
                                  1,
                                  nbJobs,
                                  1,
                                  error,
                                  DRMAA_ERROR_STRING_BUFFER);

    if (errnum != DRMAA_ERRNO_SUCCESS) {
        fprintf (stderr, "Could not submit job: %s\n", error);
        drmaa_release_job_ids (ids);
        return;
    }

    runningJobIds_out.clear();
    char jobid[DRMAA_JOBNAME_BUFFER];
    while (drmaa_get_next_job_id (ids, jobid, DRMAA_JOBNAME_BUFFER) == DRMAA_ERRNO_SUCCESS) {
        runningJobIds_out.push_back(jobid);
        printf ("A job has been submitted with id %s\n", jobid);
    }
    drmaa_release_job_ids (ids);
}

void DrmaaJobs::wait(const std::string & runningJobId) {

    char error[DRMAA_ERROR_STRING_BUFFER];
    int errnum = 0;

    const char* jobid = runningJobId.c_str();

    char jobid_out[DRMAA_JOBNAME_BUFFER];
    int status = 0;
    drmaa_attr_values_t *rusage = NULL;

    errnum = drmaa_wait (jobid,
                          jobid_out,
                          DRMAA_JOBNAME_BUFFER,
                          &status,
                          DRMAA_TIMEOUT_WAIT_FOREVER,
                          &rusage,
                          error,
                          DRMAA_ERROR_STRING_BUFFER);

    if (errnum != DRMAA_ERRNO_SUCCESS) {
        fprintf (stderr, "Could not wait for job: %s\n", error);
        return;
    }

    printf("~~~ Job %s status: ~~~~~~~ \n", jobid);
    ExitJobStatus exitStatus = getJobStatus(status);

    printf("~~~ Job %s Usage: ~~~~~~~~ \n", jobid);
    char usage[DRMAA_ERROR_STRING_BUFFER];
    while (drmaa_get_next_attr_value (rusage, usage, DRMAA_ERROR_STRING_BUFFER) == DRMAA_ERRNO_SUCCESS) {
        printf ("  %s\n", usage);
    }
    printf("~~~~~~~~~~~~~~~~~~~~~~~~~ \n");
    drmaa_release_attr_values (rusage);
}


void DrmaaJobs::synchronize(const std::list<std::string> & runningJobIds) {
    char error[DRMAA_ERROR_STRING_BUFFER];
    int errnum = 0;

    //const char *jobids[100];
    const char ** jobids = new const char* [runningJobIds.size()+1];
    int index=0;
    for(std::list<std::string>::const_iterator i = runningJobIds.begin();i!=runningJobIds.end(); ++i) {
        jobids[index] = (*i).c_str();
        index++;
    }
    jobids[runningJobIds.size()] = NULL;

    errnum = drmaa_synchronize (jobids,
                                DRMAA_TIMEOUT_WAIT_FOREVER,
                                0,//means that we keep the jobs information available and will display and delete it later (inside wait())
                                error,
                                DRMAA_ERROR_STRING_BUFFER);

    delete[] jobids;

    if (errnum != DRMAA_ERRNO_SUCCESS) {
        fprintf (stderr, "Could not wait for jobs: %s\n", error);
    }
    else {
        printf ("Job tasks have finished.\n");

        // to get and diplay job status (and delete the job information)
        for(std::list<std::string>::const_iterator i = runningJobIds.begin();i!=runningJobIds.end(); ++i) {
            wait(*i);
            //mRunningJobMap.erase[*i];
        }
    }

}






void DrmaaJobs::control(const std::string & runningJobId, Action action) {

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
        break;
    }

    char error[DRMAA_ERROR_STRING_BUFFER];
    int errnum = 0;

    const char * jobId = runningJobId.c_str();
    errnum = drmaa_control(jobId,
                           drmaa_action,
                           error,
                           DRMAA_ERROR_STRING_BUFFER);

    if(errnum != DRMAA_ERRNO_SUCCESS) {
        fprintf (stderr, "Could not control the job: %s\n", error);
        return;
    }

}


bool DrmaaJobs::isJobTemplateIdValid(int jobTemplateId) {
    if(mJobTemplatesMap.count(jobTemplateId) == 0) {
        fprintf (stderr, "Job template with id %d doesn't exit\n", jobTemplateId);
        return false;
    } else return true;
}



DrmaaJobs::ExitJobStatus DrmaaJobs::getJobStatus(int drmaa_exit_status) {

    int aborted = 0;
    drmaa_wifaborted(&aborted, drmaa_exit_status, NULL, 0);

    if (aborted == 1) {
        printf("Job never ran\n");
        return EXIT_ABORTED;
    }

    int exited = 0;
    drmaa_wifexited(&exited, drmaa_exit_status, NULL, 0);

    if (exited == 1) {
        int exit_status = 0;

        drmaa_wexitstatus(&exit_status, drmaa_exit_status, NULL, 0);
        printf("Job finished regularly with exit status %d\n", exit_status);
        return FINISHED_REGULARLY;
     }

    int signaled = 0;
    drmaa_wifsignaled(&signaled, drmaa_exit_status, NULL, 0);

    if (signaled == 1) {
        char termsig[DRMAA_SIGNAL_BUFFER+1];

        drmaa_wtermsig(termsig, DRMAA_SIGNAL_BUFFER, drmaa_exit_status, NULL, 0);
        printf("Job finished due to signal %s\n", termsig);

        return FINISHED_TERM_SIG;
    }

    printf("Job finished with unclear conditions\n");
    return FINISHED_UNCLEAR_CONDITIONS;

}


int DrmaaJobs::getNextId() {
    return m_currentId++;
}


DrmaaJobs::JobStatus DrmaaJobs::jobStatus(const std::string & runningJobId) {

    //printf("drmaa job id %s \n", runningJobId.c_str());

    char error[DRMAA_ERROR_STRING_BUFFER];
    int errnum = 0;

    int status = 0;
    const char* jobid = runningJobId.c_str();
    errnum = drmaa_job_ps(jobid,
                          &status,
                          error,
                          DRMAA_ERROR_STRING_BUFFER);

    if (errnum != DRMAA_ERRNO_SUCCESS) {
        //fprintf (stderr, "Could not get job' status: %s\n", error);
        return UNDETERMINED;
    }

    //printf("job %s ", runningJobId.c_str());

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


void DrmaaJobs::jobStatus(const std::list<std::string> & runningJobIds, std::list<JobStatus> & statusList_out) {
    statusList_out.clear();
    for(std::list<std::string>::const_iterator i = runningJobIds.begin(); i != runningJobIds.end(); ++i) {
        statusList_out.push_back(jobStatus(*i));
    }
}


