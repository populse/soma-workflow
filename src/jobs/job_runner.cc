#include <soma/pipeline/job_runner.h>

#include <soma/pipeline/job_template.h>

#include <cstdio>

JobRunner::JobRunner(const JobTemplate * jobTemplate)
    : DrmaaManip(), mJobTemplate(jobTemplate)
{


}

void JobRunner::runBulkJobs(int start, int end, int incr) {

    if( mJobTemplate->getErrnum()!=DRMAA_ERRNO_SUCCESS )
        return;

    mJobIdList.clear();

    drmaa_job_ids_t *ids = NULL;

    mErrnum = drmaa_run_bulk_jobs (&ids,
                                   const_cast<drmaa_job_template_t*>(mJobTemplate->getDrmaaJobTemplate()),
                                   start,
                                   end,
                                   incr,
                                   mError,
                                   DRMAA_ERROR_STRING_BUFFER);

    if (mErrnum != DRMAA_ERRNO_SUCCESS) {
        fprintf (stderr, "Could not submit job: %s\n", mError);
    }
    else {

        char jobid[DRMAA_JOBNAME_BUFFER];

        while (drmaa_get_next_job_id (ids, jobid, DRMAA_JOBNAME_BUFFER) == DRMAA_ERRNO_SUCCESS) {
            mJobIdList.push_back(jobid);
            printf ("A job task has been submitted with id %s\n", jobid);
        }
    }

    drmaa_release_job_ids (ids);
}


void JobRunner::runJob() {

    if( mJobTemplate->getErrnum()!=DRMAA_ERRNO_SUCCESS )
        return;

    mJobIdList.clear();

    char jobid[DRMAA_JOBNAME_BUFFER];
    mErrnum = drmaa_run_job (jobid,
                            DRMAA_JOBNAME_BUFFER,
                            const_cast<drmaa_job_template_t*>(mJobTemplate->getDrmaaJobTemplate()),
                            mError,
                            DRMAA_ERROR_STRING_BUFFER);

    if (mErrnum != DRMAA_ERRNO_SUCCESS) {
        fprintf (stderr, "Could not submit job: %s\n", mError);
    }
    else {
         mJobIdList.push_back(jobid);
        printf ("Your job has been submitted with id %s\n", jobid);
    }
}


void JobRunner::wait() {

    if(mJobTemplate->getErrnum()!=DRMAA_ERRNO_SUCCESS || mErrnum!=DRMAA_ERRNO_SUCCESS || mJobIdList.size() == 0 ) return;

    for(std::list<std::string>::iterator iter = mJobIdList.begin(); iter != mJobIdList.end(); ++iter) {

    char jobid_out[DRMAA_JOBNAME_BUFFER];
    int status = 0;
    drmaa_attr_values_t *rusage = NULL;

    std::string jid = *iter;
    const char* jobid = jid.c_str();
    mErrnum = drmaa_wait (jobid,
                         jobid_out,
                         DRMAA_JOBNAME_BUFFER,
                         &status,
                         DRMAA_TIMEOUT_WAIT_FOREVER,
                         &rusage,
                         mError,
                         DRMAA_ERROR_STRING_BUFFER);

    if (mErrnum != DRMAA_ERRNO_SUCCESS) {
        fprintf (stderr, "Could not wait for job: %s\n", mError);
        return;
    }


    int aborted = 0;
    drmaa_wifaborted(&aborted, status, NULL, 0);
    if (aborted == 1) {
        printf("Job %s never ran\n", jobid);
        return;
    } else {
        int exited = 0;
        drmaa_wifexited(&exited, status, NULL, 0);

        if (exited == 1) {
            int exit_status = 0;

            drmaa_wexitstatus(&exit_status, status, NULL, 0);
            printf("Job %s finished regularly with exit status %d\n", jobid, exit_status);
        }
        else {

            int signaled = 0;
            drmaa_wifsignaled(&signaled, status, NULL, 0);

            if (signaled == 1) {
                char termsig[DRMAA_SIGNAL_BUFFER+1];

                drmaa_wtermsig(termsig, DRMAA_SIGNAL_BUFFER, status, NULL, 0);
                printf("Job %s finished due to signal %s\n", jobid, termsig);
            }
            else {
                printf("Job %s finished with unclear conditions\n", jobid);
            }
        }
    }


    printf ("Job Usage:\n");
    char usage[DRMAA_ERROR_STRING_BUFFER];
    while (drmaa_get_next_attr_value (rusage, usage, DRMAA_ERROR_STRING_BUFFER) == DRMAA_ERRNO_SUCCESS) {
        printf ("  %s\n", usage);
    }

    drmaa_release_attr_values (rusage);

}

}


void JobRunner::synchronise(int dispose) {

    if(mJobTemplate->getErrnum()!=DRMAA_ERRNO_SUCCESS || mErrnum!=DRMAA_ERRNO_SUCCESS || mJobIdList.size() == 0 ) return;

    const char *jobids[2] = {DRMAA_JOB_IDS_SESSION_ALL, NULL};

    mErrnum = drmaa_synchronize (jobids,
                                DRMAA_TIMEOUT_WAIT_FOREVER,
                                dispose,
                                mError,
                                DRMAA_ERROR_STRING_BUFFER);

    if (mErrnum != DRMAA_ERRNO_SUCCESS) {
        fprintf (stderr, "Could not wait for jobs: %s\n", mError);
    }
    else {
        printf ("All job tasks have finished.\n");
    }
}
