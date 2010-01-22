#include <soma/pipeline/job_manager.h>

#include <cstdio>
#include <stddef.h>
#include <unistd.h>
#include <soma/pipeline/drmaa/drmaa.h>

#include <list>
#include <string>

#include <soma/pipeline/job_template.h>
#include <soma/pipeline/job_runner.h>

//drmaa_remote_command
//drmaa_js_state
//drmaa_wd
//drmaa_job_category
//drmaa_native_specification
//drmaa_block_email
//drmaa_start_time
//drmaa_job_name
//drmaa_input_path
//drmaa_output_path
//drmaa_error_path
//drmaa_join_files
//drmaa_transfer_files



int JobManager::myTest(int argc, char **argv) {
    if(JobSession::initDrmaa()) return 1;
    {
       JobTemplate jobTemplate(argv[1], argc-2, argv+2);


       jobTemplate.setAttribute(DRMAA_JOB_NAME, "myJob");

       jobTemplate.setAttribute(DRMAA_TRANSFER_FILES, "ieo");

       //jobTemplate.setAttribute(DRMAA_INPUT_PATH, "[is143016]:/home/sl225510/bidon");

       jobTemplate.setAttribute(DRMAA_OUTPUT_PATH, "[is143016]:/home/sl225510/out");

       jobTemplate.setAttribute(DRMAA_ERROR_PATH, "[is143016]:/home/sl225510/err");

       jobTemplate.displayAttributes();


       JobRunner jobRunner(&jobTemplate);
       jobRunner.runBulkJobs(1,6,1);
       //jobRunner.runJob();

       jobRunner.wait();

       // job template destruction
   }
   return JobSession::exitDrmaa();
}






int JobManager::runJobArray(int argc, char **argv) {

    if(JobSession::initDrmaa()) return 1;
    {
       JobTemplate jobTemplate(argv[1], argc-2, argv+2);

       if (jobTemplate.getErrnum() == DRMAA_ERRNO_SUCCESS) {
           JobRunner jobRunner(&jobTemplate);
           jobRunner.runBulkJobs(1,30,2);
       }

        // job template destruction

   }

    return JobSession::exitDrmaa();
}

int JobManager::waitForAJob(int argc, char **argv) {

    if(JobSession::initDrmaa()) return 1;


    char error[DRMAA_ERROR_STRING_BUFFER];
    int errnum = 0;

    {
       JobTemplate jobTemplate(argv[1], argc-2, argv+2);

       if (jobTemplate.getErrnum() == DRMAA_ERRNO_SUCCESS) {

          JobRunner jobRunner(&jobTemplate);
          jobRunner.runJob();

          if(jobRunner.getErrnum() == DRMAA_ERRNO_SUCCESS) {

              jobRunner.wait();
          }
       }
       // jobTemplate destruction
    }
   return JobSession::exitDrmaa();

}


int JobManager::waitForJobs(int argc, char **argv) {

    if(JobSession::initDrmaa()) return 1;

    {
        JobTemplate jobTemplate(argv[1], argc-2, argv+2);

        if (jobTemplate.getErrnum() == DRMAA_ERRNO_SUCCESS) {

            JobRunner jobRunner(&jobTemplate);

            jobRunner.runBulkJobs(1, 30, 2);

            if(jobRunner.getErrnum() == DRMAA_ERRNO_SUCCESS) {

                jobRunner.synchronise();

            }
        }
        // jobTemplate destruction
    }

    return JobSession::exitDrmaa();
}



int JobManager::waitForJobsAndRecoverAccountingInformation(int argc, char **argv) {

    if(JobSession::initDrmaa()) return 1;

    char error[DRMAA_ERROR_STRING_BUFFER];
    int errnum = 0;

    {
        JobTemplate jobTemplate(argv[1], argc-2, argv+2);

        if (jobTemplate.getErrnum() == DRMAA_ERRNO_SUCCESS) {

            JobRunner jobRunner(&jobTemplate);

            jobRunner.runBulkJobs(1,30,2);

            if(jobRunner.getErrnum() == DRMAA_ERRNO_SUCCESS) {

                jobRunner.synchronise(0);

                if(jobRunner.getErrnum() == DRMAA_ERRNO_SUCCESS) {

                    jobRunner.wait();
                }
            }
        }
        // jobTemplate destruction
    }

    return JobSession::exitDrmaa();
}

int JobManager::runJobAndGetStatus(int argc, char **argv) {

    if(JobSession::initDrmaa()) return 1;

    char error[DRMAA_ERROR_STRING_BUFFER];
    int errnum = 0;

    {
        printf ("Job template \n");
        JobTemplate jobTemplate(argv[1], argc-2, argv+2);

        if (jobTemplate.getErrnum() == DRMAA_ERRNO_SUCCESS) {

            char jobid[DRMAA_JOBNAME_BUFFER];

            errnum = drmaa_run_job (jobid, DRMAA_JOBNAME_BUFFER, const_cast<drmaa_job_template_t*>(jobTemplate.getDrmaaJobTemplate()), error,
                                    DRMAA_ERROR_STRING_BUFFER);

            if (errnum != DRMAA_ERRNO_SUCCESS) {
                fprintf (stderr, "Could not submit job: %s\n", error);
            }
            else {
                int status = 0;

                printf ("Your job has been submitted with id %s\n", jobid);

                sleep (200);

                errnum = drmaa_job_ps (jobid, &status, error,
                                       DRMAA_ERROR_STRING_BUFFER);

                if (errnum != DRMAA_ERRNO_SUCCESS) {
                    fprintf (stderr, "Could not get job' status: %s\n", error);
                }
                else {
                    switch (status) {
                    case DRMAA_PS_UNDETERMINED:
                        printf ("Job status cannot be determined\n");
                        break;
                    case DRMAA_PS_QUEUED_ACTIVE:
                        printf ("Job is queued and active\n");
                        break;
                    case DRMAA_PS_SYSTEM_ON_HOLD:
                        printf ("Job is queued and in system hold\n");
                        break;
                    case DRMAA_PS_USER_ON_HOLD:
                        printf ("Job is queued and in user hold\n");
                        break;
                    case DRMAA_PS_USER_SYSTEM_ON_HOLD:
                        printf ("Job is queued and in user and system hold\n");
                        break;
                    case DRMAA_PS_RUNNING:
                        printf ("Job is running\n");
                        break;
                    case DRMAA_PS_SYSTEM_SUSPENDED:
                        printf ("Job is system suspended\n");
                        break;
                    case DRMAA_PS_USER_SUSPENDED:
                        printf ("Job is user suspended\n");
                        break;
                    case DRMAA_PS_USER_SYSTEM_SUSPENDED:
                        printf ("Job is user and system suspended\n");
                        break;
                    case DRMAA_PS_DONE:
                        printf ("Job finished normally\n");
                        break;
                    case DRMAA_PS_FAILED:
                        printf ("Job finished, but failed\n");
                        break;
                    } /* switch */
                } /* else */
            } /* else */
        }

        // jobTemplate destruction
    }

    return JobSession::exitDrmaa();
 }

int JobSession::getDRMAndDRMAAInfo(int argc, char **argv)  {

char error[DRMAA_ERROR_STRING_BUFFER];
int errnum = 0;
char contact[DRMAA_CONTACT_BUFFER];
char drm_system[DRMAA_DRM_SYSTEM_BUFFER];
char drmaa_impl[DRMAA_DRM_SYSTEM_BUFFER];
unsigned int major = 0;
unsigned int minor = 0;

errnum = drmaa_get_contact (contact, DRMAA_CONTACT_BUFFER, error,
                            DRMAA_ERROR_STRING_BUFFER);

if (errnum != DRMAA_ERRNO_SUCCESS) {
   fprintf (stderr, "Could not get the contact string list: %s\n", error);
}
else {
   printf ("Supported contact strings: \"%s\"\n", contact);
}

errnum = drmaa_get_DRM_system (drm_system, DRMAA_DRM_SYSTEM_BUFFER, error,
                            DRMAA_ERROR_STRING_BUFFER);

if (errnum != DRMAA_ERRNO_SUCCESS) {
   fprintf (stderr, "Could not get the DRM system list: %s\n", error);
}
else {
   printf ("Supported DRM systems: \"%s\"\n", drm_system);
}

errnum = drmaa_get_DRMAA_implementation (drmaa_impl, DRMAA_DRM_SYSTEM_BUFFER,
                                         error, DRMAA_ERROR_STRING_BUFFER);

if (errnum != DRMAA_ERRNO_SUCCESS) {
   fprintf (stderr, "Could not get the DRMAA implementation list: %s\n", error);
}
else {
   printf ("Supported DRMAA implementations: \"%s\"\n", drmaa_impl);
}

errnum = drmaa_init (NULL, error, DRMAA_ERROR_STRING_BUFFER);

if (errnum != DRMAA_ERRNO_SUCCESS) {
   fprintf (stderr, "Could not initialize the DRMAA library: %s\n", error);
   return 1;
}

errnum = drmaa_get_contact (contact, DRMAA_CONTACT_BUFFER, error,
                            DRMAA_ERROR_STRING_BUFFER);

if (errnum != DRMAA_ERRNO_SUCCESS) {
   fprintf (stderr, "Could not get the contact string: %s\n", error);
}
else {
   printf ("Connected contact string: \"%s\"\n", contact);
}

errnum = drmaa_get_DRM_system (drm_system, DRMAA_CONTACT_BUFFER, error,
                            DRMAA_ERROR_STRING_BUFFER);

if (errnum != DRMAA_ERRNO_SUCCESS) {
   fprintf (stderr, "Could not get the DRM system: %s\n", error);
}
else {
   printf ("Connected DRM system: \"%s\"\n", drm_system);
}

errnum = drmaa_get_DRMAA_implementation (drmaa_impl, DRMAA_DRM_SYSTEM_BUFFER,
                                         error, DRMAA_ERROR_STRING_BUFFER);

if (errnum != DRMAA_ERRNO_SUCCESS) {
   fprintf (stderr, "Could not get the DRMAA implementation list: %s\n", error);
}
else {
   printf ("Supported DRMAA implementations: \"%s\"\n", drmaa_impl);
}

errnum = drmaa_version (&major, &minor, error, DRMAA_ERROR_STRING_BUFFER);

if (errnum != DRMAA_ERRNO_SUCCESS) {
   fprintf (stderr, "Could not get the DRMAA version: %s\n", error);
}
else {
   printf ("Using DRMAA version %d.%d\n", major, minor);
}

errnum = drmaa_exit (error, DRMAA_ERROR_STRING_BUFFER);

if (errnum != DRMAA_ERRNO_SUCCESS) {
   fprintf (stderr, "Could not shut down the DRMAA library: %s\n", error);
   return 1;
}

return 0;

}




int JobSession::initDrmaa() {

    char error[DRMAA_ERROR_STRING_BUFFER];
    int errnum = 0;

    errnum = drmaa_init (NULL, error, DRMAA_ERROR_STRING_BUFFER);

    if (errnum != DRMAA_ERRNO_SUCCESS) {
        fprintf (stderr, "Could not initialize the DRMAA library: %s\n", error);
        return 1;
    } else return 0;

}


int JobSession::exitDrmaa() {

    char error[DRMAA_ERROR_STRING_BUFFER];
    int errnum = 0;

    errnum = drmaa_exit (error, DRMAA_ERROR_STRING_BUFFER);

    if (errnum != DRMAA_ERRNO_SUCCESS) {
       fprintf (stderr, "Could not shut down the DRMAA library: %s\n", error);
       return 1;
    } else return 0;

}
