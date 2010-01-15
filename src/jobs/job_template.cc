#include <soma/pipeline/job_template.h>

#include <cstdio>


JobTemplate::JobTemplate(char * remote_command, int nbArguments, char ** arguments)
    : DrmaaManip(), mDrmaaJT(0)
{
    printf("JobTemplate::JobTemplate remote_command = %s, nbArguments = %d \n", remote_command, nbArguments);
    for(int i = 0; i < nbArguments ; i++) {
        printf("Argument %i : %s \n", nbArguments, arguments[i]);
    }


    // job template creation
    mErrnum = drmaa_allocate_job_template (&mDrmaaJT, mError, DRMAA_ERROR_STRING_BUFFER);

    if (mErrnum != DRMAA_ERRNO_SUCCESS) {
       fprintf (stderr, "Could not create job template: %s\n", mError);
       return;
    }


    mErrnum = drmaa_set_attribute (mDrmaaJT, DRMAA_REMOTE_COMMAND, remote_command,
                                   mError, DRMAA_ERROR_STRING_BUFFER);

    if (mErrnum != DRMAA_ERRNO_SUCCESS) {
        fprintf (stderr, "Could not set attribute \"%s\": %s\n",
                 DRMAA_REMOTE_COMMAND, mError);

        return;
    }

    int iattrib;
    for( iattrib = 0 ; iattrib < nbArguments ; iattrib++) {
        const char *args[2] = {arguments[iattrib], NULL};
        mErrnum = drmaa_set_vector_attribute (mDrmaaJT, DRMAA_V_ARGV, args, mError,
                                             DRMAA_ERROR_STRING_BUFFER);

        printf ("Attribute %d : %s\n", iattrib, arguments[iattrib]);
        if(mErrnum != DRMAA_ERRNO_SUCCESS) {
            fprintf (stderr, "Could not set attribute \"%s\": %s\n",
                     arguments[iattrib], mError);
            return;
        }
    }
}


JobTemplate::~JobTemplate() {
    if(mDrmaaJT !=0) {
        mErrnum = drmaa_delete_job_template (mDrmaaJT, mError, DRMAA_ERROR_STRING_BUFFER);

        if (mErrnum != DRMAA_ERRNO_SUCCESS) {
            fprintf (stderr, "Could not delete job template: %s\n", mError);
        }
    }
}



void JobTemplate::displayAttributes() {
    if(mErrnum != DRMAA_ERRNO_SUCCESS) return;

    int errnum = 0;
    char error[DRMAA_ERROR_STRING_BUFFER];

    drmaa_attr_names_t * attributeNames;

    errnum = drmaa_get_attribute_names(&attributeNames,
                                       error,
                                       DRMAA_ERROR_STRING_BUFFER);

    if(errnum != DRMAA_ERRNO_SUCCESS ) {
        fprintf (stderr, "Could not get attribute names : %s\n", error);
        return;
    }


    printf("**** Job attributes ****\n");
    char attributName[DRMAA_ERROR_STRING_BUFFER];
    while (drmaa_get_next_attr_name(attributeNames, attributName, DRMAA_ERROR_STRING_BUFFER) == DRMAA_ERRNO_SUCCESS)
    {
        char attributeValue [DRMAA_ERROR_STRING_BUFFER];
        errnum = drmaa_get_attribute (mDrmaaJT,
                                      attributName,
                                      attributeValue,
                                      DRMAA_ERROR_STRING_BUFFER,
                                      error,
                                      DRMAA_ERROR_STRING_BUFFER);
        if(errnum != DRMAA_ERRNO_SUCCESS ) {
            fprintf (stderr, "Job attribute \"%s\" : %s\n", attributName, error);
//            char attributeValue [DRMAA_ERROR_STRING_BUFFER];
//            drmaa_attr_values_t * values;
//            errnum = drmaa_get_vector_attribute (mDrmaaJT,
//                                                 attributName,
//                                                 &values,
//                                                 error,
//                                                 DRMAA_ERROR_STRING_BUFFER);
//            if(errnum != DRMAA_ERRNO_SUCCESS ) {
//                fprintf (stderr, "Job attribute \"%s\" : %s\n", attributName, error);
//            } else {
//
//                printf ("Job attribute \"%s\" : ", attributName);
//                while (drmaa_get_next_attr_value (values, attributeValue, DRMAA_JOBNAME_BUFFER) == DRMAA_ERRNO_SUCCESS) {
//                    printf ("%s ", attributName);
//                }
//                printf("\n");
//
//            }
        } else {
            printf ("Job attribute \"%s\" : \"%s\" \n", attributName, attributeValue);
        }
    }
    printf("************************\n");
}


void JobTemplate::setAttribute(const std::string & attributeName, const std::string & attributeValue) {
    if(mErrnum != DRMAA_ERRNO_SUCCESS) return;

    int errnum = 0;
    char error[DRMAA_ERROR_STRING_BUFFER];

    errnum = drmaa_set_attribute(mDrmaaJT,
                                 attributeName.c_str(),
                                 attributeValue.c_str(),
                                 error,
                                 DRMAA_ERROR_STRING_BUFFER);

    if(errnum != DRMAA_ERRNO_SUCCESS) {
        fprintf(stderr, "Could not set attribute \"%s\" : %s \n", attributeName.c_str(), error);
    }
}

