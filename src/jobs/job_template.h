#ifndef JOB_TEMPLATE_H
#define JOB_TEMPLATE_H

#include <soma/pipeline/drmaamanip.h>
#include <string>
#include <list>

class JobTemplate : public DrmaaManip
{
public:
    JobTemplate(char * remote_command, int nbArguments, char ** arguments);

    ~JobTemplate();

    void displayAttributes();

    void setAttribute(const std::string & attributeName, const std::string & attributeValue);

    const drmaa_job_template_t * getDrmaaJobTemplate() const {
        return mDrmaaJT;
    }

private:
    drmaa_job_template_t * mDrmaaJT;
};

#endif // JOB_TEMPLATE_H
