#ifndef JOB_RUNNER_H
#define JOB_RUNNER_H

#include <soma/pipeline/drmaamanip.h>

#include <list>
#include <string>

class JobTemplate;

typedef char JobId[DRMAA_JOBNAME_BUFFER];

class JobRunner : public DrmaaManip
{
public:
    JobRunner(const JobTemplate * jobTemplate);

    void runBulkJobs(int start, int end, int incr);

    void runJob();

    void wait(); // wait for the first job in the list

    void synchronise(int dispose = 1); // !!! Use DRMAA_JOB_IDS_SESSION_ALL which means that it will synchronise all the jobs and not only the jobs in mJobIdList

    std::list<std::string> getJobList() const {
        return mJobIdList;
    }
    
private:
    std::list<std::string> mJobIdList;

    int mNbJob;
    const JobTemplate * mJobTemplate;

};

#endif // JOB_RUNNER_H
