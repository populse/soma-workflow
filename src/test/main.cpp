
#include "jobManager.h"


int main (int argc, char **argv) {
    if(argc < 2) return 0;

    return JobManager::myTest(argc, argv);

    //return JobManager::runJobArray(argc, argv);
    //return JobManager::waitForJobs(argc, argv);
    //return JobManager::waitForAJob(argc, argv);
    //return JobManager::waitForJobsAndRecoverAccountingInformation(argc, argv);
    //return JobManager::runJobAndGetStatus(argc, argv);
    //return JobManager::getDRMAndDRMAAInfo(argc, argv);

}



