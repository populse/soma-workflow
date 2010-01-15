#ifndef JOB_MANAGER_H
#define JOB_MANAGER_H

class JobSession {

public :

    static int initDrmaa();
    static int exitDrmaa();
    static int getDRMAndDRMAAInfo(int argc, char **argv);

};


class JobManager {

public :

    static int runJobArray(int argc, char **argv);

    static int waitForJobs(int argc, char **argv);

    static int waitForAJob(int argc, char **argv);

    static int waitForJobsAndRecoverAccountingInformation(int argc, char **argv);

    static int runJobAndGetStatus(int argc, char **argv);

    static int myTest(int argc, char **argv);

};


#endif //JOB_MANAGER_H
