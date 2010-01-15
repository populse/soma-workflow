// Define the SIP wrapper to the jobs library.

%Import aims.sip

%Module somajobssip 0

class JobTemplate {

%TypeHeaderCode
#include <soma/pipeline/job_template.h>
%End

public:
    JobTemplate(/*char *,*/ SIP_PYTUPLE)
    [(char * remote_command, int nbArguments, char ** arguments)];
%MethodCode
      int nbArguments = PyTuple_Size(a0);
      if( nbArguments < 1 )
        printf("No argument : Can't create a JobTemplate without an executable name \n");
      else {
        char * remote_command = PyString_AsString(PyTuple_GET_ITEM(a0,0));
        char ** arguments = new char * [nbArguments-1];
        for(int i = 1 ; i < nbArguments ; i++) {
            arguments[i-1] = PyString_AsString(PyTuple_GET_ITEM(a0, i));
            printf(" Argument %i %s \n", i-1, arguments[i-1]);
        }
        Py_BEGIN_ALLOW_THREADS
        sipCpp = new JobTemplate(remote_command, nbArguments-1, arguments);
        Py_END_ALLOW_THREADS
        delete[] arguments;
      }

/*    int nbArguments = PyTuple_Size(a1);
    char ** arguments = new char * [nbArguments];
    for(int i = 0 ; i < nbArguments ; i++) {
        arguments[i] = PyString_AsString(PyTuple_GET_ITEM(a1, i));
    }
    Py_BEGIN_ALLOW_THREADS
    sipCpp = new JobTemplate(a0, nbArguments, arguments);
    Py_END_ALLOW_THREADS
    delete[] arguments;
*/
%End


    //JobTemplate(char * remote_command, int nbArguments, char ** arguments);

    ~JobTemplate();

    void displayAttributes();

    void setAttribute(const std::string & attributeName, const std::string & attributeValue);

};



class JobRunner {

%TypeHeaderCode
#include <soma/pipeline/job_runner.h>
%End

public:
    JobRunner(const JobTemplate * jobTemplate);

    void runBulkJobs(int start, int end, int incr);

    void runJob();

    void wait(); // wait for the first job in the list

    void synchronise(int dispose = 1); // !!! Use DRMAA_JOB_IDS_SESSION_ALL which means that it will synchronise all the jobs and not only the jobs in mJobIdList

//   std::list<std::string> getJobList() const {
//        return mJobIdList;
//    }

};


class JobSession {

%TypeHeaderCode
#include <soma/pipeline/job_manager.h>
%End

public:

    static int initDrmaa();
    static int exitDrmaa();

};





