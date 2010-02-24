// Define the SIP wrapper to the jobs library.

%Module somadrmaajobssip 0

class DrmaaJobs {

%TypeHeaderCode
 #include <soma/pipeline/drmaajobs.h>
%End

public:

    int allocateJobTemplate();

    void deleteJobTemplate(int jobTemplateId);

    void setCommand(int, const char *, SIP_PYLIST);
%MethodCode
     int nbArguments = PyList_Size(a2);
     const char ** arguments = new const char * [nbArguments];
     for(int i = 0 ; i < nbArguments ; i++) {
        arguments[i] = const_cast<const char *>(PyString_AsString(PyList_GET_ITEM(a2, i)));
        printf("SIP: Argument %i %s \n", i, arguments[i]);
    }
    Py_BEGIN_ALLOW_THREADS
    sipCpp->setCommand(a0, a1, nbArguments, arguments);
    Py_END_ALLOW_THREADS
    delete[] arguments;

%End

    void setAttribute(int jobTemplateId, const char *name, const char *value);

    void setVectorAttribute(int, const char* , SIP_PYLIST);
%MethodCode
     int nbArguments = PyList_Size(a2);
     const char ** arguments = new const char * [nbArguments];
     for(int i = 0 ; i < nbArguments ; i++) {
         arguments[i] = const_cast<const char *>(PyString_AsString(PyList_GET_ITEM(a2, i)));
         printf("SIP: Argument %i %s \n", i, arguments[i]);
     }
     Py_BEGIN_ALLOW_THREADS
     sipCpp->setVectorAttribute(a0, a1, nbArguments, arguments);
     Py_END_ALLOW_THREADS
     delete[] arguments;
%End

    int runJob(int jobTemplateId);

/*    void runBulkJobs(int, int, SIP_PYLIST);
%MethodCode
    std::list<int> runningJobIds;
    Py_BEGIN_ALLOW_THREADS
    sipCpp->runBulkJobs(a0, a1, runningJobIds);
    Py_END_ALLOW_THREADS
    for(std::list<int>::iterator i = runningJobIds.begin(); i != runningJobIds.end(); ++i) {
        PyList_Append(a2, Py_BuildValue("i",*i));
    }
%End
*/

    SIP_PYLIST runBulkJobs(int, int);
%MethodCode
    std::list<int> runningJobIds;
    Py_BEGIN_ALLOW_THREADS
    sipCpp->runBulkJobs(a0, a1, runningJobIds);
    Py_END_ALLOW_THREADS
    PyObject* result = PyList_New(0);
    for(std::list<int>::iterator i = runningJobIds.begin(); i != runningJobIds.end(); ++i) {
        PyList_Append(result, Py_BuildValue("i",*i));
    }
    return result;
%End

    void wait(int runningJobId);

    //int waitForAnyJob();

    void synchronize(SIP_PYLIST);
%MethodCode
    std::list<int> runningJobIds;
    for(int i = 0; i < PyList_Size(a0); i++) {
        runningJobIds.push_back((int)PyInt_AsLong(PyList_GET_ITEM(a0,i)));
    }
    Py_BEGIN_ALLOW_THREADS
    sipCpp->synchronize(runningJobIds);
    Py_END_ALLOW_THREADS
%End


   void synchronizeAllJobs();


   /*int*/SIP_PYOBJECT jobStatus(int);
%MethodCode
 //   PyObject * result;
 //   Py_BEGIN_ALLOW_THREADS
 //   result = PyInt_FromLong((long)(sipCpp->jobStatus(a0)));
 //   Py_END_ALLOW_THREADS
 //   return result;
    DrmaaJobs::JobStatus status;
    Py_BEGIN_ALLOW_THREADS
    status = sipCpp->jobStatus(a0);
    Py_END_ALLOW_THREADS
    switch (status) {
        case DrmaaJobs::UNDETERMINED:
            return PyString_FromString("undetermined");
        case DrmaaJobs::QUEUED_ACTIVE:
            return PyString_FromString("queued and active");
        case DrmaaJobs::SYSTEM_ON_HOLD:
            return PyString_FromString("queued and in system hold");
        case DrmaaJobs::USER_ON_HOLD:
            return PyString_FromString("queued and in user hold");
        case DrmaaJobs::USER_SYSTEM_ON_HOLD:
            return PyString_FromString("queued and in user and system hold");
        case DrmaaJobs::RUNNING:
            return PyString_FromString("running");
        case DrmaaJobs::SYSTEM_SUSPENDED:
            return PyString_FromString("system suspended");
        case DrmaaJobs::USER_SUSPENDED:
            return PyString_FromString("user suspended");
        case DrmaaJobs::USER_SYSTEM_SUSPENDED:
            return PyString_FromString("user and system suspended");
        case DrmaaJobs::DONE:
            return PyString_FromString("finished normally");
        case DrmaaJobs::FAILED:
            return PyString_FromString("finished but failed");
    }
%End

/*
    SIP_PYLIST jobStatus(SIP_PYLIST);
%MethodCode
    PyObject* result = PyList_New(0);
    for(int i = 0 ; i < PyList_Size(a0) ; ++i) {
            PyList_Append(result, sipSelf->jobStatus(PyInt_AsLong(PyList_GET_ITEM(a0, i))));
    }
    return result;
%End
*/

    void suspend(int);
%MethodCode
     Py_BEGIN_ALLOW_THREADS
     sipCpp->control(a0, DrmaaJobs::SUSPEND);
     Py_END_ALLOW_THREADS
%End

    void resume(int);
%MethodCode
     Py_BEGIN_ALLOW_THREADS
     sipCpp->control(a0, DrmaaJobs::RESUME);
     Py_END_ALLOW_THREADS
%End

    void hold(int);
%MethodCode
     Py_BEGIN_ALLOW_THREADS
     sipCpp->control(a0, DrmaaJobs::HOLD);
     Py_END_ALLOW_THREADS
%End

    void release(int);
%MethodCode
     Py_BEGIN_ALLOW_THREADS
     sipCpp->control(a0, DrmaaJobs::RELEASE);
     Py_END_ALLOW_THREADS
%End

    void terminate(int);
%MethodCode
     Py_BEGIN_ALLOW_THREADS
     sipCpp->control(a0, DrmaaJobs::TERMINATE);
     Py_END_ALLOW_THREADS
%End

};
