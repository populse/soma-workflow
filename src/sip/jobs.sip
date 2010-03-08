// Define the SIP wrapper to the jobs library.

%Module soma.pipeline.somadrmaajobssip 0

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
     }
     Py_BEGIN_ALLOW_THREADS
     sipCpp->setVectorAttribute(a0, a1, nbArguments, arguments);
     Py_END_ALLOW_THREADS
     delete[] arguments;
%End

    SIP_PYOBJECT runJob(int jobTemplateId);
%MethodCode
    std::string runningJobId;
    Py_BEGIN_ALLOW_THREADS
    runningJobId= sipCpp->runJob(a0);
    Py_END_ALLOW_THREADS
    PyObject * result = PyString_FromStringAndSize(runningJobId.c_str(), runningJobId.size());
    return result;
%End

    SIP_PYLIST runBulkJobs(int, int);
%MethodCode
    std::list<std::string> runningJobIds;
    Py_BEGIN_ALLOW_THREADS
    sipCpp->runBulkJobs(a0, a1, runningJobIds);
    Py_END_ALLOW_THREADS
    PyObject* result = PyList_New(0);
    for(std::list<std::string>::iterator i = runningJobIds.begin(); i != runningJobIds.end(); ++i) {
        PyList_Append(result, Py_BuildValue("i",PyString_FromString(i->c_str())));
    }
    return result;
%End

    void wait(SIP_PYOBJECT);
%MethodCode
    std::string runningJobId(PyString_AsString(a0));
    Py_BEGIN_ALLOW_THREADS
    sipCpp->wait(runningJobId);
    Py_END_ALLOW_THREADS
%End


    void synchronize(SIP_PYLIST);
%MethodCode
    std::list<std::string> runningJobIds;
    for(int i = 0; i < PyList_Size(a0); i++) {
        runningJobIds.push_back(PyString_AsString(PyList_GET_ITEM(a0,i)));
    }
    Py_BEGIN_ALLOW_THREADS
    sipCpp->synchronize(runningJobIds);
    Py_END_ALLOW_THREADS
%End



   SIP_PYOBJECT jobStatus(SIP_PYOBJECT);
%MethodCode
   std::string runningJobId(PyString_AsString(a0));
   DrmaaJobs::JobStatus status;
   Py_BEGIN_ALLOW_THREADS
   status = sipCpp->jobStatus(runningJobId);
   Py_END_ALLOW_THREADS
   switch (status) {
        case DrmaaJobs::UNDETERMINED:
            return PyString_FromString("undetermined");
        case DrmaaJobs::QUEUED_ACTIVE:
            return PyString_FromString("queued_active");//queued and active");
        case DrmaaJobs::SYSTEM_ON_HOLD:
            return PyString_FromString("system_on_hold);//queued and in system hold");
        case DrmaaJobs::USER_ON_HOLD:
            return PyString_FromString("user_on_hold");//queued and in user hold");
        case DrmaaJobs::USER_SYSTEM_ON_HOLD:
            return PyString_FromString("user_system_on_hold");//queued and in user and system hold");
        case DrmaaJobs::RUNNING:
            return PyString_FromString("running");
        case DrmaaJobs::SYSTEM_SUSPENDED:
            return PyString_FromString("system_suspended");
        case DrmaaJobs::USER_SUSPENDED:
            return PyString_FromString("user_suspended");
        case DrmaaJobs::USER_SYSTEM_SUSPENDED:
            return PyString_FromString("user_system_suspended");//user and system suspended");
        case DrmaaJobs::DONE:
            return PyString_FromString("done");//finished normally");
        case DrmaaJobs::FAILED:
            return PyString_FromString("failed");//"finished but failed");
    }
%End


    void suspend(SIP_PYOBJECT);
%MethodCode
     std::string runningJobId(PyString_AsString(a0));
     Py_BEGIN_ALLOW_THREADS
     sipCpp->control(runningJobId, DrmaaJobs::SUSPEND);
     Py_END_ALLOW_THREADS
%End

    void resume(SIP_PYOBJECT);
%MethodCode
     std::string runningJobId(PyString_AsString(a0));
     Py_BEGIN_ALLOW_THREADS
     sipCpp->control(runningJobId, DrmaaJobs::RESUME);
     Py_END_ALLOW_THREADS
%End

    void hold(SIP_PYOBJECT);
%MethodCode
     std::string runningJobId(PyString_AsString(a0));
     Py_BEGIN_ALLOW_THREADS
     sipCpp->control(runningJobId, DrmaaJobs::HOLD);
     Py_END_ALLOW_THREADS
%End

    void release(SIP_PYOBJECT);
%MethodCode
     std::string runningJobId(PyString_AsString(a0));
     Py_BEGIN_ALLOW_THREADS
     sipCpp->control(runningJobId, DrmaaJobs::RELEASE);
     Py_END_ALLOW_THREADS
%End

    void terminate(SIP_PYOBJECT);
%MethodCode
     std::string runningJobId(PyString_AsString(a0));
     Py_BEGIN_ALLOW_THREADS
     sipCpp->control(runningJobId, DrmaaJobs::TERMINATE);
     Py_END_ALLOW_THREADS
%End

};
