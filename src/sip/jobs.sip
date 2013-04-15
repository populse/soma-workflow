// Define the SIP wrapper to the jobs library.

%Module soma.workflow.somadrmaajobssip 0


%Exception DrmaaError(SIP_Exception)
/PyName=DrmaaError/
{
%TypeHeaderCode
  #include <soma/workflow/drmaajobs.h>
%End
%RaiseCode
        const char *detail = sipExceptionRef.what();

        SIP_BLOCK_THREADS
        PyErr_SetString(sipException_DrmaaError, detail);
        SIP_UNBLOCK_THREADS
%End
}; 


class DrmaaJobs {

%TypeHeaderCode
 #include <soma/workflow/drmaajobs.h>
%End

public:
  
    void initSession() throw (DrmaaError); 

    int allocateJobTemplate() throw (DrmaaError);

    void deleteJobTemplate(int jobTemplateId);

    void setCommand(int, const char *, SIP_PYLIST) throw (DrmaaError);
%MethodCode
     int nbArguments = PyList_Size(a2);
     const char ** arguments = new const char * [nbArguments];
     for(int i = 0 ; i < nbArguments ; i++) {
        arguments[i] = const_cast<const char *>(PyString_AsString(PyList_GET_ITEM(a2, i)));
    }
    Py_BEGIN_ALLOW_THREADS
    try
    {
    sipCpp->setCommand(a0, a1, nbArguments, arguments);
    }
    catch (DrmaaError &sipExceptionRef)
    {
      Py_BLOCK_THREADS

      const char *detail = sipExceptionRef.what();

      SIP_BLOCK_THREADS
      PyErr_SetString(sipException_DrmaaError, detail);
      SIP_UNBLOCK_THREADS

      return NULL;
    }
    catch (...)
    {
      Py_BLOCK_THREADS

      sipRaiseUnknownException();
      return NULL;
    }
    Py_END_ALLOW_THREADS
    delete[] arguments;

%End

    void setAttribute(int jobTemplateId, const char *name, const char *value) throw (DrmaaError);

    void setVectorAttribute(int, const char* , SIP_PYLIST) throw (DrmaaError);
%MethodCode
     int nbArguments = PyList_Size(a2);
     const char ** arguments = new const char * [nbArguments];
     for(int i = 0 ; i < nbArguments ; i++) {
         arguments[i] = const_cast<const char *>(PyString_AsString(PyList_GET_ITEM(a2, i)));
     }
     Py_BEGIN_ALLOW_THREADS
     try
     {
     sipCpp->setVectorAttribute(a0, a1, nbArguments, arguments);
     }
     catch (DrmaaError &sipExceptionRef)
     {
        Py_BLOCK_THREADS

        const char *detail = sipExceptionRef.what();

        SIP_BLOCK_THREADS
        PyErr_SetString(sipException_DrmaaError, detail);
        SIP_UNBLOCK_THREADS

        return NULL;
     }
     catch (...)
     {
        Py_BLOCK_THREADS

        sipRaiseUnknownException();
        return NULL;
     }
     Py_END_ALLOW_THREADS
     delete[] arguments;
%End

    SIP_PYOBJECT runJob(int jobTemplateId) throw (DrmaaError);
%MethodCode
    std::string submittedJobId;
    Py_BEGIN_ALLOW_THREADS
    try
    {
    submittedJobId= sipCpp->runJob(a0);
    }
    catch (DrmaaError &sipExceptionRef)
    {
      Py_BLOCK_THREADS

      const char *detail = sipExceptionRef.what();

      SIP_BLOCK_THREADS
      PyErr_SetString(sipException_DrmaaError, detail);
      SIP_UNBLOCK_THREADS

      return NULL;
    }
    catch (...)
    {
      Py_BLOCK_THREADS

      sipRaiseUnknownException();
      return NULL;
    }
    Py_END_ALLOW_THREADS
    PyObject * result = PyString_FromStringAndSize(submittedJobId.c_str(), submittedJobId.size());
    return result;
%End

    SIP_PYLIST runBulkJobs(int, int) throw (DrmaaError);
%MethodCode
    std::list<std::string> submittedJobIds;
    Py_BEGIN_ALLOW_THREADS
    try
    {
    sipCpp->runBulkJobs(a0, a1, submittedJobIds);
    }
    catch (DrmaaError &sipExceptionRef)
    {
      Py_BLOCK_THREADS

      const char *detail = sipExceptionRef.what();

      SIP_BLOCK_THREADS
      PyErr_SetString(sipException_DrmaaError, detail);
      SIP_UNBLOCK_THREADS

      return NULL;
    }
    catch (...)
    {
      Py_BLOCK_THREADS

      sipRaiseUnknownException();
      return NULL;
    }
    Py_END_ALLOW_THREADS
    PyObject* result = PyList_New(0);
    for(std::list<std::string>::iterator i = submittedJobIds.begin(); i != submittedJobIds.end(); ++i) {
        PyList_Append(result, Py_BuildValue("i",PyString_FromString(i->c_str())));
    }
    return result;
%End

    // wait method
    // @type  a0: string
    // @param a0: job id
    // @type  a1: int
    // @param a1: timeout in second (-1 means to wait indefinitely and 0 
    // to return immediately) 
    // @rtype: tuple (string, int, string, list of string)
    // @return: (exitStatus, exitValue, termSignal, resourceUsage)
    SIP_PYOBJECT wait(SIP_PYOBJECT, int=-1);
%MethodCode
    std::string submittedJobId(PyString_AsString(a0));
    DrmaaJobs::ExitJobInfo jobInfo;
    Py_BEGIN_ALLOW_THREADS
    jobInfo = sipCpp->wait(submittedJobId, a1);
    Py_END_ALLOW_THREADS

    PyObject * status; 
    PyObject* exitValue;
    PyObject* termSig;
    switch (jobInfo.status) {
        case DrmaaJobs::EXIT_UNDETERMINED:
            status=PyString_FromString("exit_status_undetermined");
            exitValue=Py_None;
            termSig= Py_None;
            break;
        case DrmaaJobs::EXIT_ABORTED:
            status=PyString_FromString("aborted");
            exitValue=Py_None;
            termSig= Py_None;
            break;
        case DrmaaJobs::FINISHED_REGULARLY:
            status=PyString_FromString("finished_regularly");
            exitValue=PyInt_FromSsize_t(jobInfo.exitValue);
            termSig= Py_None;
            break;
        case DrmaaJobs::FINISHED_TERM_SIG:
            status=PyString_FromString("finished_signal");
            exitValue=Py_None;
            termSig= PyString_FromString(jobInfo.termSignal.c_str());
            break;
        default:
            status=PyString_FromString("finished_unclear_condition");
            exitValue=Py_None;
            termSig= Py_None;
            break;
    }
    
    PyObject* resourceUsage = PyList_New(0);
    for(std::list<std::string>::iterator i = jobInfo.resourceUsage.begin(); i != jobInfo.resourceUsage.end(); ++i) {
        PyList_Append(resourceUsage, PyString_FromString(i->c_str()));
    }

    PyObject* result = PyTuple_New(4);
    PyTuple_SET_ITEM(result, 0, status);
    PyTuple_SET_ITEM(result, 1, exitValue);
    PyTuple_SET_ITEM(result, 2, termSig);
    PyTuple_SET_ITEM(result, 3, resourceUsage);
    
    return result;
    
%End

    // synchronize method
    // @type  a0: sequence of strings
    // @param a0: job id to wait for
    // @type  a1: int
    // @param a1: timeout in second (-1 means to wait indefinitely and 0 
    // to return immediately) 
    void synchronize(SIP_PYLIST, int=-1);
%MethodCode
    std::list<std::string> submittedJobIds;
    for(int i = 0; i < PyList_Size(a0); i++) {
        submittedJobIds.push_back(PyString_AsString(PyList_GET_ITEM(a0,i)));
    }
    Py_BEGIN_ALLOW_THREADS
    sipCpp->synchronize(submittedJobIds, a1);
    Py_END_ALLOW_THREADS
%End



   SIP_PYOBJECT jobStatus(SIP_PYOBJECT) throw (DrmaaError);
%MethodCode
   std::string submittedJobId(PyString_AsString(a0));
   DrmaaJobs::JobStatus status;
   Py_BEGIN_ALLOW_THREADS
   try
   {
   status = sipCpp->jobStatus(submittedJobId);
   }
   catch (DrmaaError &sipExceptionRef)
   {
     Py_BLOCK_THREADS

     const char *detail = sipExceptionRef.what();

     SIP_BLOCK_THREADS
     PyErr_SetString(sipException_DrmaaError, detail);
     SIP_UNBLOCK_THREADS

     return NULL;
   }
   catch (...)
   {
     Py_BLOCK_THREADS

     sipRaiseUnknownException();
     return NULL;
   }
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


    void suspend(SIP_PYOBJECT) throw (DrmaaError);
%MethodCode
    std::string submittedJobId(PyString_AsString(a0));
    Py_BEGIN_ALLOW_THREADS
    try 
    {
    sipCpp->control(submittedJobId, DrmaaJobs::SUSPEND);
    }
    catch (DrmaaError &sipExceptionRef)
    {
      Py_BLOCK_THREADS

      const char *detail = sipExceptionRef.what();

      SIP_BLOCK_THREADS
      PyErr_SetString(sipException_DrmaaError, detail);
      SIP_UNBLOCK_THREADS

      return NULL;
    }
    catch (...)
    {
      Py_BLOCK_THREADS

      sipRaiseUnknownException();
      return NULL;
    }
    Py_END_ALLOW_THREADS
%End

    void resume(SIP_PYOBJECT) throw (DrmaaError);
%MethodCode
    std::string submittedJobId(PyString_AsString(a0));
    Py_BEGIN_ALLOW_THREADS
    try
    {
    sipCpp->control(submittedJobId, DrmaaJobs::RESUME);
    }
    catch (DrmaaError &sipExceptionRef)
    {
      Py_BLOCK_THREADS

      const char *detail = sipExceptionRef.what();

      SIP_BLOCK_THREADS
      PyErr_SetString(sipException_DrmaaError, detail);
      SIP_UNBLOCK_THREADS

      return NULL;
    }
    catch (...)
    {
      Py_BLOCK_THREADS

      sipRaiseUnknownException();
      return NULL;
    }
    Py_END_ALLOW_THREADS
%End

    void hold(SIP_PYOBJECT) throw (DrmaaError);
%MethodCode
    std::string submittedJobId(PyString_AsString(a0));
    Py_BEGIN_ALLOW_THREADS
    try
    {
    sipCpp->control(submittedJobId, DrmaaJobs::HOLD);
    }
    catch (DrmaaError &sipExceptionRef)
    {
      Py_BLOCK_THREADS

      const char *detail = sipExceptionRef.what();

      SIP_BLOCK_THREADS
      PyErr_SetString(sipException_DrmaaError, detail);
      SIP_UNBLOCK_THREADS

      return NULL;
    }
    catch (...)
    {
      Py_BLOCK_THREADS

      sipRaiseUnknownException();
      return NULL;
    }
    Py_END_ALLOW_THREADS
%End

    void release(SIP_PYOBJECT) throw (DrmaaError);
%MethodCode
    std::string submittedJobId(PyString_AsString(a0));
    Py_BEGIN_ALLOW_THREADS
    try
    {
    sipCpp->control(submittedJobId, DrmaaJobs::RELEASE);
    }
    catch (DrmaaError &sipExceptionRef)
    {
      Py_BLOCK_THREADS

      const char *detail = sipExceptionRef.what();

      SIP_BLOCK_THREADS
      PyErr_SetString(sipException_DrmaaError, detail);
      SIP_UNBLOCK_THREADS

      return NULL;
    }
    catch (...)
    {
      Py_BLOCK_THREADS

      sipRaiseUnknownException();
      return NULL;
    }
    Py_END_ALLOW_THREADS
%End

    void terminate(SIP_PYOBJECT) throw (DrmaaError);
%MethodCode
    std::string submittedJobId(PyString_AsString(a0));
    Py_BEGIN_ALLOW_THREADS
    try 
    {
    sipCpp->control(submittedJobId, DrmaaJobs::TERMINATE);
    }
    catch (DrmaaError &sipExceptionRef)
    {
      Py_BLOCK_THREADS

      const char *detail = sipExceptionRef.what();

      SIP_BLOCK_THREADS
      PyErr_SetString(sipException_DrmaaError, detail);
      SIP_UNBLOCK_THREADS

      return NULL;
    }
    catch (...)
    {
      Py_BLOCK_THREADS

      sipRaiseUnknownException();
      return NULL;
    }
    Py_END_ALLOW_THREADS
%End

};
