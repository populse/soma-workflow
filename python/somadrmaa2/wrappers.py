
"""DRMAA2 C library function wrappers"""

from ctypes import *
from ctypes.util import find_library
from soma_workflow.utils import DetectFindLib
from somadrmaa.errors import DrmaaException
import os


_drmaa_lib_env_name = 'DRMAA_LIBRARY_PATH'

(DRMMA_LIB_FOUND, _lib) = DetectFindLib(_drmaa_lib_env_name, 'drmaav2')


STRING = c_char_p
size_t = c_ulong
ptrdiff_t = c_int

drmaa2_lasterror = _lib.drmaa2_lasterror
drmaa2_lasterror_text = _lib.drmaa2_lasterror_text
drmaa2_lasterror_text.restype = STRING


def error_check(code):
    if code == 0:
        return
    else:
        raise DrmaaException("code %s: %s" % (code, drmaa2_lasterror_text()))


class drmaa2_list_s(Structure):
    _fields_ = [
        ('free_callback', c_void_p),
        ('type', c_int),
        ('size', c_long),
        ('head', c_void_p),
      ]
drmaa2_string_list = POINTER(drmaa2_list_s)

class drmaa2_dict_s(Structure):
    _fields_ = [
        #('free_entry', drmaa2_dict_entryfree),
        #('head', drmaa2_item),
    ]

drmaa2_dict = POINTER(drmaa2_dict_s)

class drmaa2_jsession_s(Structure):
    _fields_ = []

drmaa2_jsession = POINTER(drmaa2_jsession_s)

class drmaa2_version_s(Structure):
    _fields_ = [('major', STRING), ('minor', STRING)]

drmaa2_version = POINTER(drmaa2_version_s)

class drmaa2_jtemplate_s(Structure):
    _fields_ = [
        ('remoteCommand', STRING),
        ('args', drmaa2_string_list),
        ('submitAsHold', c_int),
        ('rerunnable', c_int),
        ('jobEnvironment', drmaa2_dict),
        ('workingDirectory', STRING),
        ('jobCategory', STRING),
        ('email', drmaa2_string_list),
        ('emailOnStarted', c_int),
        ('emailOnTerminated', c_int),
        ('jobName', STRING),
        ('inputPath', STRING),
        ('outputPath', STRING),
        ('errorPath', STRING),
        ('joinFiles', c_int),
        ('reservationId', STRING),
        ('queueName', STRING),
        ('minSlots', c_longlong),
        ('maxSlots', c_longlong),
        ('priority', c_longlong),
        ('candidateMachines', drmaa2_string_list),
        ('minPhysMemory', c_longlong),
        ('machineOS', c_void_p), ## TODO drmaa2_os
        ('machineArch', c_void_p), ## TODO drmaa2_cpu
        ('startTime', c_void_p), ## TODO time_t
        ('deadlineTime', c_void_p), ## TODO time_t
        ('stageInFiles', drmaa2_dict),
        ('stageOutFiles', drmaa2_dict),
        ('resourceLimits', drmaa2_dict),
        ('accountingId', STRING),
        ('implementationSpecific', c_void_p),
    ]

drmaa2_jtemplate = POINTER(drmaa2_jtemplate_s)

class drmaa2_j_s(Structure):
    _fields_ = []

drmaa2_j = POINTER(drmaa2_j_s)

drmaa2_create_jsession = _lib.drmaa2_create_jsession
drmaa2_create_jsession.restype = drmaa2_jsession
drmaa2_create_jsession.argtypes = [STRING, STRING]
drmaa2_close_jsession = _lib.drmaa2_close_jsession
drmaa2_close_jsession.argtypes = [drmaa2_jsession]
drmaa2_close_jsession.restype = error_check

def init(contact=None, name=None):
    session = drmaa2_create_jsession(name, contact)
    err = drmaa2_lasterror()
    if err != 0:
        error_check(err)
    return session


def exit(session):
    return drmaa2_close_jsession(session)


drmaa2_jsession_free = _lib.drmaa2_jsession_free
drmaa2_jsession_free.argtypes = [drmaa2_jsession]
drmaa2_jsession_free.restype = error_check
drmaa2_get_drmaa_name = _lib.drmaa2_get_drmaa_name
drmaa2_get_drmaa_name.restype = STRING
drmaa2_get_drmaa_version = _lib.drmaa2_get_drmaa_version
drmaa2_get_drmaa_version.restype = drmaa2_version
drmaa2_jtemplate_create = _lib.drmaa2_jtemplate_create
drmaa2_jtemplate_create.restype = drmaa2_jtemplate
drmaa2_jsession_run_job = _lib.drmaa2_jsession_run_job
drmaa2_jsession_run_job.argtypes = [drmaa2_jsession, drmaa2_jtemplate]
drmaa2_jsession_run_job.restype = drmaa2_j

