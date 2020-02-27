# -*- coding: utf-8 -*-

"""DRMAA2 C library function wrappers"""

from __future__ import absolute_import
from __future__ import print_function
from ctypes import *
from ctypes.util import find_library
from soma_workflow.utils import DetectFindLib
from somadrmaa.errors import DrmaaException
import os
from six.moves import range


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


drmaa2_list_entryfree = CFUNCTYPE(None, POINTER(c_void_p))


class drmaa2_list_s(Structure):
    _fields_ = [
        ('free_callback', drmaa2_list_entryfree),
        ('type', c_int),
        ('size', c_long),
        ('head', c_void_p),
    ]
drmaa2_list = POINTER(drmaa2_list_s)
drmaa2_string_list = drmaa2_list

# list types
DRMAA2_UNSET_LISTTYPE = -1
DRMAA2_STRINGLIST = 0
DRMAA2_JOBLIST = 1
DRMAA2_QUEUEINFOLIST = 2
DRMAA2_MACHINEINFOLIST = 3
DRMAA2_SLOTINFOLIST = 4
DRMAA2_RESERVATIONLIST = 5


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
        ('machineOS', c_void_p),  # TODO drmaa2_os
        ('machineArch', c_void_p),  # TODO drmaa2_cpu
        ('startTime', c_void_p),  # TODO time_t
        ('deadlineTime', c_void_p),  # TODO time_t
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


def init(contact='', name=''):
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
drmaa2_j_wait_terminated = _lib.drmaa2_j_wait_terminated
drmaa2_j_wait_terminated.argtypes = [drmaa2_j, c_int]
drmaa2_j_wait_terminated.restype = error_check
drmaa2_list_create = _lib.drmaa2_list_create
drmaa2_list_create.argtypes = [c_int, drmaa2_list_entryfree]
drmaa2_list_create.restype = drmaa2_list
drmaa2_list_add = _lib.drmaa2_list_add
drmaa2_list_add.argtypes = [drmaa2_list, STRING]
    ## WARNING: using STRING, should be c_void_p
drmaa2_list_add.restype = error_check
drmaa2_string_list_default_callback = _lib.drmaa2_string_list_default_callback
drmaa2_string_list_default_callback.argtypes = [POINTER(c_void_p)]
drmaa2_string_list_default_callback.restype = None


def drmaa2_string_list_default_callback_py(item):
    drmaa2_string_list_default_callback(item)
drmaa2_string_list_default_callback_c = drmaa2_list_entryfree(
    drmaa2_string_list_default_callback_py)
drmaa2_list_size = _lib.drmaa2_list_size
drmaa2_list_size.argtypes = [drmaa2_list]
drmaa2_list_size.restype = c_long
drmaa2_list_free = _lib.drmaa2_list_free
drmaa2_list_free.argtypes = [POINTER(drmaa2_list)]  # FIXME: causes segfault
drmaa2_list_free.restype = None
drmaa2_list_get = _lib.drmaa2_list_get
drmaa2_list_get.argtypes = [drmaa2_list, c_long]
drmaa2_list_get.restype = c_void_p
drmaa2_string_list_get = _lib.drmaa2_list_get
drmaa2_string_list_get.argtypes = [drmaa2_list, c_long]
drmaa2_string_list_get.restype = STRING


def drmaa2_make_string_list(str_list):
    dlist = drmaa2_list_create(
        DRMAA2_STRINGLIST, drmaa2_string_list_default_callback_c)
    for i in range(len(str_list)):
        item = str_list[-i - 1]
        print('insert item:', item)
        # insertion is at the beginning of the list
        drmaa2_list_add(dlist, item)
    return dlist


def drmaa2_list_from_string_list(drmaa2_str_list):
    pylist = []
    for i in range(drmaa2_list_size(drmaa2_str_list)):
        if drmaa2_str_list.contents.type == DRMAA2_STRINGLIST:
            item = drmaa2_string_list_get(drmaa2_str_list, i)
        else:
            item = drmaa2_list_get(drmaa2_str_list, i)
        pylist.append(item)
    return pylist
