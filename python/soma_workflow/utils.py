# -*- coding: utf-8 -*-
'''
@author: Soizic Laguitton

@organization: I2BM, Neurospin, Gif-sur-Yvette, France
@organization: CATI, France
@organization: U{IFR 49<http://www.ifr49.org>}

@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''

from __future__ import print_function


from __future__ import absolute_import
import copy
import os
from soma_workflow.client import Workflow, Group, Job, Helper
from soma_workflow.configuration import cpu_count, default_cpu_number
import soma_workflow.client_types as sct
import six

try:
    from traits.api import Undefined
except ImportError:
    class Undefined(object):
        pass


def DetectFindLib(env_name, libname):
    '''Try to find libname using ctype

    '''
    import ctypes
    from ctypes.util import find_library
    from ctypes import CDLL
    from ctypes import cdll
    import glob

    libpath = None
    IS_LIB_FOUND = True
    _lib = None

    if env_name in os.environ:
        libpath = os.environ[env_name]
    else:
        libpath = find_library(libname)

    libsoname = "lib" + libname + ".so"
    if "LD_LIBRARY_PATH" in os.environ:
        ld_lib_paths = os.environ["LD_LIBRARY_PATH"]
        ld_lib_paths = ld_lib_paths.split(os.pathsep)
        for ld_lib_path in ld_lib_paths:
            files = glob.glob(os.path.join(ld_lib_path, libsoname))
            if(len(files) >= 1):
                libpath = files[0]
                break

    if libpath is None:
#         try:
#             soname="lib"+libname+".so"
#             _lib=cdll.LoadLibrary(soname,mode=ctypes.RTLD_GLOBAL)
#         except OSError, e:
#             IS_LIB_FOUND=False
#         except:
#             IS_LIB_FOUND=False
        IS_LIB_FOUND = False
    else:
        _lib = CDLL(libpath, mode=ctypes.RTLD_GLOBAL)

    return (IS_LIB_FOUND, _lib)


def process_group(group, to_remove, name):
    new_group = []
    for element in group:
        if isinstance(element, Job):
            if element in to_remove:
                if not to_remove[element] in new_group:
                    new_group.append(to_remove[element])
            else:
                new_group.append(element)
        else:

            new_group.append(
                process_group(element.elements, to_remove, element.name))
    return Group(new_group, name)


class SerialJob(object):

    _job_sequence = None
    _input_dep = None
    _output_dep = None
    _working_directory = None
    _group = None

    def __init__(self):
        self._job_sequence = []
        self._input_dep = []
        self._output_dep = []
        self._working_directory = None
        self._group = None

    def job_sequence(self):
        return self._job_sequence

    def input_dep(self):
        return self._input_dep

    def output_dep(self):
        return self._output_dep

    def working_directory(self):
        return self._working_directory

    def group(self):
        return self._group

    def is_valid(self):
        return len(self._job_sequence) > 1

    def add_job(self, job, group, input_dep, output_dep):
        assert self.is_consistent(job, group)

        if not self._job_sequence:
            self._working_directory = job.working_directory
            self._group = group
            self._input_dep = input_dep

        self._job_sequence.append(job)
        self._output_dep = output_dep

    def is_consistent(self, job, group):
        if not self._job_sequence:
            return True

        is_consistent = job.working_directory == self._working_directory and \
            group == self._group and \
            job.stdin == None and \
            job.stdout_file == None and \
            job.stderr_file == None and \
            job.parallel_job_info == None
        return is_consistent


def explore(root_job,
            current_serial_job,
            serial_jobs,
            explored,
            workflow):

    if root_job in explored:
        return

    input_dep = []
    output_dep = []
    for dep in workflow.dependencies:
        if dep[0] == root_job:
            output_dep.append(dep[1])
        elif dep[1] == root_job:
            input_dep.append(dep[0])

    group = None
    if root_job in workflow.root_group:
        group = workflow.root_group
    else:
        for gp in workflow.groups:
            if root_job in gp.elements:
                group = gp
                break

    explored.add(root_job)

    if len(input_dep) == 1:
        if current_serial_job.is_consistent(root_job, group):
            current_serial_job.add_job(root_job, group, input_dep, output_dep)
        else:
            if current_serial_job.is_valid():
                serial_jobs.append(current_serial_job)
            current_serial_job = SerialJob()
            current_serial_job.add_job(root_job, group, input_dep, output_dep)

        if len(output_dep) > 1:
            if current_serial_job.is_valid():
                serial_jobs.append(current_serial_job)
            for job in output_dep:
                current_serial_job = SerialJob()
                explore(job,
                        current_serial_job,
                        serial_jobs,
                        explored,
                        workflow)
        elif len(output_dep) == 0:
            if current_serial_job.is_valid():
                serial_jobs.append(current_serial_job)
        elif len(output_dep) == 1:
            explore(output_dep[0],
                    current_serial_job,
                    serial_jobs,
                    explored,
                    workflow)

    elif len(input_dep) == 0:
        if len(output_dep) == 1:
            current_serial_job = SerialJob()
            current_serial_job.add_job(root_job, group, input_dep, output_dep)
            explore(output_dep[0],
                    current_serial_job,
                    serial_jobs,
                    explored,
                    workflow)
        elif len(output_dep) > 1:
            for job in output_dep:
                current_serial_job = SerialJob()
                explore(job,
                        current_serial_job,
                        serial_jobs,
                        explored,
                        workflow)

    elif len(input_dep) > 1:
        if current_serial_job.is_valid():
            serial_jobs.append(current_serial_job)
        current_serial_job = SerialJob()
        if len(output_dep) == 1:
            current_serial_job.add_job(root_job, group, input_dep, output_dep)
            explore(output_dep[0],
                    current_serial_job,
                    serial_jobs,
                    explored,
                    workflow)

        elif len(output_dep) > 1:
            for job in output_dep:
                current_serial_job = SerialJob()
                explore(job,
                        current_serial_job,
                        serial_jobs,
                        explored,
                        workflow)


def to_json(value):
    '''
    Convert value to an object which will mark some types through JSON
    serialization. Typically, tuples will be replaced with lists which firsst
    element is '<tuple>', Undefined with ['<undefined'], sets with ['<set>,
    items], etc.

    "Decding" can be done using :func:`from_json`
    '''
    if isinstance(value, tuple):
        value = ['<tuple>'] + [to_json(x) for x in value]
    if isinstance(value, set):
        value = ['<set>'] + [to_json(x) for x in value]
    elif isinstance(value, list):
        value = [to_json(x) for x in value]
    elif hasattr(value, 'items'):
        new_value = {}
        for key, item in six.iteritems(value):
            new_value[key] = to_json(item)
        value = new_value
    elif value is Undefined:
        value = ['<undefined>']
    elif isinstance(value, sct.SpecialPath):
        # force back to standard string type
        # otherwise json refuses to serialize them
        if hasattr(value, 'get_engine_main_path'):
            value = value.get_engine_main_path()
        elif hasattr(value, 'client_path'):
            # get_EngineTemporaryPath
            value = value.client_path()
        else:
            value = '<special_path>'
    elif isinstance(value, bytes):
        value = six.ensure_str(value)
    return value


def from_json(value):
    '''
    Reverse of :func:`to_json`

    Convert value from an object which matches JSON serialization, containing
    "code" for some types. Typically, tuples, sets, Undefined, etc.
    '''
    if hasattr(value, 'items'):
        new_value = type(value)()
        for key, item in six.iteritems(value):
            new_value[key] = from_json(item)
        return new_value
    if not isinstance(value, list):
        return value
    if len(value) < 1:
        return value
    code = value[0]
    if code == '<tuple>':
        return tuple([from_json(x) for x in value[1:]])
    elif code == '<undefined>':
        return Undefined
    elif code == '<set>':
        return set([from_json(x) for x in value[1:]])
    return [from_json(x) for x in value]
