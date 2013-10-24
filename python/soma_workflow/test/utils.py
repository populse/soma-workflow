# -*- coding: utf-8 -*-
"""
Created on Thu Oct 17 14:17:27 2013

@author: laure.hugo@cea.fr
"""
#-----------------------------------------------------------------------------
# Imports
#-----------------------------------------------------------------------------

import sys
import os
from contextlib import contextmanager


#-----------------------------------------------------------------------------
# Functions
#-----------------------------------------------------------------------------

def select_resources(resource_ids):
    sys.stdout.write("Configured resources:\n")
    for i in range(0, len(resource_ids)):
        sys.stdout.write(
            "  " + repr(i) + " -> " + repr(resource_ids[i]) + "\n")
    sys.stdout.write("Select a resource number: ")
    resource_index = int(sys.stdin.readline())
    resource_id = resource_ids[resource_index]
    sys.stdout.write("Selected resource => " + repr(resource_id) + "\n")
    sys.stdout.write("---------------------------------\n")
    return resource_id


def get_user_id(config):
    login = None
    password = None
    if config.get_mode() == 'remote':
        sys.stdout.write("This is a remote connection\n")
        sys.stdout.write("login:")
        login = sys.stdin.readline()
        login = login.rstrip()
        sys.stdout.write("password:")
        password = sys.stdin.readline()
        password = password.rstrip()
        #password = getpass.getpass()
        sys.stdout.write("Login => " + repr(login) + "\n")
    return (login, password)


def select_workflow_type(wf_types):
    sys.stdout.write("Workflow example to test: \n")
    sys.stdout.write("all -> all \n")
    for i in range(0, len(wf_types)):
        sys.stdout.write("  " + repr(i) + " -> " + repr(wf_types[i]) + "\n")
    sys.stdout.write("Select one or several workflow(s) : \n")
    selected_wf_type_indexes = []
    line = sys.stdin.readline()
    line = line.rstrip()
    if line == "all":
        selected_wf_type = wf_types
        sys.stdout.write("Selected workflow(s): all \n")
    else:
        for strindex in line.split(" "):
            selected_wf_type_indexes.append(int(strindex))
        selected_wf_type = []
        sys.stdout.write("Selected workflow(s): \n")
        for wf_type_index in selected_wf_type_indexes:
            selected_wf_type.append(wf_types[int(wf_type_index)])
            sys.stdout.write(
                "  => " + repr(wf_types[int(wf_type_index)]) + "\n")
    sys.stdout.write("---------------------------------\n")
    return selected_wf_type


def select_file_path_type(path_types):
    sys.stdout.write("File path type: \n")
    sys.stdout.write("all -> all \n")
    for i in range(0, len(path_types)):
        sys.stdout.write("  " + repr(i) + " -> " + repr(path_types[i]) + "\n")
    sys.stdout.write("Select one or several path type : \n")
    selected_path_type_indexes = []
    line = sys.stdin.readline()
    line = line.rstrip()
    if line == "all":
        selected_path_type = path_types
        sys.stdout.write("Selected path type: all \n")
    else:
        for strindex in line.split(" "):
            selected_path_type_indexes.append(int(strindex))
        selected_path_type = []
        sys.stdout.write("Selected path types: \n")
        for path_type_index in selected_path_type_indexes:
            selected_path_type.append(path_types[int(path_type_index)])
            sys.stdout.write(
                "  => " + repr(path_types[int(path_type_index)]) + "\n")
    sys.stdout.write("---------------------------------\n")
    return selected_path_type


def select_test_type(test_types):
    sys.stdout.write("Tests to perform: \n")
    sys.stdout.write("all -> all \n")
    for i in range(0, len(test_types)):
        sys.stdout.write("  " + repr(i) + " -> " + repr(test_types[i]) + "\n")
    sys.stdout.write("Select one or several test : \n")
    selected_test_type_indexes = []
    line = sys.stdin.readline()
    line = line.rstrip()
    if line == "all":
        selected_test_type = test_types
        sys.stdout.write("Selected test types: all \n")
    else:
        for strindex in line.split(" "):
            selected_test_type_indexes.append(int(strindex))
        selected_test_type = []
        sys.stdout.write("Selected test types: \n")
        for test_type_index in selected_test_type_indexes:
            selected_test_type.append(test_types[int(test_type_index)])
            sys.stdout.write(
                "  => " + repr(test_types[int(test_type_index)]) + "\n")
    sys.stdout.write("---------------------------------\n")
    return selected_test_type


@contextmanager
def suppress_stdout(debug=False):
    '''Suppress momentarily the stdout'''
    if debug:
        yield
    else:
        with open(os.devnull, "w") as devnull:
            old_stdout = sys.stdout
            sys.stdout = devnull
            try:
                yield
            finally:
                sys.stdout = old_stdout
