# -*- coding: utf-8 -*-
"""
Created on Thu Oct 17 14:17:27 2013

@author: laure.hugo@cea.fr
"""
#-----------------------------------------------------------------------------
# Imports
#-----------------------------------------------------------------------------

#import unittest
#import os
#import getpass
import sys

#from soma_workflow.client import Job
#from soma_workflow.client import SharedResourcePath
#from soma_workflow.client import FileTransfer
#from soma_workflow.client import Group
#from soma_workflow.client import Workflow
#from soma_workflow.client import WorkflowController
#from soma_workflow.client import Helper
from soma_workflow.configuration import Configuration
#from soma_workflow.errors import ConfigurationError
#from soma_workflow.errors import UnknownObjectError
#from soma_workflow.utils import checkFiles
#from soma_workflow.utils import identicalFiles
#import soma_workflow.constants as constants
#from soma_workflow.test.test_workflow_examples import WorkflowExamples


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


def select_configuration(resource_id, config_file_path):
    login = None
    password = None
    config = Configuration.load_from_file(resource_id, config_file_path)

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
        sys.stdout.write("---------------------------------\n")
    return (login, password)


def select_workflow_type(wf_types):
    wf_types = ["multiple", "special command"]
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
