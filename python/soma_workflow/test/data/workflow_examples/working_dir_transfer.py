# -*- coding: utf-8 -*-
from __future__ import print_function

from __future__ import absolute_import
from soma_workflow.client import Job, Workflow, WorkflowController, FileTransfer

import getpass

import os

# workind directory creation (this is not a part of the example)
my_working_directory = "/tmp/my_working_directory"
if not os.path.isdir(my_working_directory):
    os.mkdir(my_working_directory)
f = open("/tmp/my_working_directory/myfile1", "wb")
f.write("Content of my file1 \n")
f.close()
f = open("/tmp/my_working_directory/myfile2", "wb")
f.write("Content of my file2 \n")
f.close()

# Creation of the FileTransfer object to transfer the working directory
my_working_directory = FileTransfer(is_input=True,
                                    client_path="/tmp/my_working_directory",
                                    name="working directory")

# Jobs and Workflow
job1 = Job(command=["cp", "myfile1", "copy_of_myfile1"],
           name="job1",
           referenced_input_files=[my_working_directory],
           referenced_output_files=[my_working_directory],
           working_directory=my_working_directory)

job2 = Job(command=["cp", "myfile2", "copy_of_myfile2"],
           name="job2",
           referenced_input_files=[my_working_directory],
           referenced_output_files=[my_working_directory],
           working_directory=my_working_directory)

workflow = Workflow(jobs=[job1, job2],
                    dependencies=[])

# Submit the workflow
print("password? ")
login = 'myself'
password = getpass.getpass()
controller = WorkflowController("DSV_cluster", login, password)

controller.submit_workflow(workflow=workflow,
                           name="working directory transfer example")
