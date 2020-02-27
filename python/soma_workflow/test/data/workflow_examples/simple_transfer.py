# -*- coding: utf-8 -*-
from __future__ import absolute_import
from soma_workflow.client import Job, Workflow, WorkflowController, FileTransfer
# import os

# file creation (it is not a part of the example)
# example_directory = "/tmp/soma_workflow_examples"
# if not os.path.isdir(example_directory):
  # os.mkdir(example_directory)

# f = open("/tmp/soma_workflow_examples/myfile", "wb")
# f.write("Content of my file \n")
# f.close()


# FileTransfer creation for input files
myfile = FileTransfer(is_input=True,
                      client_path="/tmp/soma_workflow_examples/myfile",
                      name="myfile")

# FileTransfer creation for output files
copy_of_myfile = FileTransfer(is_input=False,
                              client_path="/tmp/soma_workflow_examples/copy_of_myfile",
                              name="copy of my file")

# Job and Workflow
copy_job = Job(command=["cp", myfile, copy_of_myfile],
               name="copy",
               referenced_input_files=[myfile],
               referenced_output_files=[copy_of_myfile])

workflow = Workflow(jobs=[copy_job],
                    dependencies=[])


login = 'myself'
password = 'carrot'
controller = WorkflowController("DSV_cluster", login, password)

controller.submit_workflow(workflow=workflow,
                           name="simple transfer")
