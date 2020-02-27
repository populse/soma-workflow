# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import print_function
import time
import os

from soma_workflow.client import Job, Workflow, WorkflowController, Helper, FileTransfer
from soma_workflow.configuration import Configuration
# from soma_workflow.connection import RemoteConnection

user = 'nobody'
try:
    import pwd
    user = pwd.getpwuid(os.getuid()).pw_name
except Exception:
    pass

controller = WorkflowController("Gabriel", user)

# FileTransfer creation for input files
file1 = FileTransfer(is_input=True,
                     client_path="%s/create_file.py"
                     % Configuration.get_home_dir(),
                     name="script")

file2 = FileTransfer(is_input=True,
                     client_path="%s/output_file"
                     % Configuration.get_home_dir(),
                     name="file created on the server")

# Job and Workflow
run_script = Job(command=["python", file1, file2],
                 name="copy",
                 referenced_input_files=[file1],
                 referenced_output_files=[file2])

workflow = Workflow(jobs=[run_script],
                    dependencies=[])

workflow_id = controller.submit_workflow(workflow=workflow,
                                         name="Simple transfer")

# You may use the gui or manually transfer the files:
manual = True
if manual:
    Helper.transfer_input_files(workflow_id, controller)
    Helper.wait_workflow(workflow_id, controller)
    Helper.transfer_output_files(workflow_id, controller)

# RemoteConnection.kill_remote_servers("Gabriel")
print("Finished !!!")
