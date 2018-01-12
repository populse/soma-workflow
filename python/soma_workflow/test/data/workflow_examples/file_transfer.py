import time

from soma_workflow.client import Job, Workflow, WorkflowController, Helper, FileTransfer
# from soma_workflow.connection import RemoteConnection

controller = WorkflowController("Gabriel", "mb253889")

# FileTransfer creation for input files
file1 = FileTransfer(is_input=True,
                     client_path="/home/mb253889/create_file.py",
                     name="script")

file2 = FileTransfer(is_input=True,
                     client_path="/home/mb253889/output_file",
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