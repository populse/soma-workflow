from soma_workflow.client import Job, Workflow, WorkflowController, SharedResourcePath, FileTransfer

# SharedResourcePath creation for the input file.
# The input file is read direclty in the data directory located on the
# the computing resource side.
myfile = SharedResourcePath(relative_path="myfile",
                            namespace="MyApp",
                            uuid="my_example_dir")

# FileTransfer creation for the output file.
# That way the output file will not be written in the data directory
# located on the computing resource file system.
copy_of_myfile = FileTransfer(is_input=False,
                              client_path="/tmp/soma_workflow_examples/copy_of_myfile",
                              name="copy of my file")

# Job and Workflow creation
copy_job = Job(command=["cp", myfile, copy_of_myfile],
               name="copy",
               referenced_input_files=[],
               referenced_output_files=[copy_of_myfile])

workflow = Workflow(jobs=[copy_job],
                    dependencies=[])

# workflow submission
controller = WorkflowController("DSV_cluster", login, password)

controller.submit_workflow(workflow=workflow,
                           name="shared resource path example")
