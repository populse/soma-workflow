from soma_workflow.client import Job, Workflow, WorkflowController

job_1 = Job(command=["sleep", "60"], name="job 1")
job_2 = Job(command=["sleep", "60"], name="job 2")
job_3 = Job(command=["sleep", "60"], name="job 3")
job_4 = Job(command=["sleep", "60"], name="job 4")

jobs = [job_1, job_2, job_3, job_4]
dependencies = [(job_1, job_2),
                (job_1, job_3),
                (job_2, job_4),
                (job_3, job_4)]

workflow = Workflow(jobs=jobs,
                    dependencies=dependencies)


controller = WorkflowController("DSV_cluster", login, password)

controller.submit_workflow(workflow=workflow,
                           name="simple example")
