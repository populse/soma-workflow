# -*- coding: utf-8 -*-
from __future__ import absolute_import
from soma_workflow.client import Job, Workflow, Group, WorkflowController
from six.moves import range

jobs = []
dependencies = []
group_elements = []

first_job = Job(command=["sleep", "10"], name="first job")
last_job = Job(command=["sleep", "10"], name="last job")

jobs.append(first_job)
jobs.append(last_job)

for i in range(0, 30):
    job = Job(command=["sleep", "60"], name="job " + repr(i))

    jobs.append(job)

    dependencies.append((first_job, job))
    dependencies.append((job, last_job))

    group_elements.append(job)


thirty_jobs_group = Group(elements=group_elements,
                          name="my 30 jobs")

workflow = Workflow(jobs=jobs,
                    dependencies=dependencies,
                    root_group=[first_job, thirty_jobs_group, last_job])

login = 'myself'
password = 'carrot'
controller = WorkflowController("DSV_cluster", login, password)

controller.submit_workflow(workflow=workflow,
                           name="Simple workflow with group")
