from __future__ import with_statement

# -*- coding: utf-8 -*-
"""
Created on Mon Oct 21 14:05:37 2013

@author: jinpeng.li@cea.fr
@author: laure.hugo@cea.fr
"""

import os
from abc import abstractmethod
from soma_workflow.client import Group
from soma_workflow.client import Workflow
from soma_workflow.errors import ConfigurationError





class WorkflowExamples(object):

    @staticmethod
    def get_workflow_example_list():
        return (
            ["simple",
             "multiple",
             "with exception 1",
             "with exception 2",
             "command check test",
             "special transfers",
             "hundred of jobs",
             "ten jobs",
             "fake pipelineT1",
             "serial",
             "hundred with dependencies",
             "thousands",
             "thousands with dependencies",
             "native specification for PBS",
             "wrong native specification for PBS"]
        )

    def __init__(self):
        '''
        @type with_transfers: boolean
        @type with_shared_resource_path: boolean

        if with_transfer and not with_shared_resource_path:
            The input and ouput files are temporary files on the computing
            resource and these files can be transfered from and to the
            computing resource using soma workflow API

        if with_shared_resource_path and not with_transfers:
            The files are read and written on the computing resource.

        if with_shared_resource_path and with_transfer:
            The files are read from data located on the computing resource but
            the output will be written in temporary files and transfered to the
            computing resource.
        '''
        # Define example directories
        import soma_workflow
        self.examples_dir = os.path.join(soma_workflow.__path__[0],
                                         "..", "..", "test", "jobExamples")
        self.output_dir = os.path.join(soma_workflow.__path__[0],
                                       "..", "..", "test", "out")
        if not os.path.isdir(self.output_dir):
            os.mkdir(self.output_dir)
        if (not os.path.isdir(self.examples_dir) or
                not os.path.isdir(self.output_dir)):
            raise ConfigurationError("%s or %s does not exist." % (
                                     self.examples_dir,
                                     self.output_dir))

    def get_workflow_example(self, example_index):
        workflow = None
        if example_index == 0:
            workflow = self.simple_example()
        elif example_index == 1:
            workflow = self.multiple_simple_example()
        elif example_index == 2:
            workflow = self.simple_example_with_exception1()
        elif example_index == 3:
            workflow = self.simple_example_with_exception2()
        elif example_index == 4:
            workflow = self.special_command()
        elif example_index == 5:
            workflow = self.special_transfer()
        elif example_index == 6:
            workflow = self.n_jobs(100)
        elif example_index == 7:
            workflow = self.n_jobs(10)
        elif example_index == 8:
            workflow = self.fake_pipelineT1()
        elif example_index == 9:
            workflow = self.serial_jobs()
        elif example_index == 10:
            workflow = self.n_jobs_with_dependencies(500)
        elif example_index == 11:
            workflow = self.n_jobs(12000)
        elif example_index == 12:
            workflow = self.n_jobs_with_dependencies(2000)
        elif example_index == 13:
            workflow = self.native_spec_pbs()
        elif example_index == 14:
            workflow = self.wrong_native_spec_pbs()
        return workflow

    @abstractmethod
    def job1(self):
        pass

    @abstractmethod
    def job2(self):
        pass

    @abstractmethod
    def job3(self):
        pass

    @abstractmethod
    def job4(self):
        pass

    @abstractmethod
    def job_test_command_1(self):
        pass

    @abstractmethod
    def job_test_dir_contents(self):
        pass

    @abstractmethod
    def job_test_multi_file_format(self):
        pass

    @abstractmethod
    def job_sleep(self, period):
        pass

    @abstractmethod
    def job1_exception(self):
        pass

    @abstractmethod
    def job3_exception(self):
        pass

    def special_transfer(self):
       # jobs
        test_dir_contents = self.job_test_dir_contents()
        test_multi_file_format = self.job_test_multi_file_format()
        # building the workflow
        jobs = [test_dir_contents, test_multi_file_format]
        dependencies = []

        workflow = Workflow(jobs, dependencies)
        return workflow

    def special_command(self):
        # jobs
        test_command_job = self.job_test_command_1()
        # building the workflow
        jobs = [test_command_job]

        dependencies = []

        workflow = Workflow(jobs, dependencies)
        return workflow

    def simple_example(self):
        # jobs
        job1 = self.job1()
        job2 = self.job2()
        job3 = self.job3()
        job4 = self.job4()
        # building the workflow
        jobs = [job1, job2, job3, job4]

        dependencies = [(job1, job2),
                        (job1, job3),
                        (job2, job4),
                        (job3, job4)]

        group_1 = Group(name='group_1', elements=[job2, job3])
        group_2 = Group(name='group_2', elements=[job1, group_1])

        workflow = Workflow(jobs, dependencies, root_group=[group_2, job4])
        return workflow

    def wrong_native_spec_pbs(self):
       # jobs
        job1 = self.job1(option="-l walltime=5:00:00, pmem=16gb")
        job2 = self.job1(option="-l walltime=5:00:0")
        job3 = self.job1()
        # building the workflow
        jobs = [job1, job2, job3]

        workflow = Workflow(jobs, dependencies=[],
                            name="jobs with wrong native spec for pbs")
        return workflow

    def native_spec_pbs(self):
       # jobs
        job1 = self.job1(option="-l walltime=5:00:00,pmem=16gb")
        job2 = self.job1(option="-l walltime=5:00:0")
        job3 = self.job1()
        # building the workflow
        jobs = [job1, job2, job3]

        workflow = Workflow(jobs, dependencies=[],
                            name="jobs with native spec for pbs")
        return workflow

    def simple_example_with_exception1(self):
        # jobs
        job1 = self.job1_exception()
        job2 = self.job2()
        job3 = self.job3()
        job4 = self.job4()
        jobs = [job1, job2, job3, job4]

        dependencies = [(job1, job2),
                        (job1, job3),
                        (job2, job4),
                        (job3, job4)]

        group_1 = Group(name='group_1', elements=[job2, job3])
        group_2 = Group(name='group_2', elements=[job1, group_1])

        workflow = Workflow(jobs, dependencies, root_group=[group_2, job4])
        return workflow

    def simple_example_with_exception2(self):
        # jobs
        job1 = self.job1()
        job2 = self.job2()
        job4 = self.job4()
        job3 = self.job3_exception()
        jobs = [job1, job2, job3, job4]

        dependencies = [(job1, job2),
                        (job1, job3),
                        (job2, job4),
                        (job3, job4)]

        group_1 = Group(name='group_1', elements=[job2, job3])
        group_2 = Group(name='group_2', elements=[job1, group_1])

        workflow = Workflow(jobs, dependencies, root_group=[group_2, job4])
        return workflow

    def multiple_simple_example(self):
        workflow1 = self.simple_example()
        workflow2 = self.simple_example_with_exception1()
        workflow3 = self.simple_example_with_exception2()

        jobs = workflow1.jobs
        jobs.extend(workflow2.jobs)
        jobs.extend(workflow3.jobs)

        dependencies = workflow1.dependencies
        dependencies.extend(workflow2.dependencies)
        dependencies.extend(workflow3.dependencies)

        group1 = Group(name="simple example", elements=workflow1.root_group)
        group2 = Group(name="simple with exception in Job1",
                       elements=workflow2.root_group)
        group3 = Group(name="simple with exception in Job3",
                       elements=workflow3.root_group)

        workflow = Workflow(jobs, dependencies,
                            root_group=[group1, group2, group3])
        return workflow

    def n_jobs(self, nb=300, time=60):
        jobs = []
        for i in range(0, nb):
            job = self.job_sleep(time)
            jobs.append(job)

        dependencies = []

        workflow = Workflow(jobs, dependencies)
        return workflow

    def n_jobs_with_dependencies(self, nb=500, time=60):
        dependencies = []
        jobs = []
        intermed_job1 = self.job_sleep(2)
        jobs.append(intermed_job1)
        intermed_job2 = self.job_sleep(2)
        jobs.append(intermed_job2)

        elem_group1 = []
        for i in range(0, nb):
            job = self.job_sleep(time)
            jobs.append(job)
            elem_group1.append(job)
            dependencies.append((job, intermed_job1))
        group1 = Group(name="Group 1", elements=elem_group1)

        elem_group2 = []
        for i in range(0, nb):
            job = self.job_sleep(time)
            jobs.append(job)
            elem_group2.append(job)
            dependencies.append((intermed_job1, job))
            dependencies.append((job, intermed_job2))
        group2 = Group(name="Group 2", elements=elem_group2)

        elem_group3 = []
        for i in range(0, nb):
            job = self.job_sleep(time)
            jobs.append(job)
            elem_group3.append(job)
            dependencies.append((intermed_job2, job))
        group3 = Group(name="Group 3", elements=elem_group3)

        root_group = [group1, intermed_job1, group2, intermed_job2, group3]
        workflow = Workflow(jobs, dependencies, root_group)
        return workflow

    def serial_jobs(self, nb=5):
        jobs = []
        dependencies = []
        previous_job = self.job_sleep(60)
        jobs.append(previous_job)
        for i in range(0, nb):
            job = self.job_sleep(60)
            jobs.append(job)
            dependencies.append((previous_job, job))
            previous_job = job

        workflow = Workflow(jobs, dependencies)
        return workflow

    def fake_pipelineT1(self):
        jobs = []
        dependencies = []
        root_group = []
        for i in range(0, 100):
            job1 = self.job_sleep(60)
            job1.name = "Brain extraction"
            jobs.append(job1)

            job11 = self.job_sleep(1)
            job11.name = "test 1"
            jobs.append(job11)
            job12 = self.job_sleep(1)
            job12.name = "test 2"
            jobs.append(job12)
            job13 = self.job_sleep(1)
            job13.name = "test 3"
            jobs.append(job13)

            job2 = self.job_sleep(120)
            job2.name = "Gray/white segmentation"
            jobs.append(job2)
            job3 = self.job_sleep(400)
            job3.name = "Left hemisphere sulci recognition"
            jobs.append(job3)
            job4 = self.job_sleep(400)
            job4.name = "Right hemisphere sulci recognition"
            jobs.append(job4)

            #dependencies.append((job1, job2))
            dependencies.append((job1, job11))
            dependencies.append((job11, job12))
            dependencies.append((job12, job13))
            dependencies.append((job13, job2))
            dependencies.append((job2, job3))
            dependencies.append((job2, job4))

            group_sulci = Group(name="Sulci recognition",
                                elements=[job3, job4])
            group_subject = Group(
                name="sulci recognition -- subject " + repr(i),
                elements=[job1, job11, job12, job13, job2, group_sulci])

            root_group.append(group_subject)
        workflow = Workflow(jobs, dependencies, root_group)
        return workflow
