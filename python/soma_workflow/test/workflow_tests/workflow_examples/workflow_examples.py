# -*- coding: utf-8 -*-
from __future__ import with_statement, print_function
from __future__ import absolute_import

# -*- coding: utf-8 -*-
"""
Created on Mon Oct 21 14:05:37 2013

@author: jinpeng.li@cea.fr
@author: laure.hugo@cea.fr
@author: Soizic Laguitton
@organization: U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
"""

from six.moves import range
from six.moves import zip
import os
import inspect
from abc import abstractmethod
from soma_workflow.client import Group
from soma_workflow.client import Workflow
from soma_workflow.client import BarrierJob
from soma_workflow.client import MapJob
from soma_workflow.client import ReduceJob
from soma_workflow.client import LeaveOneOutJob
from soma_workflow.client import CrossValidationFoldJob
from soma_workflow.errors import ConfigurationError
import tempfile
import warnings


class WorkflowExamples(object):

    def __init__(self):
        # Define example directories
        import soma_workflow
        module_file = soma_workflow.__file__
        if module_file.endswith('.pyc') or module_file.endswith('.pyo'):
            module_file = module_file[: -1]
        module_file = os.path.realpath(module_file)
        module_path = os.path.dirname(module_file)
        self.examples_dir = os.path.join(module_path,
                                         "test", "data", "jobExamples")
        tmp = tempfile.mkdtemp('', prefix='swf_test_')
        self.output_dir = tmp
        if (not os.path.isdir(self.examples_dir) or
                not os.path.isdir(self.output_dir)):
            raise ConfigurationError("%s or %s does not exist." % (
                                     self.examples_dir,
                                     self.output_dir))

    @staticmethod
    def get_workflow_example_list():
        example_names = []
        for example_func in dir(WorkflowExamples):
            prefix = "example_"
            if len(example_func) < len(prefix):
                continue
            if example_func[0: len(prefix)] == prefix:
                example_names.append(example_func)
        return example_names

    def get_workflow_example(self, example_index):
        return self.get_workflows()[example_index]

    def get_workflows(self):
        workflows = []
        example_funcs = WorkflowExamples.get_workflow_example_list()
        for example_func in example_funcs:
            get_example_func = getattr(self, example_func)
            workflow = get_example_func()
            workflows.append(workflow)
        return workflows

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

    def example_special_transfer(self):
        # jobs
        test_dir_contents = self.job_test_dir_contents()
        test_multi_file_format = self.job_test_multi_file_format()
        # building the workflow
        jobs = [test_dir_contents, test_multi_file_format]
        dependencies = []
        function_name = inspect.stack()[0][3]
        workflow = Workflow(jobs, dependencies, name=function_name)
        return workflow

    def example_special_command(self):
        # jobs
        test_command_job = self.job_test_command_1()
        warnings.warn("This workflow prints a warning.")
        # building the workflow
        jobs = [test_command_job]

        dependencies = []
        function_name = inspect.stack()[0][3]
        workflow = Workflow(jobs, dependencies, name=function_name)
        return workflow

    def example_simple(self):
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

        function_name = inspect.stack()[0][3]
        workflow = Workflow(jobs,
                            dependencies,
                            root_group=[group_2, job4],
                            name=function_name)
        return workflow

    def example_wrong_native_spec_pbs(self):
       # jobs
        job1 = self.job1(option="-l walltime=5:00:00, pmem=16gb")
        job2 = self.job1(option="-l walltime=5:00:0")
        job3 = self.job1()
        # building the workflow
        jobs = [job1, job2, job3]

        workflow = Workflow(jobs, dependencies=[],
                            name="jobs with wrong native spec for pbs")
        return workflow

    def example_native_spec_pbs(self):
       # jobs
        job1 = self.job1(option="-l walltime=5:00:00,pmem=16gb")
        job2 = self.job1(option="-l walltime=5:00:0")
        job3 = self.job1()
        # building the workflow
        jobs = [job1, job2, job3]

        workflow = Workflow(jobs, dependencies=[],
                            name="jobs with native spec for pbs")
        return workflow

    def example_simple_exception1(self):
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

        function_name = inspect.stack()[0][3]
        workflow = Workflow(jobs,
                            dependencies,
                            root_group=[group_2, job4],
                            name=function_name)
        return workflow

    def example_simple_exception2(self):
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

        function_name = inspect.stack()[0][3]
        workflow = Workflow(jobs,
                            dependencies,
                            root_group=[group_2, job4],
                            name=function_name)
        return workflow

    def example_multiple(self):
        workflow1 = self.example_simple()
        workflow2 = self.example_simple_exception1()
        workflow3 = self.example_simple_exception2()

        jobs = workflow1.jobs
        jobs.extend(workflow2.jobs)
        jobs.extend(workflow3.jobs)

        dependencies = list(workflow1.dependencies)
        dependencies.extend(workflow2.dependencies)
        dependencies.extend(workflow3.dependencies)

        param_links = dict(workflow1.param_links)
        param_links.update(workflow2.param_links)
        param_links.update(workflow3.param_links)

        group1 = Group(name="simple example", elements=workflow1.root_group)
        group2 = Group(name="simple with exception in Job1",
                       elements=workflow2.root_group)
        group3 = Group(name="simple with exception in Job3",
                       elements=workflow3.root_group)

        function_name = inspect.stack()[0][3]
        workflow = Workflow(jobs,
                            dependencies,
                            root_group=[group1, group2, group3],
                            name=function_name)
        return workflow

    def example_n_jobs(self, nb=300, time=60):
        jobs = []
        for i in range(0, nb):
            job = self.job_sleep(time)
            jobs.append(job)

        dependencies = []
        function_name = inspect.stack()[0][3]
        workflow = Workflow(jobs, dependencies, name=function_name)
        return workflow

    def example_n_jobs_with_dependencies(self, nb=500, time=60):
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
        function_name = inspect.stack()[0][3]
        workflow = Workflow(jobs, dependencies, root_group, name=function_name)
        return workflow

    def example_serial_jobs(self, nb=5):
        jobs = []
        dependencies = []
        previous_job = self.job_sleep(2)
        jobs.append(previous_job)
        for i in range(0, nb):
            job = self.job_sleep(2)
            jobs.append(job)
            dependencies.append((previous_job, job))
            previous_job = job

        function_name = inspect.stack()[0][3]
        workflow = Workflow(jobs, dependencies, name=function_name)
        return workflow

    def example_fake_pipelineT1(self, n_iter=100):
        jobs = []
        dependencies = []
        root_group = []
        for i in range(0, n_iter):
            job1 = self.job_sleep(2)
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

            job2 = self.job_sleep(2)
            job2.name = "Gray/white segmentation"
            jobs.append(job2)
            job3 = self.job_sleep(2)
            job3.name = "Left hemisphere sulci recognition"
            jobs.append(job3)
            job4 = self.job_sleep(2)
            job4.name = "Right hemisphere sulci recognition"
            jobs.append(job4)

            # dependencies.append((job1, job2))
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
        function_name = inspect.stack()[0][3]
        workflow = Workflow(jobs, dependencies, root_group, name=function_name)
        return workflow

    def example_barrier(self):
        jobs = [self.job_sleep(2), self.job_sleep(2),
                self.job_sleep(2), self.job_sleep(2),
                self.job_sleep(2), self.job_sleep(2),
                self.job_sleep(2)]
        job_names = ['step1.1', 'step1.2',
                     'step2.1.1', 'step2.1.2',  'step2.2.1', 'step2.2.2',
                     'step3']
        barriers = [BarrierJob(name='barrier1'),
                    BarrierJob(name='barrier2.1'),
                    BarrierJob(name='barrier2.2'),
                    BarrierJob(name='barrier3')]
        for j, n in zip(jobs, job_names):
            j.name = n
        dependencies = [(jobs[0], barriers[0]), (jobs[1], barriers[0]),
                        (barriers[0], barriers[1]), (barriers[0], barriers[2]),
                        (barriers[1], jobs[2]), (barriers[1], jobs[3]),
                        (barriers[2], jobs[4]), (barriers[2], jobs[5]),
                        (jobs[2], barriers[3]), (jobs[3], barriers[3]),
                        (jobs[4], barriers[3]), (jobs[5], barriers[3]),
                        (barriers[3], jobs[6])]
        workflow = Workflow(jobs + barriers, dependencies)
        return workflow

    def example_dynamic_outputs(self):
        # jobs
        job1 = self.job1_with_outputs1()
        job2 = self.job2_with_outputs1()
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

        links = {
            job2: {'filePathIn1': [(job1, 'filePathOut1')]},
            job3: {'filePathIn': [(job1, 'filePathOut2')]},
            job4: {'file1': [(job2, 'filePathOut')]},
        }

        function_name = inspect.stack()[0][3]
        workflow = Workflow(jobs,
                            dependencies,
                            root_group=[group_2, job4],
                            name=function_name, param_links=links)
        return workflow

    def example_dynamic_outputs_with_mapreduce(self):
        # small map/reduce
        # jobs
        job1 = self.job_list_with_outputs()
        job2_0 = self.job8_with_output()
        job2_0.name = 'job2_0'
        job2_1 = self.job8_with_output()
        job2_1.name = 'job2_1'
        job3 = self.job_reduce_cat()
        # building the workflow
        jobs = [job1, job2_0, job2_1, job3]

        dependencies = []

        group_1 = Group(name='group_1', elements=[job2_0, job2_1])

        links = {
            job2_0: {'input': [(job1, 'outputs', ('list_to_sequence', 0))]},
            job2_1: {'input': [(job1, 'outputs', ('list_to_sequence', 1))]},
            job3: {'inputs': [(job2_0, 'output', ('sequence_to_list', 0)),
                              (job2_1, 'output', ('sequence_to_list', 1))]},
        }

        function_name = inspect.stack()[0][3]
        workflow = Workflow(jobs,
                            dependencies,
                            root_group=[job1, group_1, job3],
                            name=function_name, param_links=links)
        return workflow

    def example_dynamic_outputs_with_loo(self):
        # small leave-one-out
        # jobs
        job1 = self.job_list_with_outputs2()
        job2_train = self.job_reduce_cat(14)
        job2_train.name = 'train'
        job2_test = self.job8_with_output()
        job2_test.name = 'test'
        # building the workflow
        jobs = [job1, job2_train, job2_test]

        dependencies = []

        links = {
            job2_train: {'inputs': [(job1, 'outputs',
                                     ('list_all_but_one', 2))]},
            job2_test: {'input': [(job1, 'outputs', ('list_to_sequence', 2))]},
        }

        function_name = inspect.stack()[0][3]
        workflow = Workflow(jobs,
                            dependencies,
                            root_group=[job1, job2_train, job2_test],
                            name=function_name, param_links=links)
        return workflow

    def example_dynamic_outputs_with_cv(self):
        # small 4-fold cross-validation
        # jobs
        job1 = self.job_list_with_outputs2()
        job2_train = self.job_reduce_cat(16)
        job2_train.name = 'train'
        job2_test = self.job_reduce_cat(17)
        job2_test.name = 'test'
        # building the workflow
        jobs = [job1, job2_train, job2_test]

        dependencies = []

        links = {
            job2_train: {'inputs': [(job1, 'outputs',
                                     ('list_cv_train_fold', 1, 4))]},
            job2_test: {'inputs': [(job1, 'outputs',
                                    ('list_cv_test_fold', 1, 4))]},
        }

        function_name = inspect.stack()[0][3]
        workflow = Workflow(jobs,
                            dependencies,
                            root_group=[job1, job2_train, job2_test],
                            name=function_name, param_links=links)
        return workflow

    def example_dynamic_outputs_with_mapreduce_jobs(self):
        # small map/reduce using MapJob / ReduceJob
        # jobs
        job1 = self.job_list_with_outputs()
        job2_0 = self.job8_with_output()
        job2_0.name = 'job2_0'
        job2_1 = self.job8_with_output()
        job2_1.name = 'job2_1'
        job3 = self.job_reduce_cat()
        map_job = MapJob(referenced_input_files=job1.referenced_output_files,
                         name='map')
        reduce_job = ReduceJob()
        # building the workflow
        jobs = [job1, job2_0, job2_1, job3, map_job, reduce_job]

        dependencies = []

        group_1 = Group(name='group_1', elements=[job2_0, job2_1])

        links = {
            map_job: {'inputs': [(job1, 'outputs')]},
            job2_0: {'input': [(map_job, 'output_0')]},
            job2_1: {'input': [(map_job, 'output_1')]},
            reduce_job: {'input_0': [(job2_0, 'output')],
                         'input_1': [(job2_1, 'output')],
                         'lengths': [(map_job, 'lengths')]},
            job3: {'inputs': [(reduce_job, 'outputs')]},
        }

        function_name = inspect.stack()[0][3]
        workflow = Workflow(jobs,
                            dependencies,
                            root_group=[job1, map_job, group_1, reduce_job,
                                        job3],
                            name=function_name, param_links=links)
        return workflow

    def example_dynamic_outputs_with_loo_jobs(self):
        # small leave-one-out
        # jobs
        job1 = self.job_list_with_outputs2()
        loo_job = LeaveOneOutJob(
            referenced_input_files=job1.referenced_output_files,
            param_dict={'index': 2})
        job2_train = self.job_reduce_cat(14)
        job2_train.name = 'train'
        job2_test = self.job8_with_output()
        job2_test.name = 'test'
        # building the workflow
        jobs = [job1, loo_job, job2_train, job2_test]

        dependencies = []

        links = {
            loo_job: {'inputs': [(job1, 'outputs')]},
            job2_train: {'inputs': [(loo_job, 'train')]},
            job2_test: {'input': [(loo_job, 'test')]},
        }

        function_name = inspect.stack()[0][3]
        workflow = Workflow(jobs,
                            dependencies,
                            root_group=[job1, loo_job, job2_train, job2_test],
                            name=function_name, param_links=links)
        return workflow

    def example_dynamic_outputs_with_cv_jobs(self):
        # small 4-fold cross-validation
        # jobs
        job1 = self.job_list_with_outputs2()
        cv_job = CrossValidationFoldJob(
            referenced_input_files=job1.referenced_output_files,
            param_dict={'nfolds': 4, 'fold': 1})
        job2_train = self.job_reduce_cat(16)
        job2_train.name = 'train'
        job2_test = self.job_reduce_cat(17)
        job2_test.name = 'test'
        # building the workflow
        jobs = [job1, cv_job, job2_train, job2_test]

        dependencies = []

        links = {
            cv_job: {'inputs': [(job1, 'outputs')]},
            job2_train: {'inputs': [(cv_job, 'train')]},
            job2_test: {'inputs': [(cv_job, 'test')]},
        }

        function_name = inspect.stack()[0][3]
        workflow = Workflow(jobs,
                            dependencies,
                            root_group=[job1, cv_job, job2_train, job2_test],
                            name=function_name, param_links=links)
        return workflow

if __name__ == "__main__":
    print(WorkflowExamples.get_workflow_example_list())
