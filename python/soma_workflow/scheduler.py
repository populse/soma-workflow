# -*- coding: utf-8 -*-

from __future__ import absolute_import
from __future__ import print_function
import six
import os
import inspect
import importlib
import threading


class Scheduler(object):

    '''
    Allow to submit, kill and get the status of jobs.

    The Scheduler class is an abstract class which specifies the jobs
    management API. It has several implementations, located in
    ``soma_workflow.schedulers.*_scheduler``.

    A scheduler implementation class can be retrived using the global function
    :func:`get_scheduler_implementation`, or instantiated using
    :func:`build_scheduler`.

    New schedulers can be written to support computing resources types that are
    currently not supported (a cluster with a DRMS which has no DRMAA
    implementation typicalyly). The various methods of the Scheduler API have
    to be overloaded in this case.
    '''
    parallel_job_submission_info = None

    logger = None

    is_sleeping = None

    jobs_finished_event = None

    def __init__(self):
        self.parallel_job_submission_info = None
        self.is_sleeping = False
        self.jobs_finished_event = threading.Event()

    def sleep(self):
        self.is_sleeping = True

    def wake(self):
        self.is_sleeping = False

    def clean(self):
        pass

    def job_submission(self, jobs):
        '''
        Submit a Soma-Workflow job

        Parameters
        ----------
        jobs: EngineJob or list[EngineJob]
            Job to be submitted

        Returns
        -------
        job_id: list[str]
            Job id for the scheduling system (DRMAA for example, or native DRMS
            identifier).
            If some submissions failed, None is in the list instead of the job
            id.
        '''
        raise Exception("Scheduler is an abstract class!")

    def get_job_status(self, scheduler_job_id):
        '''
        Parameters
        ----------
        scheduler_job_id: string
            Job id for the scheduling system (DRMAA for example)

        Returns
        -------
        status: string
            Job status as defined in constants.JOB_STATUS
        '''
        raise Exception("Scheduler is an abstract class!")

    def get_job_exit_info(self, scheduler_job_id):
        '''
        The exit info consists of 4 values returned in a tuple:
        **exit_status**: string
            one of the constants.JOB_EXIT_STATUS values
        **exit_value**: int
            exit code of the command (normally 0 in case of success)
        **term_sig**: int
            termination signal, 0 IF ok
        **resource_usage**: bytes
            bytes string in the shape
            ``b'cpupercent=60 mem=13530kb cput=00:00:12'`` etc. Items may include:

            * cpupercent
            * cput
            * mem
            * vmem
            * ncpus
            * walltime

        Parameters
        ----------
        scheduler_job_id: string
            Job id for the scheduling system (DRMAA for example)

        Returns
        -------
        exit_info: tuple
            exit_status, exit_value, term_sig, resource_usage
        '''
        raise Exception("Scheduler is an abstract class!")

    def kill_job(self, scheduler_job_id):
        '''
        Parameters
        ----------
        scheduler_job_id: string
            Job id for the scheduling system (DRMAA for example)
        '''
        raise Exception("Scheduler is an abstract class!")

    @classmethod
    def build_scheduler(cls, config):
        ''' Create a scheduler of the expected type, using configuration to
        parameterize it.

        Parameters
        ----------
        config: Configuration
            configuration object instance
        '''
        raise Exception("Scheduler is an abstract class!")


def get_scheduler_implementation(scheduler_type):
    ''' Get the scheduler class implementation corresponding to the expected
        one.

        Parameters
        ----------
        scheduler_type: str
            scheduler type: 'drmaa', 'drmaa2', 'local_basic', 'mpi', or other
            custom scheduler

        Returns
        -------
        scheduler_class: Scheduler subclass
    '''
    from . import schedulers
    if scheduler_type == 'local_basic':
        scheduler_type = 'local'
    sched_dir = os.path.dirname(schedulers.__file__)
    if os.path.exists(os.path.join(sched_dir,
                                   '%s_scheduler.py' % scheduler_type)):
        sched_mod = '%s_scheduler' % scheduler_type
        # try:
        module = importlib.import_module('.%s' % sched_mod,
                                         'soma_workflow.schedulers')
        sched_list = []
        # if there is a __main_scheduler__, just use it
        scheduler = getattr(module, '__main_scheduler__', None)
        if scheduler is not None:
            return scheduler
        for element in six.itervalues(module.__dict__):
            if element in sched_list:
                continue  # avoid duplicates
            if inspect.isclass(element) and element is not Scheduler \
                    and issubclass(element, Scheduler):
                sched_list.append(element)
                if element.__name__.lower() == ('%sscheduler'
                                                % scheduler_type).lower():
                    # fully matching
                    return element
        if len(sched_list) == 1:
            # unambiguous
            return sched_list[0]
        if len(sched_list) == 0:
            print('Warning: module soma_workflow.schedulers.%s contains '
                  'no scheduler:' % sched_mod)
        else:
            print('Warning: module soma_workflow.schedulers.%s contains '
                  'several schedulers:' % sched_mod)
            print([s.__name__ for s in sched_list])
        # except ImportError:
    raise NameError('scheduler type %s is not found' % scheduler_type)


def build_scheduler(scheduler_type, config):
    ''' Create a scheduler of the expected type, using configuration to
    parameterize it.

    Parameters
    ----------
    scheduler_type: string
        type of scheduler to be built
    config: Configuration
        configuration object

    Returns
    -------
    scheduler: Scheduler
        Scheduler instance
    '''
    scheduler_class = get_scheduler_implementation(scheduler_type)
    scheduler = scheduler_class.build_scheduler(config)
    return scheduler


def get_schedulers_list():
    '''
    List all available installed schedulers

    Returns
    -------
    schedulers: list
        schedulers list. Each item is a tuple (name, enabled)
    '''
    from . import schedulers
    dirname = os.path.dirname(schedulers.__file__)
    sched_files = os.listdir(dirname)
    schedulers = []
    for sched_file in sched_files:
        if sched_file.endswith('_scheduler.py'):
            sched_mod = sched_file[:-3]
            enabled = True
            try:
                module = importlib.import_module('.%s' % sched_mod,
                                                 'soma_workflow.schedulers')
            except NotImplementedError:
                continue  # skip not implemented / unfinished ones
            except Exception:
                enabled = False
            if sched_mod == 'local_scheduler':
                sched_mod = 'local_basic_scheduler'
            sched = sched_mod[:-10]
            schedulers.append((sched, enabled))
    return schedulers
