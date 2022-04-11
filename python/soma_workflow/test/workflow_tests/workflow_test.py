# -*- coding: utf-8 -*-
from __future__ import with_statement, print_function
from __future__ import absolute_import
'''
@author: laure.hugo@cea.fr
@author: Soizic Laguitton
@organization: U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''

import unittest
import sys
import os
import shutil
import tempfile
import socket
import atexit

from soma_workflow.client import WorkflowController
from soma_workflow.configuration import Configuration, LIGHT_MODE, \
    change_soma_workflow_directory, restore_soma_workflow_directory
from soma_workflow.test.utils import get_user_id
from soma_workflow.test.utils import suppress_stdout

from soma_workflow.test.workflow_tests import WorkflowExamplesLocal
from soma_workflow.test.workflow_tests import WorkflowExamplesShared
from soma_workflow.test.workflow_tests import WorkflowExamplesSharedTransfer
from soma_workflow.test.workflow_tests import WorkflowExamplesTransfer

import six
from six.moves import map


def init_params():

    cls = WorkflowTest

    # check global commandline arguments
    if '--isolated' in sys.argv[1:]:
        tmpdir = tempfile.mkdtemp(prefix='swf_isol')
        print('isolated mode:', tmpdir, file=sys.stderr)
        cls.isolated_dir = tmpdir
        with open(os.path.join(tmpdir, '.soma-workflow.cfg'), 'w') as f:
            f.write('''[local-server]
cluster_address = localhost
scheduler_type = local_basic
server_name = local-server
soma_workflow_dir = %s
engine_log_level = DEBUG
server_log_level = DEBUG

[local-server-ssh]
cluster_address = localhost
submitting_machines = localhost
scheduler_type = local_basic
server_name = local-server-ssh
soma_workflow_dir = %s
engine_log_level = DEBUG
server_log_level = DEBUG
''' % (tmpdir, tmpdir))
        #change_soma_workflow_directory(tmpdir, 'swf-test')
        os.environ['SOMA_WORKFLOW_CONFIG'] = os.path.join(
            tmpdir, '.soma-workflow.cfg')
        atexit.register(exit_tests)

    if '--resources' in sys.argv[1:]:
        # non-interactive list of resources
        resources = sys.argv[sys.argv[1:].index('--resources') + 2]
        cls.enabled_resources = [x.strip() for x in resources.split(',')]
        if 'localhost' in cls.enabled_resources:
            local_resource = socket.gethostname()
            cls.enabled_resources[cls.enabled_resources.index('localhost')] \
                = local_resource


def exit_tests():

    cls = WorkflowTest

    if hasattr(cls, 'isolated_dir'):
        # kill database server in local server mode
        if hasattr(cls, 'enabled_resources'):
            from soma_workflow.connection import RemoteConnection
            for resource_id in cls.enabled_resources:
                if resource_id in ('local-server', 'local-server-ssh'):
                    RemoteConnection.kill_remote_servers(resource_id)
        if '--keep-temporary' in sys.argv[1:]:
            print('leaving [non-temporary] directory %s' % cls.isolated_dir,
                  file=sys.stderr)
        else:
            shutil.rmtree(cls.isolated_dir)



class WorkflowTest(unittest.TestCase):

    LOCAL_PATH = "local path"
    FILE_TRANSFER = "file transfer"
    SHARED_RESOURCE_PATH = "shared resource path"
    SHARED_TRANSFER = "file transfer and shared resource path"

    wf_ctrl = None
    path_management = None
    wf_examples = None
    wf_id = None

    @classmethod
    def setup_wf_controller(cls, workflow_controller):
        cls.wf_ctrl = workflow_controller

    @classmethod
    def setup_path_management(cls, path_management):
        '''
        * path_management: LOCAL_PATH, FILE_TRANSFER or SHARED_RESOURCE_PATH
        '''
        cls.path_management = path_management

    @classmethod
    def setUpClass(cls):
        #import pdb
        import signal
        #import code

        def debug_stack(sig, frame):
            """Interrupt running process, and provide a python prompt for
            interactive debugging."""
            import traceback
            traceback.print_stack()

            #d={'_frame':frame}         # Allow access to frame object.
            #d.update(frame.f_globals)  # Unless shadowed by global
            #d.update(frame.f_locals)

            #i = code.InteractiveConsole(d)
            #message  = "Signal received : entering python shell.\nTraceback:\n"
            #message += ''.join(traceback.format_stack(frame))
            #i.interact(message)

            #pdb.set_trace()

        if not sys.platform.startswith('win'):
            signal.signal(signal.SIGUSR1, debug_stack)


    def setUp(self):
        if self.path_management == self.LOCAL_PATH:
            workflow_examples = WorkflowExamplesLocal()
        elif self.path_management == self.FILE_TRANSFER:
            workflow_examples = WorkflowExamplesTransfer()
        elif self.path_management == self.SHARED_RESOURCE_PATH:
            workflow_examples = WorkflowExamplesShared()
        elif self.path_management == self.SHARED_TRANSFER:
            workflow_examples = WorkflowExamplesSharedTransfer()
        self.wf_examples = workflow_examples

        # use a custom temporary soma-workflow dir to avoid concurrent
        # access problems
        tmpdb = self.wf_examples.output_dir
        if '--keep-temporary' in sys.argv[1:]:
            print('making non-temporary directory: %s' % tmpdb,
                  file=sys.stderr)
        self.soma_workflow_temp_dir = tmpdb
        change_soma_workflow_directory(tmpdb, socket.gethostname())

        self.temporaries = [self.wf_examples.output_dir]

    def tearDown(self):
        swc = self.__class__.wf_ctrl
        rmfiles = not '--keep-temporary' in sys.argv[1:]
        if swc is not None:
            if self.wf_id:
                if rmfiles:
                    try:
                        swc.delete_workflow(self.wf_id)
                    except Exception as e:
                        print('** error in delete_workflow: **',
                              file=sys.stderr)
                        import traceback
                        traceback.print_exc()
                        # debug
                        if '--debug' in sys.argv[1:]:
                            print('\n=======  client log  ========',
                                  file=sys.stderr)
                            with open('/tmp/swf_test_log') as f:
                                print(f.read(), file=sys.stderr)
                            config = swc.config
                            if config.get_mode() != 'light':
                                resource_id = config._resource_id
                                eng_log_info = config.get_engine_log_info()
                                login = config.get_login()
                                if login is None:
                                    import getpass
                                    login = getpass.getuser()
                                engine_name = "workflow_engine_" + login
                                log_file = os.path.join(
                                    eng_log_info[0],
                                    "log_" + engine_name)
                                if os.path.exists(log_file):
                                    print('\n=======  server log  =======',
                                          file=sys.stderr)
                                    with open(log_file) as f:
                                        lines = f.readlines()
                                        if len(lines) > 2000:
                                            print(
                                                'tail 2000 log lines out of %d'
                                                % len(lines), file=sys.stderr)
                                        print('\n'.join(lines[-2000:]),
                                              file=sys.stderr)

                else:
                    print('workflow %d has been kept in database in %s'
                          % (self.wf_id, self.soma_workflow_temp_dir),
                          file=sys.stderr)
            # stop workflow controler and wait for thread termination
            # swc.stop_engine()
        for t in self.temporaries:
            if os.path.isdir(t):
                if rmfiles:
                    try:
                        shutil.rmtree(t)
                    except OSError:
                        pass
                else:
                    print('leaving directory: %s' % t, file=sys.stderr)
            elif os.path.exists(t):
                if rmfiles:
                    try:
                        os.unlink(t)
                    except OSError:
                        pass
                else:
                    print('leaving file: %s' % t, file=sys.stderr)
        restore_soma_workflow_directory()

    def print_jobs(self, jobs, title='Jobs', file=sys.stderr):
        print('\n%s:' % title, self.wf_ctrl.jobs(jobs), file=file)
        print(file=file)
        for job_id in jobs:
            print('job', job_id, self.wf_ctrl.jobs([job_id])[job_id],
                  file=file)
            job_stdout_file = tempfile.NamedTemporaryFile(
                prefix="job_soma_out_log_",
                suffix=repr(job_id),
                delete=False)
            job_stdout_file = job_stdout_file.name
            job_stderr_file = tempfile.NamedTemporaryFile(
                prefix="job_soma_outerr_log_",
                suffix=repr(job_id),
                delete=False)
            job_stderr_file = job_stderr_file.name

            try:
                self.wf_ctrl.retrieve_job_stdouterr(job_id,
                                                    job_stdout_file,
                                                    job_stderr_file)
                with open(job_stdout_file) as f:
                    print('stdout:\n', f.read(), file=file)
                with open(job_stderr_file) as f:
                    print('stderr:\n', f.read(), file=file)
            finally:
                os.unlink(job_stdout_file)
                os.unlink(job_stderr_file)

    @classmethod
    def run_test(cls, debug=False, interactive=False, **kwargs):
        sys.stdout.write(
            "********* soma-workflow tests: %s *********\n" % cls.__name__)

        config_file_path = Configuration.search_config_path()
        resource_ids = Configuration.get_configured_resources(config_file_path)

        enabled_resources = getattr(WorkflowTest, 'enabled_resources', None)
        enable_resources = []
        if not hasattr(WorkflowTest, 'resource_pass'):
            WorkflowTest.resource_pass = {}

        for resource_id in resource_ids:
            sys.stdout.write("============ Resource : " + resource_id +
                             " =================== \n")
            config = Configuration.load_from_file(resource_id,
                                                  config_file_path)

            if not interactive \
                    and ((enabled_resources is None
                          and config.get_mode() != LIGHT_MODE)
                         or (enabled_resources is not None
                             and resource_id not in enabled_resources)):
                sys.stdout.write('Resource %s is not tested in '
                                 'non-interactive mode\n' % resource_id)
                continue  # skip login/password ask
            if interactive:
                if enabled_resources is None:
                    sys.stdout.write("Do you want to test the resource "
                                    "%s (Y/n) ? " % resource_id)
                    sys.stdout.flush()
                    test_resource = sys.stdin.readline()
                    if test_resource.strip() in ['no', 'n', 'N', 'No', 'NO']:
                        # Skip the resource
                        sys.stdout.write('Resource %s is not tested \n'
                                        % resource_id)
                        sys.stdout.flush()
                        continue
                    enable_resources.append(resource_id)
                    (login, password) = get_user_id(resource_id, config)
                    WorkflowTest.resource_pass[resource_id] = (login, password)
                else:
                    if resource_id not in enabled_resources:
                        continue
                    (login, password) = WorkflowTest.resource_pass[resource_id]
            else:
                (login, password) = get_user_id(resource_id, config,
                                                interactive=interactive)

            if config.get_mode() == LIGHT_MODE:
                # use a temporary sqlite database in soma-workflow to avoid
                # concurrent access problems
                tmpdb = tempfile.mkstemp('.db', prefix='swf_')
                os.close(tmpdb[0])
                os.unlink(tmpdb[1])
                # and so on for transfers / stdio files directory
                tmptrans = tempfile.mkdtemp(prefix='swf_')
                config._database_file = tmpdb[1]
                config._transfered_file_dir = tmptrans

            wf_controller = None
            try:

                with suppress_stdout(debug or kwargs.get('verbosity', 0) >= 1):
                    wf_controller = WorkflowController(resource_id,
                                                       login,
                                                       password,
                                                       config=config)
                    cls.setup_wf_controller(wf_controller)

                allowed_config = cls.allowed_config[:]
                for configuration in cls.allowed_config:
                    if config.get_mode() != configuration[0]:
                        allowed_config.remove(configuration)
                if len(allowed_config) == 0:
                    sys.stdout.write(
                        "No tests available for the resource %s \n"
                        % resource_id)

                for configuration in allowed_config:
                    (mode, file_system) = configuration
                    sys.stdout.write(
                        "\n---------------------------------------\n")
                    sys.stdout.write("Mode : " + mode + '\n')
                    sys.stdout.write("File system : " + file_system + '\n')
                    cls.setup_path_management(file_system)

                    if file_system in (cls.SHARED_RESOURCE_PATH,
                                       cls.SHARED_TRANSFER) \
                            and not config.get_path_translation():
                        sys.stdout.write(
                            "Paths translation unavailable - not testing "
                            "this case\n")
                        sys.stdout.flush()
                        continue

                    suite_list = []
                    list_tests = []
                    for test in dir(cls):
                        prefix = "test_"
                        if len(test) < len(prefix):
                            continue
                        if test[0: len(prefix)] == prefix:
                            list_tests.append(test)

                    suite_list.append(
                        unittest.TestSuite(list(map(cls, list_tests))))
                    alltests = unittest.TestSuite(suite_list)
                    with suppress_stdout(debug
                                         or kwargs.get('verbosity', 0) >= 1):
                        res = unittest.TextTestRunner(verbosity=2).run(
                            alltests)
                    sys.stdout.flush()
                    sys.stdout.write("after test\n")

                    if len(res.errors) != 0 or len(res.failures) != 0:
                        raise RuntimeError("tests failed.")

            finally:
                sys.stdout.write("del wf_controller")
                if wf_controller:
                    wf_controller.stop_engine()
                del wf_controller
                cls.setup_wf_controller(None)  # del WorkflowController
                sys.stdout.write("deleted.")
                if config.get_mode() == LIGHT_MODE:
                    if not kwargs.get('keep_temporary', False):
                        if os.path.exists(config._database_file):
                            os.unlink(config._database_file)
                        if os.path.exists(config._database_file + '-journal'):
                            os.unlink(config._database_file + '-journal')
                        shutil.rmtree(config._transfered_file_dir)
                    else:
                        print('temporary files kept:')
                        print('databse file:', config._database_file)
                        print('transfers:', config._transfered_file_dir)

        if interactive and enabled_resources is None:
            print('set enabled_resources')
            WorkflowTest.enabled_resources = enable_resources

    @classmethod
    def run_test_function(cls, debug=False, interactive=False, **kwargs):
        ''' Same as run_test() but returns True in case of success and False
        in case of failure instead of raising an exception
        '''
        try:
            cls.run_test(debug=debug, interactive=interactive, **kwargs)
            return True
        except Exception:
            import traceback
            traceback.print_exc()
            return False

    @staticmethod
    def print_help(argv):
        print(argv[0],
              '[-h|--help] [--interactive] [--keep-temporary] [--debug] '
              '[--resources <resource1,resource2,...>] [--isolated]')
        print('--interactive: ask computing resources to be tested from the '
              'current user config')
        print('--resources: provide a non-interactive list of computing '
              'resources to be tested')
        print('--isolated: isolate config from the actual user config: use a '
              'temporary one containing 3 configs: the local resource, '
              '"local-server": a local server config (client-server mode on '
              'the local machine), and "local-server-ssh": a local server in '
              '"remote mode" using ssh - "ssh localhost" needs to work '
              'without a password)')
        print('-v|--verbose')

    @staticmethod
    def parse_args(argv):
        kwargs = {}
        if len(argv) > 1:
            if '-h' in argv[1:] or '--help' in argv[1:]:
                WorkflowTest.print_help(argv)
                sys.exit(0)
            if '--interactive' in argv[1:]:
                kwargs['interactive'] = True
            if '--keep-temporary' in argv[1:]:
                kwargs['keep_temporary'] = True
            if '--debug' in argv[1:]:
                kwargs['debug'] = True
            else:
                kwargs['debug'] = False
            if '-v' in argv[1:] or '--verbose' in argv[1:]:
                kwargs['verbosity'] = 2
        return kwargs

    def print_job_io_info(self, job_id, msg=None, file=sys.stderr):
        # this debug info should be removed when we find
        # out why tests randomly fail from time to time.
        (jobs_info, transfers_info, workflow_status, workflow_queue,
            tmp_files) = self.wf_ctrl.workflow_elements_status(self.wf_id)
        job_list = self.wf_ctrl.jobs([job_id])
        job_name, job_command, job_submission_date = job_list[job_id]

        print('\n** failure in %s job stdout/stderr **'
              % self.__class__.__name__, file=file)
        print('job id:', job_id, ', job name:', job_name, file=file)
        if msg:
            print('++ error message: ++', file=file)
            print(msg, file=file)
            print('++ (message end) ++', file=file)
        eng_stdout, eng_stderr = \
            self.wf_ctrl._engine_proxy.stdouterr_file_path(job_id)
        print('job engine stdout:', eng_stdout, ', stderr:', eng_stderr,
              file=file)
        print('++ stdout: ++', file=file)
        print(open(eng_stdout).read(), file=file)
        print('++ (stdout end) ++', file=file)
        print('++ stderr: ++', file=file)
        print(open(eng_stderr).read(), file=file)
        print('++ (stderr end) ++', file=file)
        jobs_files = [
            (ji[0],
             self.wf_ctrl._engine_proxy.stdouterr_file_path(ji[0]))
            for ji in jobs_info]
        print('engine jobs files:', jobs_files, file=file)
        print('jobs list:', job_list, file=file)
        print('tmp_files:', tmp_files, file=file)
        print('** (job failure end) **', file=file)

    def assertTrue(self, condition, msg=None):
        if not bool(condition) and hasattr(self, 'tested_job'):
            self.print_job_io_info(self.tested_job, msg)
        return super(WorkflowTest, self).assertTrue(condition, msg)

    def assertFalse(self, condition, msg=None):
        if bool(condition) and hasattr(self, 'tested_job'):
            self.print_job_io_info(self.tested_job, msg)
        return super(WorkflowTest, self).assertFalse(condition, msg)

    def assertEqual(self, first, second, msg=None):
        if first != second and hasattr(self, 'tested_job'):
            self.print_job_io_info(self.tested_job, msg)
        return super(WorkflowTest, self).assertEqual(first, second, msg)

    def assertNonEqual(self, first, second, msg=None):
        if first == second and hasattr(self, 'tested_job'):
            self.print_job_io_info(self.tested_job, msg)
        return super(WorkflowTest, self).assertNonEqual(first, second, msg)

init_params()
