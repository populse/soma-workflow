from __future__ import with_statement, print_function

'''
author: Soizic Laguitton

organization: I2BM, Neurospin, Gif-sur-Yvette, France
organization: CATI, France

license: `CeCILL-B <http://www.cecill.info/licences/Licence_CeCILL_B-en.html>`_
'''

# TODO:
# clean() is called way too often (for each workflow / job / file to be
# removed), and too much power is probably taken in scanning obsolete items and
# files. It should be actually done after a short time, combining several calls
# to clean()


#-------------------------------------------------------------------------------
# Imports
#-------------------------------------------------------------------------
import sqlite3
import threading
import os
import shutil
import logging
import pickle
from datetime import date
from datetime import timedelta
from datetime import datetime
import socket
import itertools
import io
import traceback
import math
import glob
import ctypes
import ctypes.util
import tempfile
import json

import soma_workflow.constants as constants
from soma_workflow.client import FileTransfer, TemporaryPath
from soma_workflow.errors import UnknownObjectError, DatabaseError
from soma_workflow.info import DB_VERSION, DB_PICKLE_PROTOCOL
from soma_workflow import utils

# python 2/3 compatibility
import sys
import six

if sys.version_info[0] >= 3:
    StringIO = io.StringIO

    def unicode(string):
        if isinstance(string, bytes):
            return string.decode('utf-8')
        return str(string)

    def keys(thing):
        return list(thing.keys())

else:
    import StringIO
    StringIO = StringIO.StringIO

    def keys(thing):
        return thing.keys()

if not hasattr(six, 'next'):
    # ubuntu 12.04 does not have next() in its six module
    def six_next(obj):
        return obj.next()
    six.next = six_next
    del six_next


#-----------------------------------------------------------------------------
# Globals and constants
#-----------------------------------------------------------------------------

strtime_format = '%Y-%m-%d %H:%M:%S'
file_separator = ', '
update_interval = timedelta(0, 30, 0)

#-----------------------------------------------------------------------------
# Local utilities
#-----------------------------------------------------------------------------


def adapt_datetime(ts):
    return ts.strftime(strtime_format)

sqlite3.register_adapter(datetime, adapt_datetime)


#-----------------------------------------------------------------------------
# Classes and functions
#-----------------------------------------------------------------------------


'''
Job server database tables:
  Users
    id
    login or other userId

  Jobs
    => identification:
      id      : int
      user_id : int

    => used by the job system (DrmaaWorkflowEngine, WorkflowDatabaseServer)
      drmaa_id           : string, None if not submitted
                           submitted job DRMAA identifier
      expiration_date    : date
      status             : string
                           job status as defined in constants.JOB_STATUS
      last_status_update : date
      workflow_id        : int, optional
                           id of the workflow the job belongs to.
                           None if it doesn't belong to any.
      stdout_file        : file path
      stderr_file        : file path, optional
      input_params_file  : file path, optional
      output_params_file : file path, optional
      pickled_engine_job

    => used to submit the job
      command             : string
                            job command
      stdin_file          : file path, optional
                            job's standard input as a path to a file.
                            C{None} if the job doesn't require an input stream.
      join_errout         : boolean
                            C{True} if the standard error should be
                            redirect in the same file as the standard output
      (stdout_file        : file path)
                             job's standard output as a path to a file
      (stderr_file        : file path, optional)
                            job's standard output as a path to a file
      working_directory   : dir path, optional
                            path of the job working directory.
      custom_submission   : boolean
                            C{True} if it was a custom submission.
                            If C{True} the standard output files won't
                            be deleted with the job.
      parallel_config_name : string, optional
                             if the job is made to run on several nodes:
                             name of the parallel configuration as defined
                             in configuration.PARALLEL_CONFIGURATIONS.
      nodes_number         : int, optional
                             number of nodes requested by the job to run
      cpu_per_node         : int, optional
                             number of CPU/cores needed for each node
      queue                : string, optional
                             name of the queue used to submit the job.

    => for user and administrator usage
      name               : string, optional
                           optional name of the job.
      submission_date    : date
      execution_date     : date
      ending_date        : date
      exit_status        : string, optional
                           exit status string as defined in constants.JOB_EXIT_STATUS
      exit_value         : int, optional
                           if the status is FINISHED_REGULARLY, it contains the operating
      terminating_signal : string, optional
                           if the status is FINISHED_TERM_SIG, it contain a
                           representation  of the signal that caused the termination of the job.
    system exit code of the job.
      resource_usage_file  : string, optional
                             contain the resource usage information of the job.



  Transfer
    id
    engine file path
    client file path (optional)
    transfer date
    expiration date
    user_id
    workflow_id (optional)
    status
    client_paths
    transfer_type

  Input/Ouput junction table
    job_id
    engine file path (transferid)
    input or output

  Workflows
    id,
    user_id,
    pickled_engine_workflow,
    expiration_date,
    name,
    ended_transfered,
    status
'''


def create_database(database_file):
    connection = sqlite3.connect(
        database_file, timeout=5, isolation_level="EXCLUSIVE",
        check_same_thread=False)
    cursor = connection.cursor()
    cursor.execute(
        '''CREATE TABLE users (
            id    INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
            login VARCHAR(255) NOT NULL UNIQUE)''')
    cursor.execute(
        '''CREATE TABLE jobs (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
            user_id              INTEGER NOT NULL CONSTRAINT known_user REFERENCES users (id),

            drmaa_id             VARCHAR(255),
            expiration_date      DATE NOT NULL,
            status               VARCHAR(255) NOT NULL,
            last_status_update   DATE NOT NULL,
            workflow_id          INTEGER CONSTRAINT known_workflow REFERENCES workflows (id),

            command              TEXT,
            stdin_file           TEXT,
            join_errout          BOOLEAN NOT NULL,
            stdout_file          TEXT NOT NULL,
            stderr_file          TEXT,
            working_directory    TEXT,
            custom_submission    BOOLEAN NOT NULL,
            parallel_config_name TEXT,
            nodes_number         INTEGER,
            cpu_per_node         INTEGER,
            queue                TEXT,
            input_params_file    TEXT,
            output_params_file   TEXT,

            name                 TEXT,
            submission_date      DATE,
            execution_date       DATE,
            ending_date          DATE,
            exit_status          VARCHAR(255),
            exit_value           INTEGER,
            terminating_signal   VARCHAR(255),
            resource_usage       TEXT,
            output_params        TEXT,

            pickled_engine_job   TEXT
            )''')

    cursor.execute(
        '''CREATE TABLE transfers (
            id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
            engine_file_path  TEXT,
            client_file_path TEXT,
            transfer_date    DATE,
            expiration_date  DATE NOT NULL,
            user_id          INTEGER NOT NULL CONSTRAINT known_user REFERENCES users (id),
            workflow_id      INTEGER CONSTRAINT known_workflow REFERENCES workflows (id),
            status           VARCHAR(255) NOT NULL,
            client_paths     TEXT,
            transfer_type TEXT)''')

    cursor.execute(
        '''CREATE TABLE temporary_paths (
            temp_path_id     INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
            engine_file_path TEXT,
            expiration_date  DATE NOT NULL,
            user_id          INTEGER NOT NULL CONSTRAINT known_user REFERENCES users (id),
            workflow_id      INTEGER CONSTRAINT known_workflow REFERENCES workflows (id),
            status           VARCHAR(255) NOT NULL)''')

    cursor.execute(
        '''CREATE TABLE ios (
            job_id           INTEGER NOT NULL CONSTRAINT known_job REFERENCES jobs(id),
            engine_file_id  INTEGER NOT NULL CONSTRAINT known_engine_file REFERENCES transfers (id),
            is_input         BOOLEAN NOT NULL,
            PRIMARY KEY (job_id, engine_file_id, is_input))''')

    cursor.execute(
        '''CREATE TABLE ios_tmp (
            job_id        INTEGER NOT NULL CONSTRAINT known_job REFERENCES jobs(id),
            temp_path_id  INTEGER NOT NULL CONSTRAINT known_tmp_path REFERENCES temporary_paths (temp_path_id),
            is_input      BOOLEAN NOT NULL,
            PRIMARY KEY   (job_id, temp_path_id, is_input))''')

    cursor.execute('''CREATE TABLE fileCounter (count INTEGER)''')
    cursor.execute('INSERT INTO fileCounter (count) VALUES (?)', [0])

    cursor.execute(
        '''CREATE TABLE workflows (
            id               INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
            user_id           INTEGER NOT NULL CONSTRAINT known_user REFERENCES users (id),
            pickled_engine_workflow   TEXT,
            expiration_date    DATE NOT NULL,
            name               TEXT,
            ended_transfers    TEXT,
            status             TEXT,
            last_status_update DATE NOT NULL,
            queue              TEXT) ''')

    cursor.execute('''CREATE TABLE db_version (
        version TEXT NOT NULL,
        python_version TEXT NOT NULL)''')
    cursor.execute('''INSERT INTO db_version (version, python_version)
      VALUES (?, ?)''', [DB_VERSION, '%d.%d.%d' % sys.version_info[:3]])

    # parameters dependencies
    cursor.execute(
        '''CREATE TABLE param_links (
            workflow_id       INTEGER NOT NULL CONSTRAINT known_worflow REFERENCES workflows (id),
            dest_job_id       INTEGER NOT NULL CONSTRAINT known_job REFERENCES jobs (id),
            dest_param        TEXT,
            src_job_id        INTEGER NOT NULL CONSTRAINT known_job2 REFERENCES jobs (id),
            src_param         TEXT,
            pickled_function  TEXT)''')

    cursor.close()
    connection.commit()
    connection.close()


# -- this is a copy of the find_library in soma-base soma.utils.find_library
ctypes_find_library = ctypes.util.find_library

def find_library(name):
    ''' :func:`ctypes.util.find_library` is broken on linux at least: it relies
    on ``ldconfig``, which only searches system paths, not user paths nor
    ``LD_LIBRARY_PATH``, or alternatively uses ``gcc``, which is not always
    installed nor configured.

    Here we are looking in ``[[DY]LD_LIBRARY_]PATH`` (depending on the system)
    '''
    def sorted_match(filenames):
        return sorted(filenames)[-1] # probably not the best

    exts = ['.so']
    patterns = [ext + '.*' for ext in exts]
    fname = 'lib' + name
    if sys.platform.startswith('linux'):
        envar = 'LD_LIBRARY_PATH'
    elif sys.platform == 'darwin':
        envar = 'DYLD_LIBRARY_PATH'
        exts = ['.dylib']
        patterns = ['.*' + ext for ext in exts]
    elif sys.platform.startswith('win'):
        envar = 'PATH'
        exts = ['.dll', '.DLL']
        patterns = ['.*' + ext for ext in exts]
    else:
        # other undetermined system (bsd, othe unix...?), assume ELF
        envar = 'LD_LIBRARY_PATH'
    paths = os.environ.get(envar)
    if paths is None:
        # no path: fallback to ctypes
        return ctypes_find_library(name)

    paths = paths.split(os.pathsep)
    names = [fname + ext for ext in exts] + [name + ext for ext in exts]
    patterns = [fname + pattern for pattern in patterns] \
        + [name + pattern for pattern in patterns]
    found = None
    for path in paths:
        for tname in names:
            filename = os.path.join(path, tname)
            if os.path.exists(filename):
                found = filename
                break
        for tname in patterns:
            filenames = glob.glob(os.path.join(path, tname))
            if len(filenames) != 0:
                found = sorted_match(filenames)
                break

    if found is not None:
        return os.path.basename(os.path.realpath(found))

    # not found: fallback to ctypes
    return ctypes_find_library(name)
#--


_sqlite3_max_variable_number = -1

def sqlite3_max_variable_number():
    ''' Get the max number of variables sqlite3 can accept in a query/insert
    operation. This calls the C API using ctypes, and a temporary database,
    since python sqlite3 module does not expose the sqlite3_limit() function.

    Returns
    -------
    max_var: int
        max variable number, or 0 if an error occurred.
    '''
    global _sqlite3_max_variable_number
    if _sqlite3_max_variable_number != -1:
        return _sqlite3_max_variable_number

    try:

        lib = find_library('sqlite3')
        if lib is None and sys.platform.startswith('win'):
            lib = find_library('sqlite3-0')
        dll = ctypes.CDLL(lib)

        if dll is not None:
            t = tempfile.mkstemp(suffix='.sqlite')
            os.close(t[0])
            try:
                db = ctypes.c_void_p(None)
                dll.sqlite3_open_v2(t[1], ctypes.byref(db), 2,
                                    ctypes.c_void_p(None))
                _sqlite3_max_variable_number = dll.sqlite3_limit(db, 9, -1)
            finally:
                dll.sqlite3_close(db)
                os.unlink(t[1])
    except:
        pass
    if _sqlite3_max_variable_number == -1:
        _sqlite3_max_variable_number = 0
    return _sqlite3_max_variable_number


def print_job_status(database_file):
    connection = sqlite3.connect(
        database_file, timeout=5, isolation_level="EXCLUSIVE",
        check_same_thread=False)
    cursor = connection.cursor()

    for row in cursor.execute('SELECT id, status, queue FROM jobs'):
        job_id, status, queue = row
        print("job_id: " + repr(job_id) + " status: " + repr(status)
              + " queue " + repr(queue))

    cursor.close()
    connection.close()


def print_tables(database_file):

    connection = sqlite3.connect(
        database_file, timeout=5, isolation_level="EXCLUSIVE",
        check_same_thread=False)
    cursor = connection.cursor()

    print("==== users table: ========")
    for row in cursor.execute('SELECT * FROM users'):
        id, login = row
        print('id=', repr(id).rjust(2), 'login=', repr(login).rjust(7))

    print("==== transfers table: ====")
    for row in cursor.execute('SELECT * FROM transfers'):
        print(row)
        # engine_file_path, client_file_path, transfer_date, expiration_date,
        #     user_id = row
        # print('| engine_file_path', repr(engine_file_path).ljust(25), '|
        # client_file_path=', repr(client_file_path).ljust(25) , '|
        # transfer_date=', repr(transfer_date).ljust(7), '| expiration_date=',
        # repr(expiration_date).ljust(7), '| user_id=', repr(user_id).rjust(2),
        # ' |')

    print("==== temporary_paths table: ====")
    for row in cursor.execute('SELECT * FROM temporary_paths'):
        print(row)

    print("==== workflows table: ========")
    for row in cursor.execute('SELECT * FROM workflows'):
        print(row)
        # id, submission_date, user_id, expiration_date, stdout_file, stderr_file, join_errout, stdin_file, name, drmaa_id,     working_directory = row
        # print('id=', repr(id).rjust(3), 'submission_date=',
        # repr(submission_date).rjust(7), 'user_id=', repr(user_id).rjust(3),
        # 'expiration_date' , repr(expiration_date).rjust(7), 'stdout_file',
        # repr(stdout_file).rjust(10), 'stderr_file',
        # repr(stderr_file).rjust(10), 'join_errout',
        # repr(join_errout).rjust(5), 'stdin_file', repr(stdin_file).rjust(10),
        # 'name', repr(name).rjust(10), 'drmaa_id', repr(drmaa_id).rjust(10),
        # 'working_directory', repr(working_directory).rjust(10))

    print("==== jobs table: ========")
    for row in cursor.execute('SELECT * FROM jobs'):
        print(row)

    print("==== ios table: =========")
    for row in cursor.execute('SELECT * FROM ios'):
        job_id, engine_file_path, is_input = row
        print('| job_id=', repr(job_id).rjust(2), '| engine_file_path=',
              repr(engine_file_path).ljust(25), '| is_input=',
              repr(is_input).rjust(2), ' |')

    print("==== param_links table: =========")
    for row in cursor.execute('SELECT * FROM param_links'):
        workflow_id, dest_job_id, dest_param, src_job_id, src_param, func \
            = row
        if func is not None:
            if sys.version_info[0] >= 3:
                func = pickle.loads(func, encoding='utf-8')
            else:
                func = pickle.loads(func)
        print('| workflow_id=', repr(workflow_id).rjust(2), '| dest_job_id=',
              repr(dest_job_id).ljust(2), '| dest_param=',
              repr(dest_param).rjust(25), '| src_job_id=', repr(src_job_id).rjust(2), '| src_param=',
              repr(src_param).rjust(25), '| function=',
              repr(func).ljust(25), '|')

    # print("==== file counter table: =========")
    # for row in cursor.execute('SELECT * FROM fileCounter'):
        # count, foo = row
        # print('| count=', repr(count).rjust(2), '| foo=', repr(foo).ljust(2),
        # ' |')
    cursor.close()
    connection.close()

class WorkflowDatabaseServer(object):

    def __init__(self,
                 database_file,
                 tmp_file_dir_path,
                 shared_tmp_dir=None,
                 logging_configuration=None,
                 remove_orphan_files=True):
        '''
        The constructor gets as parameter the database information.

        @type  database_file: string
        @param database_file: the SQLite database file
        @type  tmp_file_dir_path: string
        @param tmp_file_dir_path: place on the resource file system where
        the files will be transfered
        '''

        #print('WorkflowDatabaseServer::__init__, remove orphan files:', remove_orphan_files)
        self._remove_orphan_files = remove_orphan_files
        self._tmp_file_dir_path = tmp_file_dir_path
        self._database_file = database_file
        if shared_tmp_dir:
            self._shared_temp_dir = shared_tmp_dir
        else:
            self._shared_temp_dir = self._tmp_file_dir_path
        # patch EngineTemporaryPath
        from soma_workflow.engine import EngineTemporaryPath
        EngineTemporaryPath.temporary_directory = self._shared_temp_dir

        self._lock = threading.RLock()

        self.logger = logging.getLogger('jobServer')
        self.logger.debug("=> starting database server, within the constructor")
        self._free_file_counters = []

        # For some reason logger does not work so we log using logging
        if logging_configuration:
            (server_log_file,
             server_log_format,
             server_log_level) = logging_configuration

            logging.basicConfig(filename=server_log_file,
                                format=server_log_format,
                                level=eval("logging." + server_log_level))

        with self._lock:
            if not os.path.isfile(database_file):
                self.logger.info("Database creation " + database_file)
                create_database(database_file)
            else:
                connection = self._connect()
                cursor = connection.cursor()
                version = None
                for row in cursor.execute("SELECT * FROM db_version"):
                    try:
                        version, py_ver = row
                    except ValueError:
                        # row has not 2 values, the database is older than 2.0
                        # (and is incompatible)
                        raise ValueError(
                            "The database table db_version does not have the "
                            "expected 2 columns, meaning that the database is "
                            "incompatible. Please erase the file %s and run "
                            "again" % database_file)
                    break

                try:
                    if version == None:
                        count = six.next(cursor.execute(
                            "SELECT count(*) FROM workflows WHERE "
                            "queue=?", ["default queue"]))[0]
                    elif unicode(version) != unicode(DB_VERSION):
                        raise Exception('Wrong db version')
                    if py_ver is None:
                        py_ver0 = 2
                    else:
                        py_ver0 = int(py_ver.split('.')[0])
                    if py_ver0 != sys.version_info[0]:
                        raise Exception('Mismatching python version, the '
                                        'database works with python %d and we '
                                        'are using python %d'
                                        % (py_ver0, sys.version_info[0]))
                except Exception as e:
                    cursor.close()
                    connection.close()
                    raise DatabaseError(str(e) + "\n\n"
                                        "Your database file might not be compatible "
                                        "with the current version of Soma-workflow.\n\n"
                                        "To solve the problem: \n  1. Log on the host"
                                        " " +
                                        repr(
                                            socket.gethostname()) + " (if it is not the current machine). \n  2. Delete"
                                        " the file " +
                                        str(database_file) + " \n"
                                        "  3. Clear the content of the directory: " + repr(tmp_file_dir_path))

    def __del__(self):
        # send VACUUM command ?
        pass

    def test(self):
        self.logger.debug("=======>Dans test")
        logging.info("Testing that the database_server is reachable as a remote object")
        return True


    def _connect(self):
        try:
            connection = sqlite3.connect(
                self._database_file, timeout=10, isolation_level="EXCLUSIVE",
                check_same_thread=False)
            # set journal_mode to TRUNCATE mode. On some systems / filesystems
            # / python versions (3), using the default DELETE mode can
            # cause some OperationalError : IO failure when commiting
            # transactions.
            cursor = connection.cursor()
            cursor.execute("PRAGMA journal_mode = TRUNCATE")
        except Exception as e:
            six.reraise(DatabaseError,
                        DatabaseError('On database file %s: %s: %s \n'
                                      % (self._database_file, type(e), e)),
                        sys.exc_info()[2])
        return connection

    def _user_transfer_dir_path(self, login, user_id):
        if hasattr(login, 'decode'): # python3 bytes object
            login = login.decode('utf8')
        path = os.path.join(
            self._tmp_file_dir_path, login + "_" + repr(user_id))
        return path  # supposes simple logins. Or use only the user id ?

    def register_user(self, login):
        '''
        Register a user so that he can submit job.

        Returns
        -------
        user_id: UserIdentifier
            user identifier
        '''
        self.logger.debug("=> register_user")
        with self._lock:
            connection = self._connect()
            cursor = connection.cursor()
            try:
                count = six.next(cursor.execute(
                    'SELECT count(*) FROM users WHERE login=?', [login]))[0]
                if count == 0:
                    cursor.execute(
                        'INSERT INTO users (login) VALUES (?)', [login])
                user_id = six.next(cursor.execute(
                    'SELECT id FROM users WHERE login=?', [login]))[0]
            except Exception as e:
                connection.rollback()
                cursor.close()
                connection.close()
                six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])
            connection.commit()
            cursor.close()
            connection.close()

            personal_path = self._user_transfer_dir_path(login, user_id)
            if not os.path.isdir(personal_path):
                try:
                    os.mkdir(personal_path)
                    os.chmod(personal_path, 0o775)
                except OSError:
                    pass
                
        if self._remove_orphan_files:
            self.remove_orphan_files()

        return user_id

    def clean(self):
        '''
        Delete all expired jobs, transfers and workflows, except transfers which are requested
        by valid job.
        '''
        self.logger.debug("=> clean")
        with self._lock:
            connection = self._connect()
            cursor = connection.cursor()

            try:
                #
                # Jobs and associated files (std out, std err and ressouce
                # usage file)
                jobsToDelete = []
                for row in cursor.execute(
                        'SELECT id FROM jobs WHERE expiration_date < ?',
                        [date.today()]):
                    jobsToDelete.append(row[0])

                maxv = sqlite3_max_variable_number()
                nmax = maxv
                if maxv == 0:
                    nmax = len(jobsToDelete)
                nchunks = int(math.ceil(float(len(jobsToDelete)) / nmax))

                for chunk in range(nchunks):
                    if chunk < nchunks - 1:
                        n = nmax
                    else:
                        n = len(jobsToDelete) - chunk * nmax
                    job_str = ','.join(['?'] * n)
                    cursor.execute('DELETE FROM ios WHERE job_id IN (%s)'
                                   % job_str, jobsToDelete[chunk * nmax:chunk * nmax + n])
                    cursor.execute(
                        'DELETE FROM ios_tmp WHERE job_id IN (%s)' % job_str,
                        jobsToDelete[chunk * nmax:chunk * nmax + n])

                    for stdof, stdef, ipf, opf in cursor.execute(
                            '''SELECT
                            stdout_file,
                            stderr_file,
                            input_params_file,
                            output_params_file
                            FROM jobs
                            WHERE id IN (%s) AND NOT custom_submission'''
                            % job_str,
                            jobsToDelete[chunk * nmax:chunk * nmax + n]):
                        self.__removeFile(self._string_conversion(stdof))
                        self.__removeFile(self._string_conversion(stdef))
                        self.__removeFile(self._string_conversion(ipf))
                        self.__removeFile(self._string_conversion(opf))

                cursor.execute(
                    'DELETE FROM jobs WHERE expiration_date < ?',
                    [date.today()])

                #
                # Transfers

                # get back the expired transfers
                transfersToDelete = {}
                for row in cursor.execute(
                        'SELECT id, engine_file_path FROM transfers WHERE expiration_date < ?',
                        [date.today()]):
                    transfersToDelete[row[0]] = row[1]

                # check that they are not currently used (as an input of output
                # of a job)
                if len(transfersToDelete) != 0:
                    transfers_list = keys(transfersToDelete)
                    nmax = maxv
                    if nmax == 0:
                        nmax = len(transfersToDelete)
                    nchunks = int(math.ceil(float(len(transfersToDelete))
                                            / nmax))
                    for chunk in range(nchunks):
                        if chunk < nchunks - 1:
                            n = nmax
                        else:
                            n = len(transfersToDelete) - chunk * nmax
                        for engine_file_id in cursor.execute(
                                'SELECT DISTINCT engine_file_id FROM ios '
                                'WHERE engine_file_id IN (%s)'
                                % ','.join(['?'] * n),
                                transfers_list[chunk * nmax:chunk * nmax + n]):
                            transfersToDelete.remove(engine_file_id)

                # delete transfers data and associated engine file
                if len(transfersToDelete) != 0:
                    transfers_list = keys(transfersToDelete)
                    nmax = maxv
                    if nmax == 0:
                        nmax = len(transfersToDelete)
                    nchunks = int(math.ceil(float(len(transfersToDelete))
                                            / nmax))
                    for chunk in range(nchunks):
                        if chunk < nchunks - 1:
                            n = nmax
                        else:
                            n = len(transfersToDelete) - chunk * nmax
                        cursor.execute(
                            'DELETE FROM transfers '
                            'WHERE id IN (%s)' % ','.join(['?'] * n),
                            transfers_list[chunk * nmax:chunk * nmax + n])
                    for engine_file_path in six.itervalues(transfersToDelete):
                        self.__removeFile(engine_file_path)

                #
                # temporary_paths

                # get back the expired temp_path_id
                tmpToDelete = {}
                for row in cursor.execute(
                        'SELECT temp_path_id, engine_file_path '
                        'FROM temporary_paths WHERE expiration_date < ?',
                        [date.today()]):
                    tmpToDelete[row[0]] = row[1]

                # check that they are not currently used (as an input of output
                # of a job)
                if len(tmpToDelete) != 0:
                    nmax = maxv
                    if nmax == 0:
                        nmax = len(tmpToDelete)
                    nchunks = int(math.ceil(float(len(tmpToDelete)) / nmax))
                    tkeys = keys(tmpToDelete)
                    for chunk in range(nchunks):
                        if chunk < nchunks - 1:
                            n = nmax
                        else:
                            n = len(tmpToDelete) - chunk * nmax
                        for temp_path_id in cursor.execute(
                                'SELECT DISTINCT temp_path_id FROM ios_tmp '
                                'WHERE temp_path_id IN (%s)'
                                % ','.join(['?'] * n),
                                tkeys[chunk * nmax:chunk * nmax + n]):
                            tmpToDelete.remove(temp_path_id)

                # delete temporary_paths data and associated engine file
                if len(tmpToDelete) != 0:
                    nmax = maxv
                    if nmax == 0:
                        nmax = len(tmpToDelete)
                    nchunks = int(math.ceil(float(len(tmpToDelete)) / nmax))
                    tkeys = keys(tmpToDelete)
                    for chunk in range(nchunks):
                        if chunk < nchunks - 1:
                            n = nmax
                        else:
                            n = len(tmpToDelete) - chunk * nmax
                        cursor.execute(
                            'DELETE FROM temporary_paths '
                            'WHERE temp_path_id IN (%s)'
                            % ','.join(['?'] * n),
                            tkeys[chunk * nmax:chunk * nmax + n])
                    for engine_file_path in six.itervalues(tmpToDelete):
                        self.__removeFile(engine_file_path)

                #
                # Workflows

                wf = cursor.execute(
                    'SELECT id FROM workflows WHERE expiration_date < ?',
                    [date.today()])
                wf = [w[0] for w in wf]
                cursor.execute(
                    'DELETE FROM workflows WHERE id IN (%s)'
                    % ','.join(['?'] * len(wf)),
                    wf)
                cursor.execute(
                    'DELETE FROM param_links WHERE workflow_id IN (%s)'
                    % ','.join(['?'] * len(wf)),
                    wf)

            except Exception as e:
                connection.rollback()
                cursor.close()
                connection.close()
                self.logger.error('%s: %s \n' % (str(type(e)), str(e)))
                tb = StringIO()
                traceback.print_exc(file=tb)
                self.logger.error(tb.getvalue())
                six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])

            cursor.close()
            connection.commit()
            connection.close()

            # self.remove_orphan_files()

    def vacuum(self):
        '''
        Resize the database file, so that it shrinks to the necessary size, not more.
        '''
        self.logger.debug('=> vacuum')
        with self._lock:
            connection = self._connect()
            cursor = connection.cursor()
            try:
                cursor.execute('VACUUM')
            except Exception as e:
                cursor.close()
                connection.close()
                six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])
            cursor.close()
            connection.close()

    def remove_orphan_files(self):
        print('remove orphan files')
        self.logger.debug("=> remove_orphan_files")
        registered_engine_paths = []
        registered_users = []
        with self._lock:
            connection = self._connect()
            cursor = connection.cursor()
            try:
                for row in cursor.execute('SELECT engine_file_path FROM transfers'):
                    engine_path = row[0]
                    registered_engine_paths.append(
                        self._string_conversion(engine_path))
                for row in cursor.execute('SELECT stdout_file FROM jobs'):
                    stdout_file = row[0]
                    if stdout_file:
                        registered_engine_paths.append(
                            self._string_conversion(stdout_file))
                for row in cursor.execute('SELECT stderr_file FROM jobs'):
                    stderr_file = row[0]
                    if stderr_file:
                        registered_engine_paths.append(
                            self._string_conversion(stderr_file))
                for row in cursor.execute(
                        'SELECT input_params_file FROM jobs'):
                    input_params_file = row[0]
                    if input_params_file:
                        registered_engine_paths.append(
                            self._string_conversion(input_params_file))
                for row in cursor.execute(
                        'SELECT output_params_file FROM jobs'):
                    output_params_file = row[0]
                    if output_params_file:
                        registered_engine_paths.append(
                            self._string_conversion(output_params_file))
                for row in cursor.execute('SELECT id, login FROM users'):
                    user_id, login = row
                    registered_users.append((user_id, login))
            except Exception as e:
                cursor.close()
                connection.close()
                six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])
            cursor.close()
            connection.close()

        for user_info in registered_users:
            user_id, login = user_info
            directory_path = self._user_transfer_dir_path(login, user_id)
            for name in os.listdir(directory_path):
                engine_path = os.path.join(directory_path, name)
                if not engine_path in registered_engine_paths:
                    self.logger.debug(
                        "remove_orphan_files, not registered " + engine_path + " to delete!")
                    self.__removeFile(engine_path)

    def reserve_file_numbers(self, external_cursor=None, num_files=200):
        '''
        Reserve a range of numbers in the fileCounter table, which may be used
        as suffix in files managed by Soma-Workflow on server side and stored n
        the database. Allocated numbers are stored internally in the
        self._free_file_counters list, and are guaranteed not to be reused by
        other database clients.

        Numbers are preallocated by blocks for efficiency matters: allocating
        them individually when needed, during databasing operations (open
        cursors) is a very high overhead and a severe performance bottleneck
        for workflow submission especially.

        Returns
        -------
        first_number: (int)
            first allocated number
        '''
        with self._lock:
            if not external_cursor:
                self.logger.debug("=> reserve_file_numbers")
                connection = self._connect()
                cursor = connection.cursor()
            else:
                cursor = external_cursor
            try:
                count = 0
                with cursor.connection:
                    for (count,) in cursor.execute(
                            'SELECT count FROM fileCounter'):
                        break
                    # UPDATE in sqlite during cursor execution may be
                    # *very* costy... (about 0.1 second per call)
                    cursor.execute(
                        'UPDATE fileCounter SET count=count+%d' % num_files)
                self._free_file_counters = list(range(count,
                                                      count + num_files))
                return count
            except Exception as e:
                if not external_cursor:
                    connection.rollback()
                six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])
            finally:
                if not external_cursor:
                    cursor.close()
                    connection.commit()
                    connection.close()

    def ensure_file_numbers_available(self, num_files, num_realloc=0,
                                      external_cursor=None):
        '''
        Make sure the internal preallocated file numbers stack contains enough
        elements. If not, more are allocated using reserve_file_numbers().

        Parameters
        ----------
        num_files: int
            number needed in the stack. If the stack is smaller, reallocation
            is performed
        num_realloc: int (default: 0)
            when reallocation is performed, this number is used. If smaller
            than the needed number (typically, when 0), the exact needed number
            is allocated.
        external_cursor: sqlite3 Cursor (optional)
            when reallocation is needed, the database cursor may be used.
        '''
        with self._lock:
            if len(self._free_file_counters) >= num_files:
                return
            num_alloc = num_files - len(self._free_file_counters)
            if num_realloc > num_alloc:
                num_alloc = num_realloc
            self.reserve_file_numbers(external_cursor, num_alloc)

    def get_new_file_number(self, external_cursor=None):
        '''
        Get a file counter number in the internal preallocated list, and
        generate new ones if needed.

        Used to allocate file names on server side for file transfers and
        stdout / stderr streams for jobs (see generate_file_path()).
        '''
        with self._lock:
            self.ensure_file_numbers_available(1, 200, external_cursor)
            return self._free_file_counters.pop(0)

    def generate_file_path(self,
                           user_id,
                           client_file_path=None,
                           external_cursor=None,
                           login=None):
        '''
        Generates file path for transfers.
        The user_id must be valid.

        Parameters
        ----------
        user_id: UserIdentifier
            user identifier
        client_file_path: string
            the generated name can derivate from this path.
        external_cursor: SQlite Cursor object (optionsl)
        login: user login corresponding to the id (optional)
            If specified, a SQL request is saved.

        Retuns
        ------
        file path: string
        '''

        self.logger.debug("=> generate_file_path")
        with self._lock:
            if not external_cursor:
                connection = self._connect()
                cursor = connection
            else:
                cursor = external_cursor
            if login is None:
                try:
                    login = six.next(cursor.execute(
                        'SELECT login FROM users WHERE id=?', [user_id]))[0]
                    # supposes that the user_id is valid
                    login = self._string_conversion(login)
                except Exception as e:
                    if not external_cursor:
                        connection.rollback()
                        connection.close()
                    six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])

            file_num = self.get_new_file_number(external_cursor)
            userDirPath = self._user_transfer_dir_path(login, user_id)
            if client_file_path == None:
                newFilePath = os.path.join(userDirPath, repr(file_num))
                # newFilePath += repr(file_num)
            else:
                client_base_name = os.path.basename(client_file_path)
                iextention = client_base_name.find(".")
                if iextention == -1:
                    newFilePath = os.path.join(
                        userDirPath, client_base_name + '_' + repr(file_num))
                    # newFilePath +=
                    # client_file_path[client_file_path.rfind("/")+1:] + '_' +
                    # repr(file_num)
                else:
                    newFilePath = os.path.join(
                        userDirPath, client_base_name[0:iextention] + '_'
                        + repr(file_num) + client_base_name[iextention:])
                    # newFilePath +=
                    # client_file_path[client_file_path.rfind("/")+1:iextention]
                    # + '_' + repr(file_num) + client_file_path[iextention:]
            if not external_cursor:
                connection.commit()
                connection.close()
            return newFilePath

    def __removeFile(self, file_path):
        if file_path and os.path.isdir(file_path):
            try:
                shutil.rmtree(file_path)
            except Exception as e:
                self.logger.debug(
                    "Could not remove file %s, error %s: %s \n" % (file_path, type(e), e))

        elif file_path and os.path.isfile(file_path):
            try:
                os.remove(file_path)
            except Exception as e:
                self.logger.debug(
                    "Could not remove file %s, error %s: %s \n" % (file_path, type(e), e))

    # "
    # TRANSFERS
    def add_transfer(self,
                     engine_transfer,
                     user_id,
                     expiration_date=None,
                     external_cursor=None):
        '''
        Adds a transfer to the database.

        Parameters
        ----------
        engine_transfer: EngineTransfer or EngineTemporaryPath
        expiration_date: date
        user_id:  UserIdentifier
        '''

        if isinstance(engine_transfer, TemporaryPath):
            return self.add_temporary_path(
                engine_transfer, user_id, expiration_date,
                external_cursor)

        if expiration_date == None:
            expiration_date = datetime.now() + timedelta(
                hours=engine_transfer.disposal_timeout)

        with self._lock:
            if not external_cursor:
                self.logger.debug("=> add_transfer")
                connection = self._connect()
                cursor = connection.cursor()
            else:
                cursor = external_cursor

            if engine_transfer.client_paths:
                engine_transfer.engine_path \
                    = self.generate_file_path(user_id, external_cursor=cursor)
            else:
                engine_transfer.engine_path \
                    = self.generate_file_path(user_id,
                                              engine_transfer.client_path,
                                              external_cursor=cursor)
            client_path_std = None
            if engine_transfer.client_paths:
                client_path_std = file_separator.join(
                    engine_transfer.client_paths)

            try:
                cursor.execute('''INSERT INTO transfers
                        (engine_file_path,
                         client_file_path,
                         transfer_date,
                         expiration_date,
                         user_id,
                         workflow_id,
                         status,
                         client_paths)
                        VALUES (?, ?, ?, ?,
                                ?, ?, ?, ?)''',
                              (engine_transfer.engine_path,
                               engine_transfer.client_path,
                               date.today(),
                               expiration_date,
                               user_id,
                               engine_transfer.workflow_id,
                               engine_transfer.status,
                               client_path_std))
                engine_transfer.transfer_id = cursor.lastrowid
            except Exception as e:
                if not external_cursor:
                    connection.rollback()
                    cursor.close()
                    connection.close()
                six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])
            if not external_cursor:
                cursor.close()
                connection.commit()
                connection.close()

        return engine_transfer

    def add_temporary_path(self,
                           engine_temp,
                           user_id,
                           expiration_date=None,
                           external_cursor=None):
        '''
        Adds a temporary file to the database.

        Parameters
        ----------
        engine_temp: EngineTemporaryPath
        expiration_date: date
        user_id:  UserIdentifier
        '''

        if expiration_date == None:
            expiration_date = datetime.now() + timedelta(
                hours=engine_temp.disposal_timeout)

        with self._lock:
            if not external_cursor:
                self.logger.debug("=> add_temporary_path")
                connection = self._connect()
                cursor = connection.cursor()
            else:
                cursor = external_cursor

            engine_path = engine_temp.get_engine_path()
            if engine_path is None:
                engine_path = ''

            try:
                cursor.execute('''INSERT INTO temporary_paths
                        (engine_file_path,
                         expiration_date,
                         user_id,
                         workflow_id,
                         status)
                        VALUES (?, ?, ?, ?, ?)''',
                              (engine_path,
                               expiration_date,
                               user_id,
                               engine_temp.workflow_id,
                               engine_temp.status))
                engine_temp.temp_path_id = cursor.lastrowid
            except Exception as e:
                if not external_cursor:
                    connection.rollback()
                    cursor.close()
                    connection.close()
                six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])
            if not external_cursor:
                cursor.close()
                connection.commit()
                connection.close()

        return engine_temp

    def _check_transfer(self, connection, cursor, transfer_id, user_id):
        try:
            sel = cursor.execute(
                '''SELECT id
                FROM transfers
                WHERE id=? and
                      user_id=? LIMIT 1''',
                [transfer_id, user_id])
        except Exception as e:
            cursor.close()
            connection.close()
            six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])

        try:
            six.next(sel)
        except StopIteration:
            six.reraise(
                UnknownObjectError,
                UnknownObjectError("The transfer " + repr(transfer_id)
                                   + " is not valid or does not belong to "
                                   "user " + repr(user_id)),
                sys.exc_info[2])

    def _check_temporary(self, connection, cursor, temp_path_id, user_id):
        try:
            sel = cursor.execute(
                '''SELECT temp_path_id
                FROM temporary_paths
                WHERE temp_path_id=? and
                      user_id=? LIMIT 1''',
                  [temp_path_id, user_id])
        except Exception as e:
            cursor.close()
            connection.close()
            six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])

        try:
            six.next(sel)
        except StopIteration:
            six.reraise(
                UnknownObjectError,
                UnknownObjectError("The temporary path " + repr(temp_path_id)
                                     + " is not valid or does not belong to "
                                     "user " + repr(user_id)),
                sys.exc_info[2])

    def remove_transfer(self, transfer_id, user_id):
        '''
        Set the expiration date of the transfer associated to the engine file path
        to today (yesterday?). That way it will be disposed as soon as no job will need it.

        Parameters
        ----------
        transfer_id: int
            transfer identifier record to delete.
        user_id: int
            user identifier
        '''
        self.logger.debug("=> remove_transfer")
        with self._lock:
            connection = self._connect()
            cursor = connection.cursor()
            self._check_transfer(connection, cursor, transfer_id, user_id)
            yesterday = date.today() - timedelta(days=1)
            try:
                cursor.execute(
                    'UPDATE transfers SET expiration_date=? WHERE id=?',
                    (yesterday, transfer_id))
            except Exception as e:
                connection.rollback()
                cursor.close()
                connection.close()
                six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])
            connection.commit()
            cursor.close()
            connection.close()
            self.clean()

    def remove_temporary(self, temp_path_id, user_id):
        '''
        Set the expiration date of the temporary_paths associated to the engine
        file path to today (yesterday?). That way it will be disposed as soon as no
        job will need it.

        Parameters
        ----------
        temp_path_id: int
            identifying the temporary path record to delete.
        user_id: int
            user identifier
        '''
        self.logger.debug("=> remove_temporary")
        with self._lock:
            connection = self._connect()
            cursor = connection.cursor()
            self._check_temporary(connection, cursor, temp_path_id, user_id)
            yesterday = date.today() - timedelta(days=1)
            try:
                cursor.execute(
                    'UPDATE temporary_paths SET expiration_date=? WHERE temp_path_id=?', (yesterday, temp_path_id))
            except Exception as e:
                connection.rollback()
                cursor.close()
                connection.close()
                six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])
            connection.commit()
            cursor.close()
            connection.close()
            self.clean()

    def get_transfer_information(self,
                                 transfer_id,
                                 user_id):
        '''
        Returns the information related to the transfer associated to the engine file path.
        The transfer_id must be associated to a transfer.
        Returns (None, None, None, -1, None) if the transfer_id is not associated to a transfer.

        Parameters
        ----------
        transfer_id: int
            identier
        user_id: int
            user identifier

        Returns
        -------
        info: tuple
            (tranfer_id, engine_file_path, client_file_path, expiration_date, workflow_id, client_paths, transfer_type, status)
        '''
        self.logger.debug("=> get_transfer_information")
        with self._lock:
            connection = self._connect()
            cursor = connection.cursor()
            self._check_transfer(connection, cursor, transfer_id, user_id)
            try:
                (engine_file_path,
                 client_file_path,
                 expiration_date,
                 workflow_id,
                 client_paths,
                 transfer_type,
                 status) = six.next(cursor.execute(
                    '''SELECT
                    engine_file_path,
                    client_file_path,
                    expiration_date,
                    workflow_id,
                    client_paths,
                    transfer_type,
                    status
                    FROM transfers
                    WHERE id=?''',
                    [transfer_id]))
            except Exception as e:
                cursor.close()
                connection.close()
                six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])

            engine_file_path = self._string_conversion(engine_file_path)
            client_file_path = self._string_conversion(client_file_path)
            expiration_date = self._str_to_date_conversion(expiration_date)
            if client_paths:
                client_paths = self._string_conversion(
                    client_paths).split(file_separator)
            else:
                client_path = None
            transfer_type = self._string_conversion(transfer_type)
            status = self._string_conversion(status)

            cursor.close()
            connection.close()
        return (transfer_id,
                engine_file_path,
                client_file_path,
                expiration_date,
                workflow_id,
                client_paths,
                transfer_type,
                status)

    def get_temporary_information(self,
                                  temp_path_id,
                                  user_id):
        '''
        Returns the information related to the temporary path associated to the id.
        The temp_path_id must be associated to a TemporaryPath.
        Returns (None, None, None, None, None) if the temp_path_id is not associated to a temporary path.

        Parameters
        ----------
        temp_path_id: int
            identifier

        Returns
        -------
        info: tuple
            (temp_path_id, engine_file_path, expiration_date, workflow_id, status)
        '''
        self.logger.debug("=> get_temporary_information")
        with self._lock:
            connection = self._connect()
            cursor = connection.cursor()
            self._check_temporary(connection, cursor, temp_path_id, user_id)
            try:
                (engine_file_path,
                 expiration_date,
                 workflow_id,
                 status) = six.next(cursor.execute(
                    '''SELECT
                    engine_file_path,
                    expiration_date,
                    workflow_id,
                    status
                    FROM temporary_paths
                    WHERE temp_path_id=?''',
                    [temp_path_id]))
            except Exception as e:
                cursor.close()
                connection.close()
                six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])

            engine_file_path = self._string_conversion(engine_file_path)
            expiration_date = self._str_to_date_conversion(expiration_date)
            status = self._string_conversion(status)

            cursor.close()
            connection.close()
        return (temp_path_id,
                engine_file_path,
                expiration_date,
                workflow_id,
                status)

    def get_transfer_status(self, transfer_id, user_id):
        '''
        Returns the transfer status stored in the database.
        '''
        self.logger.debug("=> get_transfer_status")
        with self._lock:
            connection = self._connect()
            cursor = connection.cursor()
            self._check_transfer(connection, cursor, transfer_id, user_id)
            try:
                status = six.next(cursor.execute(
                    'SELECT status FROM transfers WHERE id=?',
                    [transfer_id]))[0]
            except Exception as e:
                cursor.close()
                connection.close()
                six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])
            status = self._string_conversion(status)
            cursor.close()
            connection.close()

        return status

    def get_temporary_status(self, temp_path_id, user_id):
        '''
        Returns the temporary path status stored in the database.
        '''
        self.logger.debug("=> get_temporary_status")
        with self._lock:
            connection = self._connect()
            cursor = connection.cursor()
            self._check_temporary(connection, cursor, temp_path_id, user_id)
            try:
                status = six.next(cursor.execute(
                    'SELECT status FROM temporary_paths WHERE temp_path_id=?',
                    [temp_path_id]))[0]
            except Exception as e:
                cursor.close()
                connection.close()
                six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])
            status = self._string_conversion(status)
            cursor.close()
            connection.close()

        return status

    def set_transfer_status(self, transfer_id, status):
        '''
        Updates the transfer status in the database.
        The status must be valid (ie a string among the transfer status
        string defined in constants.FILE_TRANSFER_STATUS

        Parameters
        ----------
        transfer_id: int
            transfer identifier
        status: string
            transfer status as defined in constants.FILE_TRANSFER_STATUS
        '''
        #if type(engine_file_path) is int:
            #return self.set_temporary_status(engine_file_path, status)
        self.logger.debug("=> set_transfer_status")
        with self._lock:
            # TBI if the status is not valid raise an exception ??
            connection = self._connect()
            cursor = connection.cursor()
            try:
                cursor.execute(
                    'UPDATE transfers SET status=? WHERE id=?',
                    (status, transfer_id))
            except Exception as e:
                connection.rollback()
                cursor.close()
                connection.close()
                six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])
            connection.commit()
            cursor.close()
            connection.close()

    def set_transfer_paths(self, transfer_id, engine_path, client_path,
                           client_paths):
        '''
        Updates the transfer paths in the database.

        Parameters
        ----------
        transfer_id: int
            transfer identifier
        engine_path: str
            path on engine side
        client_path: str
            path on client side
        client_paths: list
            filenales on client side
        '''
        #if type(engine_file_path) is int:
            #return self.set_temporary_status(engine_file_path, status)
        self.logger.debug("=> set_transfer_paths")
        with self._lock:
            # TBI if the status is not valid raise an exception ??
            connection = self._connect()
            cursor = connection.cursor()
            if client_paths:
                client_paths = file_separator.join(
                    engine_transfer.client_paths)
            try:
                cursor.execute(
                    '''UPDATE transfers SET
                    engine_file_path=?,
                    client_file_path=?,
                    client_paths=?
                    WHERE id=?''',
                    (engine_path, client_path, client_paths, transfer_id))
            except Exception as e:
                connection.rollback()
                cursor.close()
                connection.close()
                six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])
            connection.commit()
            cursor.close()
            connection.close()

    def set_temporary_status(self, temp_path_id, status):
        '''
        Updates the temporary path status in the database.
        The status must be valid (ie a string among the transfer status
        string defined in constants.FILE_TRANSFER_STATUS

        @type  status: string
        @param status: transfer status as defined in constants.FILE_TRANSFER_STATUS
        '''
        self.logger.debug("=> set_temporary_status")
        with self._lock:
            # TBI if the status is not valid raise an exception ??
            connection = self._connect()
            cursor = connection.cursor()
            try:
                cursor.execute(
                    'UPDATE temporary_paths SET status=? WHERE temp_path_id=?',
                    (status, temp_path_id))
            except Exception as e:
                connection.rollback()
                cursor.close()
                connection.close()
                six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])
            connection.commit()
            cursor.close()
            connection.close()

    def set_transfer_type(self, transfer_id, transfer_type, user_id):
        self.logger.debug("=> set_transfer_type")
        with self._lock:
            connection = self._connect()
            cursor = connection.cursor()
            try:
                cursor.execute(
                    'UPDATE transfers SET transfer_type=? WHERE id=?', (transfer_type, transfer_id))
            except Exception as e:
                connection.rollback()
                cursor.close()
                connection.close()
                six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])
            connection.commit()
            cursor.close()
            connection.close()

    def add_workflow_ended_transfer(self, workflow_id, transfer_id):
        '''
        To signal that a transfer belonging to a workflow finished.
        '''
        self.logger.debug("=> add_workflow_ended_transfer")
        separator = ", "
        with self._lock:
            connection = self._connect()
            cursor = connection.cursor()
            try:
                str_ended_transfers = six.next(cursor.execute(
                    'SELECT ended_transfers FROM workflows WHERE id=?',
                    [workflow_id]))[0]
                if str_ended_transfers != None:
                    ended_transfers = self._string_conversion(
                        str_ended_transfers).split(separator)
                    ended_transfers.append(str(transfer_id))
                    str_ended_transfers = separator.join(ended_transfers)
                else:
                    str_ended_transfers = transfer_id
                cursor.execute(
                    'UPDATE workflows SET ended_transfers=? WHERE id=?',
                    (str_ended_transfers, workflow_id))
            except Exception as e:
                connection.rollback()
                cursor.close()
                connection.close()
                six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])
            connection.commit()
            cursor.close()
            connection.close()

    def pop_workflow_ended_transfer(self, workflow_id):
        '''
        Returns the ended transfers for a workflow and clear the ended transfer list.
        '''
        self.logger.debug("=> pop_workflow_ended_transfer")
        separator = ", "
        ended_transfers = []
        with self._lock:
            connection = self._connect()
            cursor = connection.cursor()
            try:
                str_ended_transfers = six.next(cursor.execute(
                    'SELECT ended_transfers FROM workflows WHERE id=?',
                    [workflow_id]))[0]
                if str_ended_transfers != None:
                    ended_transfers = self._string_conversion(
                        str_ended_transfers).split(separator)
                cursor.execute(
                    'UPDATE workflows SET ended_transfers=? WHERE id=?',
                    (None, workflow_id))
            except Exception as e:
                connection.rollback()
                cursor.close()
                connection.close()
                six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])
            connection.commit()
            cursor.close()
            connection.close()
        return ended_transfers

    #
    # WORKFLOWS

    def add_workflow(self,
                     user_id,
                     engine_workflow,
                     login=None):
        '''
        Register a workflow to the database and returns identifiers for every
        workflow element.

        * user_id *string*
          User identifier

        * engine_workflow *EngineWorkflow*

        * returns: * tuple(string, dictionary, dictionary)*
              * workflow identifier
              * dictionary tr_id -> EngineTransfer
              * dictionary job_id -> EngineJob
        '''
        # get back the workflow id first
        self.logger.debug("=> add_workflow")
        with self._lock:
            # try to allocate enough file counters before opening a new cursor
            needed_files = len(engine_workflow.transfer_mapping) \
                + len(engine_workflow.job_mapping) * 2
            self.ensure_file_numbers_available(needed_files)

            connection = self._connect()
            cursor = connection.cursor()
            name = None
            if engine_workflow.name != None:
                if sys.version_info[0] >= 3:
                    name = engine_workflow.name
                else:
                    name = engine_workflow.name.decode('utf8')
            try:
                cursor.execute('''INSERT INTO workflows
                         (user_id,
                          pickled_engine_workflow,
                          expiration_date,
                          name,
                          status,
                          last_status_update,
                          queue)
                          VALUES (?, ?, ?, ?, ?, ?, ?)''',
                              (user_id,
                                  None,
                                  engine_workflow.expiration_date,
                                  name,
                                  constants.WORKFLOW_NOT_STARTED,
                                  datetime.now(),
                                  engine_workflow.queue))

                engine_workflow.wf_id = cursor.lastrowid

                # the transfers must be registered before the jobs
                for transfer in six.itervalues(
                        engine_workflow.transfer_mapping):
                    transfer.workflow_id = engine_workflow.wf_id
                    self.add_transfer(transfer,
                                      user_id,
                                      engine_workflow.expiration_date,
                                      external_cursor=cursor)
                    if isinstance(transfer, FileTransfer):
                        engine_workflow.registered_tr[
                            transfer.transfer_id] = transfer
                    else:
                        engine_workflow.registered_tmp[
                            transfer.temp_path_id] = transfer

                job_info = []

                if login is None:
                    login = self.get_user_login(cursor)

                for job in six.itervalues(engine_workflow.job_mapping):
                    job.workflow_id = engine_workflow.wf_id
                    job = self.add_job(user_id,
                                       job,
                                       engine_workflow.expiration_date,
                                       external_cursor=cursor,
                                       login=login)
                    job_info.append(
                        (job.job_id, job.stdout_file, job.stderr_file))
                    engine_workflow.registered_jobs[job.job_id] = job

                pickled_workflow = pickle.dumps(engine_workflow,
                                                protocol=DB_PICKLE_PROTOCOL)

                cursor.execute('''UPDATE workflows
                          SET pickled_engine_workflow=?
                          WHERE id=?''',
                              (sqlite3.Binary(pickled_workflow),
                               engine_workflow.wf_id))

                for dest_job, links \
                        in six.iteritems(engine_workflow.param_links):
                    edest_job = engine_workflow.job_mapping[dest_job]
                    for dest_param, linkl in six.iteritems(links):
                        for link in linkl:
                            esrc_job = engine_workflow.job_mapping[link[0]]
                            func = None
                            if len(link) > 2:
                                func = sqlite3.Binary(pickle.dumps(link[2]))
                            cursor.execute(
                                '''INSERT INTO param_links
                                (workflow_id,
                                dest_job_id,
                                dest_param,
                                src_job_id,
                                src_param,
                                pickled_function)
                                VALUES (?, ?, ?, ?, ?, ?)''',
                                (engine_workflow.wf_id, edest_job.job_id,
                                dest_param, esrc_job.job_id, link[1], func))

            except Exception as e:
                connection.rollback()
                cursor.close()
                connection.close()
                six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])
            connection.commit()
            cursor.close()
            connection.close()

        self.logger.debug("==>end of add_workflow")
        return engine_workflow

    def delete_workflow(self, wf_id):
        '''
        Remove the workflow from the database. Remove all associated jobs and transfers.

        Parameters
        ----------
        wf_id: int
        '''
        self.logger.debug("=> delete_workflow")
        self.logger.debug("wf_id is: " + str(wf_id))
        with self._lock:
            # set expiration date to yesterday + clean() ?
            connection = self._connect()
            cursor = connection.cursor()

            yesterday = date.today() - timedelta(days=1)

            try:
                cursor.execute(
                    'UPDATE workflows SET expiration_date=? WHERE id=?', (yesterday, wf_id))
                cursor.execute(
                    'UPDATE jobs SET expiration_date=? WHERE workflow_id=?', (yesterday, wf_id))
                cursor.execute(
                    'UPDATE transfers SET expiration_date=? WHERE workflow_id=?', (yesterday, wf_id))
                cursor.execute(
                    'UPDATE temporary_paths SET expiration_date=? WHERE workflow_id=?', (yesterday, wf_id))
            except Exception as e:
                connection.rollback()
                cursor.close()
                connection.close()
                self.vacuum()
                six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])

            cursor.close()
            connection.commit()
            connection.close()
            self.clean()
            self.vacuum()

    def change_workflow_expiration_date(self, wf_id, new_date, user_id):
        '''
        Change the workflow expiration date.

        @type wf_id: int
        @type new_date: datetime.datetime
        '''
        self.logger.debug("=> change_workflow_expiration_date")
        with self._lock:
            connection = self._connect()
            cursor = connection.cursor()
            self._check_workflow(connection, cursor, wf_id, user_id)
            try:
                cursor.execute(
                    'UPDATE workflows SET expiration_date=? WHERE id=?', (new_date, wf_id))
            except Exception as e:
                connection.rollback()
                cursor.close()
                connection.close()
                six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])
            connection.commit()
            cursor.close()
            connection.close()

    def get_engine_workflow(self, wf_id, user_id):
        '''
        Returns a EngineWorkflow object.
        The wf_id must be valid.

        Parameters
        ----------
        wf_id: int

        Returns
        -------
        engine: EngineWorkflow
            workflow object
        '''
        self.logger.debug("=> get_engine_workflow")
        with self._lock:
            connection = self._connect()
            cursor = connection.cursor()
            self._check_workflow(connection, cursor, wf_id, user_id)

            try:
                pickled_workflow = six.next(cursor.execute(
                    '''SELECT
                    pickled_engine_workflow
                    FROM workflows WHERE id=?''',
                    [wf_id]))[0]
            except Exception as e:
                cursor.close()
                connection.close()
                six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])
            cursor.close()
            connection.close()

        if pickled_workflow:
            if sys.version_info[0] >= 3:
                workflow = pickle.loads(pickled_workflow, encoding='utf-8')
            else:
                workflow = pickle.loads(pickled_workflow)
        else:
            workflow = None

        return workflow

    def set_workflow_status(self, wf_id, status, force=False):
        '''
        Updates the workflow status in the database.
        The status must be valid (ie a string among the workflow status
        string defined in constants.WORKFLOW_STATUS)

        Parameters
        ----------
        wf_id: int
        status: str
            workflow status as defined in constants.WORKFLOW_STATUS
        '''
        self.logger.debug("=> set_workflow_status, wf_id: %s, status: %s"
                          % (wf_id, status))
        with self._lock:
            # TBI if the status is not valid raise an exception ??
            connection = self._connect()
            cursor = connection.cursor()
            try:
                prev_status = six.next(cursor.execute(
                    '''SELECT status
                    FROM workflows WHERE id=?''',
                    [wf_id]))[0]
                prev_status = self._string_conversion(prev_status)
                if force or \
                        (prev_status != constants.DELETE_PENDING and
                            prev_status != constants.KILL_PENDING):
                    cursor.execute('''UPDATE workflows
                        SET status=?,
                        last_status_update=?
                        WHERE id=?''',
                                  (status,
                                    datetime.now(),
                                    wf_id))
                    self.logger.debug("===> workflow_status updated")
                else:
                    self.logger.debug("===> (workflow_status not updated)")
            except Exception as e:
                connection.rollback()
                cursor.close()
                connection.close()
                self.logger.error(
                    "===> workflow_status update failed, error: %s, : %s"
                    % (str(type(e)), str(e)))
                six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])
            connection.commit()
            cursor.close()
            connection.close()

    def get_workflow_status(self, wf_id, user_id):
        '''
        Returns the workflow status stored in the database
        (updated by L{DrmaaWorkflowEngine}) and the date of its last update.
        '''
        self.logger.debug("=> get_workflow_status, wf_id: %s, user_id: %s"
            % (wf_id, user_id))
        with self._lock:
            connection = self._connect()
            cursor = connection.cursor()
            self._check_workflow(connection, cursor, wf_id, user_id)
            try:
                (status, strdate) = six.next(cursor.execute(
                    '''SELECT status, last_status_update
                    FROM workflows WHERE id=?''',
                    [wf_id]))
            except Exception as e:
                self.logger.exception("In get_workflow_status")
                cursor.close()
                connection.close()
                six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])
            status = self._string_conversion(status)
            date = self._str_to_date_conversion(strdate)
            cursor.close()
            connection.close()
        self.logger.debug("===> status: %s, date: %s" % (status, strdate))
        self.logger.debug("===> status: %s, date: %s" % (status, repr(date)))
        return status, date

    def get_detailed_workflow_status(self, wf_id, check_status=False,
                                     with_drms_id=True):
        '''
        Gets back the status of all the workflow elements at once, minimizing
        the requests to the database.

        Parameters
        ----------
        wf_id: int
        check_status: bool (optional, default=False)
            if True, check that a workflow with status RUNNING has actually
            some running or pending jobs. If not, set the state to DONE. It
            should not happen, and if it does, it's a bug (which has actually
            happened in Soma-Workflow <= 2.8.0)
        with_drms_id: bool (optional, default=False)
            if True the DRMS id (drmaa_id) is also included in the returned
            tuple for each job. This info has been added in soma_workflow 3.0
            and is thus optional to avoid breaking compatibility with earlier
            versions.

        Returns
        -------
        tuple (sequence of tuple (job_id,
                                  status,
                                  queue,
                                  exit_info,
                                  (submission_date,
                                    execution_date,
                                    ending_date),
                                  [drmaa_id]),
                sequence of tuple (transfer_id,
                                  client_file_path,
                                  client_paths,
                                  status,
                                  transfer_type),
                workflow_status,
                workflow_queue,
                sequence of tuple (temp_path_id,
                                  engine_path,
                                  status),
        )
        '''
        self.logger.debug("=> get_detailed_workflow_status, wf_id: %s" % wf_id)
        with self._lock:
            connection = self._connect()
            cursor = connection.cursor()

            try:
                # workflow status
                (wf_status, wf_queue) = six.next(cursor.execute(
                    '''SELECT
                    status,
                    queue
                    FROM workflows WHERE id=?''',
                    [wf_id]))  # supposes that the wf_id is valid

                workflow_status = ([], [], wf_status, wf_queue, [])
                # jobs
                for row in cursor.execute('''SELECT id,
                                            status,
                                            exit_status,
                                            exit_value,
                                            terminating_signal,
                                            resource_usage,
                                            submission_date,
                                            execution_date,
                                            ending_date,
                                            queue,
                                            drmaa_id
                                     FROM jobs WHERE workflow_id=?''',
                                     [wf_id]):
                    job_id, status, exit_status, exit_value, term_signal, \
                    resource_usage, submission_date, execution_date, \
                    ending_date, queue, drmaa_id = row

                    submission_date = self._str_to_date_conversion(
                        submission_date)
                    execution_date = self._str_to_date_conversion(
                        execution_date)
                    ending_date = self._str_to_date_conversion(ending_date)
                    queue = self._string_conversion(queue)

                    if with_drms_id:
                        workflow_status[0].append(
                            (job_id, status, queue,
                              (exit_status, exit_value, term_signal,
                              resource_usage),
                              (submission_date, execution_date, ending_date,
                              queue),
                              drmaa_id))
                    else:
                        workflow_status[0].append(
                            (job_id, status, queue,
                              (exit_status, exit_value, term_signal,
                              resource_usage),
                              (submission_date, execution_date, ending_date,
                              queue)))

                # transfers
                for row in cursor.execute('''SELECT id,
                                            engine_file_path,
                                            client_file_path,
                                            client_paths,
                                            status,
                                            transfer_type
                                     FROM transfers WHERE workflow_id=?''',
                                     [wf_id]):
                    (transfer_id,
                     engine_file_path,
                     client_file_path,
                     client_paths,
                     status,
                     transfer_type) = row

                    engine_file_path = self._string_conversion(
                        engine_file_path)
                    client_file_path = self._string_conversion(
                        client_file_path)
                    status = self._string_conversion(status)
                    transfer_type = self._string_conversion(transfer_type)
                    if client_paths:
                        client_paths = self._string_conversion(
                            client_paths).split(file_separator)
                    else:
                        client_paths = None

                    workflow_status[1].append((transfer_id,
                                               engine_file_path,
                                               client_file_path,
                                               client_paths,
                                               status,
                                               transfer_type))

                # temporary_paths
                for row in cursor.execute('''SELECT temp_path_id,
                                            engine_file_path,
                                            status
                                  FROM temporary_paths WHERE workflow_id=?''',
                                  [wf_id]):
                    (temp_path_id,
                     engine_file_path,
                     status) = row

                    engine_file_path = self._string_conversion(
                        engine_file_path)
                    status = self._string_conversion(status)

                    workflow_status[4].append((temp_path_id,
                                               engine_file_path,
                                               status))

            except Exception as e:
                cursor.close()
                connection.close()
                six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])
            cursor.close()
            connection.close()

        self.logger.debug("===> status: %s, queue: %s" % (wf_status, wf_queue))
        if check_status and wf_status == constants.WORKFLOW_IN_PROGRESS:
            done = []
            not_done = []
            for job_status in workflow_status[0]:
                if job_status[1] in (constants.DONE, constants.FAILED):
                    done.append(job_status[0])
                else:
                    not_done.append(job_status[0])
            self.logger.debug("===> ended jobs: %d, not ended: %d"
                % (len(done), len(not_done)))
            if len(not_done) == 0:
                self.logger.warning("=> Workflow status error: is RUNNING "
                                    "with no jobs left to be processed")
                self.logger.warning("=> fixing workflow status")
                self.set_workflow_status(wf_id, constants.WORKFLOW_DONE, True)
        return workflow_status

    #
    # JOBS
    def _check_job(self, connection, cursor, job_id, user_id):
        return self._check_jobs(connection, cursor, [job_id], user_id)

    def _check_jobs(self, connection, cursor, job_ids, user_id):
        maxv = sqlite3_max_variable_number()
        nmax = maxv
        if maxv == 0:
            nmax = len(job_ids)
        nchunks = int(math.ceil(float(len(job_ids)) / nmax))

        for chunk in range(nchunks):
            if chunk < nchunks - 1:
                n = nmax
            else:
                n = len(job_ids) - chunk * nmax
            job_str = ','.join(['?'] * n)
            try:
                sel = cursor.execute(
                    '''SELECT id FROM jobs WHERE id IN (%s) and user_id=?'''
                    % job_str,
                    job_ids[chunk * nmax:chunk * nmax + n] + [user_id])
            except Exception as e:
                cursor.close()
                connection.close()
                six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])

        ids = set()
        for job_id in sel:
            ids.add(int(job_id[0]))
        if len(ids) != len(job_ids):
            missing = [j for j in job_ids if j not in ids]
            raise UnknownObjectError(
                "The job ids " + ','.join([str(j) for j in missing])
                + " are not valid or do not belong to the user "
                + repr(user_id))

        return True

    def is_valid_job(self, job_id, user_id):
        self.logger.debug("=> is_valid_job")
        with self._lock:
            connection = self._connect()
            cursor = connection.cursor()
            last_status_update = None
            try:
                sel = cursor.execute(
                    '''SELECT last_status_update
                    FROM jobs
                    WHERE id=?''',
                    [job_id])
                last_status_update = six.next(sel)[0]
                count = 1
            except StopIteration:
                count = 0
            except Exception as e:
                cursor.close()
                connection.close()
                six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])
            cursor.close()
            connection.close()
            last_status_update = self._str_to_date_conversion(
                last_status_update)
        return (count != 0, last_status_update)


    def get_user_login(self, user_id, external_cursor=None):
        self.logger.debug("=> get_user_login")
        with self._lock:
            if not external_cursor:
                connection = self._connect()
                cursor = connection
            else:
                cursor = external_cursor
            try:
                login = six.next(cursor.execute(
                    'SELECT login FROM users WHERE id=?', [user_id]))[0]
                # supposes that the user_id is valid
                login = self._string_conversion(login)
            except Exception as e:
                if not external_cursor:
                    connection.rollback()
                    connection.close()
                six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])
            if not external_cursor:
                connection.commit()
                connection.close()
            return login


    def add_job(self,
                user_id,
                engine_job,
                expiration_date=None,
                external_cursor=None,
                login=None):
        '''
        Adds a job to the database and returns its identifier.

        Parameters
        ----------
        user_id: UserIdentifier
        engine_job: EngineJob

        Returns
        -------
        job_desc: tuple
            the identifier of the job:
            (JobIdentifier, stdout_file_path, stderr_file_path)
        '''
        expiration_date = expiration_date
        if expiration_date == None:
            expiration_date = datetime.now() + timedelta(
                hours=engine_job.disposal_timeout)

        parallel_config_name = None
        nodes_number = 1
        cpu_per_node = 1
        if engine_job.parallel_job_info:
            parallel_config_name \
                = engine_job.parallel_job_info.get('config_name')
            nodes_number = engine_job.parallel_job_info.get('nodes_number', 1)
            cpu_per_node = engine_job.parallel_job_info.get('cpu_per_node', 1)
        command_info = ""
        for command_element in engine_job.plain_command():
            command_info = command_info + " " + repr(command_element)

        with self._lock:
            if not external_cursor:
                self.logger.debug("=> add_job")
                connection = self._connect()
                cursor = connection.cursor()
            else:
                cursor = external_cursor

            if login is None:
                login = self.get_user_login(user_id, cursor)

            try:

                if not engine_job.plain_stdout():
                    engine_job.stdout_file = self.generate_file_path(
                        user_id, external_cursor=cursor, login=login)
                    engine_job.stderr_file = self.generate_file_path(
                        user_id, external_cursor=cursor, login=login)
                    custom_submission = False  # the std out and err file has to be removed with the job
                else:
                    custom_submission = True  # the std out and err file won't to be removed with the job

                if engine_job.use_input_params_file \
                        and not engine_job.plain_input_params_file():
                    engine_job.input_params_file = self.generate_file_path(
                        user_id, external_cursor=cursor, login=login)

                if engine_job.has_outputs \
                        and not engine_job.plain_output_params_file():
                    engine_job.output_params_file = self.generate_file_path(
                        user_id, external_cursor=cursor, login=login)

                referenced_input_files = []
                referenced_input_temp = []
                for ft in engine_job.referenced_input_files:
                    eft = engine_job.transfer_mapping[ft]
                    if isinstance(eft, FileTransfer):
                        referenced_input_files.append(eft.transfer_id)
                    else:
                        referenced_input_temp.append(eft.temp_path_id)

                referenced_output_files = []
                referenced_output_temp = []
                for ft in engine_job.referenced_output_files:
                    eft = engine_job.transfer_mapping[ft]
                    if isinstance(eft, FileTransfer):
                        referenced_output_files.append(eft.engine_path)
                    else:
                        referenced_output_temp.append(eft.temp_path_id)

                cursor.execute('''INSERT INTO jobs
                         (user_id,

                          drmaa_id,
                          expiration_date,
                          status,
                          last_status_update,
                          workflow_id,

                          command,
                          stdin_file,
                          join_errout,
                          stdout_file,
                          stderr_file,
                          working_directory,
                          custom_submission,
                          parallel_config_name,
                          nodes_number,
                          cpu_per_node,
                          queue,
                          input_params_file,
                          output_params_file,

                          name,
                          submission_date,
                          execution_date,
                          ending_date,

                          exit_status,
                          exit_value,
                          terminating_signal,
                          resource_usage,

                          pickled_engine_job)
                          VALUES (?, ?, ?, ?, ?,
                                  ?, ?, ?, ?, ?,
                                  ?, ?, ?, ?, ?,
                                  ?, ?, ?, ?, ?,
                                  ?, ?, ?, ?, ?,
                                  ?, ?, ?)''',
                              (user_id,

                                  None,  # drmaa_id
                                  expiration_date,
                                  constants.NOT_SUBMITTED,  # status
                                  datetime.now(),  # last_status_update
                                  engine_job.workflow_id,

                                  command_info,
                                  engine_job.plain_stdin(),
                                  engine_job.join_stderrout,
                                  engine_job.plain_stdout(),
                                  engine_job.plain_stderr(),
                                  engine_job.plain_working_directory(),
                                  custom_submission,
                                  parallel_config_name,
                                  nodes_number,
                                  cpu_per_node,
                                  engine_job.queue,
                                  engine_job.plain_input_params_file(),
                                  engine_job.plain_output_params_file(),

                                  engine_job.name,
                                  None,  # submission_date,
                                  None,  # execution_date,
                                  None,  # ending_date,
                                  None,  # exit_status,
                                  None,  # exit_value,
                                  None,  # terminating_signal,
                                  None,  # resource_usage,

                                  None  # pickled_engine_job
                               ))

                job_id = cursor.lastrowid
                engine_job.job_id = job_id
                if not engine_job.workflow_id or engine_job.workflow_id == -1:
                    pickled_engine_job = pickle.dumps(
                        engine_job, protocol=DB_PICKLE_PROTOCOL)
                    cursor.execute(
                        'UPDATE jobs SET pickled_engine_job=? WHERE id=?',
                                  (sqlite3.Binary(pickled_engine_job), job_id))

                for transfer_id in referenced_input_files:
                    cursor.execute('''INSERT INTO ios (job_id,
                                             engine_file_id,
                                             is_input)
                             VALUES (?, ?, ?)''',
                                  (job_id, transfer_id, True))

                for transfer_id in referenced_output_files:
                    cursor.execute('''INSERT INTO ios (job_id,
                                             engine_file_id,
                                             is_input)
                            VALUES (?, ?, ?)''',
                                   (job_id, transfer_id, False))

                for temp_path_id in referenced_input_temp:
                    cursor.execute('''INSERT INTO ios_tmp (job_id,
                                             temp_path_id,
                                             is_input)
                             VALUES (?, ?, ?)''',
                                  (job_id, temp_path_id, True))

                for temp_path_id in referenced_output_temp:
                    cursor.execute('''INSERT INTO ios_tmp (job_id,
                                             temp_path_id,
                                             is_input)
                             VALUES (?, ?, ?)''',
                                  (job_id, temp_path_id, False))

            except Exception as e:
                if not external_cursor:
                    connection.rollback()
                    cursor.close()
                    connection.close()
                six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])
            if not external_cursor:
                connection.commit()
                cursor.close()
                connection.close()

        return engine_job

    def update_job_command(self, job_id, commandline):
        self.logger.debug("=> update_job_command " + str(job_id) + ':'
                          + repr(commandline))
        command_info = []
        for command_element in commandline:
            selem = repr(command_element)
            if selem.startswith('u'):
                selem = selem[1:]
            command_info.append(selem)
        command_info = u' '.join(command_info)
        with self._lock:
            connection = self._connect()
            cursor = connection.cursor()
            try:
                try:
                    cursor.execute(
                        'UPDATE jobs SET command=? WHERE id=?',
                        (command_info, job_id))
                    connection.commit()
                except:
                    connection.rollback()
                    raise
            finally:
                cursor.close()
                connection.close()

    def get_job_command(self, job_id):
        self.logger.debug("=> get_job_command " + str(job_id))
        with self._lock:
            connection = self._connect()
            cursor = connection.cursor()
            try:
                command = six.next(cursor.execute(
                    'SELECT command FROM jobs WHERE id=?', [job_id]))
            except Exception as e:
                cursor.close()
                connection.close()
                six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])
            cursor.close()
            connection.close()
        return command[0]

    def get_engine_job(self, job_id, user_id):
        '''
        Returns a EngineJob object.
        The job_id must be valid.

        @type job_id: C{JobIdentifier}
        @rtype: C{EngineJob}
        @return: workflow object
        '''
        self.logger.debug("=> get_engine_job %d" % job_id)
        with self._lock:
            connection = self._connect()
            cursor = connection.cursor()
            self._check_job(connection, cursor, job_id, user_id)
            try:
                (pickled_job, workflow_id) = six.next(cursor.execute(
                    '''SELECT
                    pickled_engine_job,
                    workflow_id
                    FROM jobs WHERE id=?''', [job_id]))
            except Exception as e:
                cursor.close()
                connection.close()
                six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])
            cursor.close()
            connection.close()

        if pickled_job:
            if sys.version_info[0] >= 3:
                job = pickle.loads(pickled_job, encoding='utf-8')
            else:
                job = pickle.loads(pickled_job)
            job.job_id = job_id
        else:
            job = None
            if workflow_id not in (None, -1):
                # job is not pickled: get the whole workflow
                workflow = self.get_engine_workflow(workflow_id, user_id)
                jobs = [workflow.job_mapping[j] for j in workflow.jobs]
                jobs = [j for j in jobs if j.job_id == job_id]
                if len(jobs) != 0:
                    job = jobs[0]

        return (job, workflow_id)

    def delete_job(self, job_id):
        '''
        Remove the job from the database. Remove all associated transfered files if
        their expiration date passed and they are not used by any other job.

        @type job_id:
        '''
        self.logger.debug("=> delete_job")
        with self._lock:
            # set expiration date to yesterday + clean() ?
            connection = self._connect()
            cursor = connection.cursor()

            yesterday = date.today() - timedelta(days=1)

            try:
                cursor.execute(
                    'UPDATE jobs SET expiration_date=? WHERE id=?', (yesterday, job_id))

            except Exception as e:
                connection.rollback()
                cursor.close()
                connection.close()
                six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])

            cursor.close()
            connection.commit()
            connection.close()
            self.clean()

    def set_queue(self, queue_name, job_ids, wf_id=None):
        '''
        job_ids: list of job_id

        queue_name: string
        '''
        with self._lock:
            # TBI if the status is not valid raise an exception ??
            connection = self._connect()
            cursor = connection.cursor()
            try:
                if wf_id != None:
                    cursor.execute(
                        '''UPDATE workflows SET queue=? WHERE id=?''',
                        (queue_name, wf_id))

                cursor.execute(
                    '''UPDATE jobs SET queue=? WHERE id in (%s)'''
                    % ','.join(['?'] * len(job_ids)),
                    list(itertools.chain((queue_name, ), job_ids)))
            except Exception as e:
                connection.rollback()
                cursor.close()
                connection.close()
                six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])
            connection.commit()
            cursor.close()
            connection.close()

    def set_jobs_status(self, job_status, force=False):
        '''
        job_status: dictionary: job_id -> status
        '''
        self.logger.debug("=> set_jobs_status")
        with self._lock:
            # TBI if the status is not valid raise an exception ??
            connection = self._connect()
            statuses = []

            # execute all queries before writing in the database, it's
            # more efficient.
            nmax = sqlite3_max_variable_number()
            if nmax == 0:
                nmax = len(job_status)
            sel = []
            nchunks = int(math.ceil(float(len(job_status)) / nmax))
            jkeys = keys(job_status)
            for chunk in range(nchunks):
                if chunk < nchunks - 1:
                    n = nmax
                else:
                    n = len(job_status) - chunk * nmax
                sel = connection.execute(
                    ''' SELECT id,
                            status,
                            last_status_update,
                            execution_date,
                            ending_date
                    FROM jobs WHERE id IN (%s)'''
                    % ','.join('?' * n), jkeys[chunk * nmax:chunk * nmax + n])
                for (job_id, previous_status, last_update, execution_date,
                     ending_date) in sel:
                    status = job_status[job_id]
                    previous_status = self._string_conversion(
                        previous_status)
                    execution_date = self._str_to_date_conversion(
                        execution_date)
                    ending_date = self._str_to_date_conversion(ending_date)
                    statuses.append((job_id, status, previous_status,
                                     last_update, execution_date,
                                     ending_date))

            cursor = connection.cursor()
            now = datetime.now()
            date_to_update = []
            try:
                for (job_id, status, previous_status, last_update,
                     execution_date, ending_date) in statuses:
                    do_update = force or \
                        (previous_status != constants.DELETE_PENDING and
                          previous_status != constants.KILL_PENDING)
                    if previous_status != status:
                        if not execution_date \
                                and status == constants.RUNNING:
                            execution_date = now
                        if not ending_date and status == constants.DONE \
                                or status == constants.FAILED:
                            ending_date = now
                            if not execution_date:
                                execution_date = now
                    else:
                        # if status has not changed, do not update to
                        # save load on the database
                        do_update = False
                        # update just last_status_update after a given
                        # time (typically 30 s), all jobs at once
                        if force or now \
                                - self._str_to_date_conversion(last_update) \
                                > update_interval:
                            date_to_update.append(job_id)
                    if do_update:
                        cursor.execute('''UPDATE jobs SET status=?,
                                            last_status_update=?,
                                            execution_date=?,
                                            ending_date=? WHERE id=?''',
                                       (status, now, execution_date,
                                        ending_date, job_id))
                if len(date_to_update) != 0:
                    # update last_status_update for all jobs which may
                    # become outdated
                    nmax -= 1
                    nchunks = int(math.ceil(float(len(date_to_update)) / nmax))
                    for chunk in range(nchunks):
                        if chunk < nchunks - 1:
                            n = nmax
                        else:
                            n = len(date_to_update) - chunk * nmax
                        cursor.execute(
                            'UPDATE jobs SET last_status_update=? '
                            'WHERE id IN (%s)'
                            % ','.join(['?'] * n),
                            [now] + date_to_update[chunk * nmax:
                                                   chunk * nmax + n])
            except Exception as e:
                connection.rollback()
                cursor.close()
                connection.close()
                six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])
            #connection.commit()
            try:
                connection.commit()
            except:
                print('DB error on file:', self._database_file, file=sys.stderr)
                cursor.close()
                connection.close()
                raise
            cursor.close()
            connection.close()

    def set_job_status(self, job_id, status, force=False):
        '''
        Updates the job status in the database.
        The status must be valid (ie a string among the job status
        string defined in constants.JOB_STATUS

        @type  status: string
        @param status: job status as defined in constants.JOB_STATUS
        '''
        self.logger.debug("=> set_job_status")
        with self._lock:
            # TBI if the status is not valid raise an exception ??
            connection = self._connect()
            sel = connection.execute(
                        ''' SELECT status,
                              execution_date,
                              ending_date
                        FROM jobs WHERE id=?''',
                        [job_id])
            try:
                (previous_status,
                 execution_date,
                 ending_date) = six.next(sel)
            except StopIteration:
                # job does not exist
                connection.close()
                return

            previous_status = self._string_conversion(previous_status)
            execution_date = self._str_to_date_conversion(
                execution_date)
            ending_date = self._str_to_date_conversion(ending_date)
            if previous_status != status:
                if not execution_date and status == constants.RUNNING:
                    execution_date = datetime.now()
                if not ending_date and status == constants.DONE or \
                    status == constants.FAILED:
                    ending_date = datetime.now()
                    if not execution_date:
                        execution_date = datetime.now()
            if force or \
                    (previous_status != constants.DELETE_PENDING and
                     previous_status != constants.KILL_PENDING):
                try:
                    connection.execute('''UPDATE jobs SET status=?,
                                          last_status_update=?,
                                          execution_date=?,
                                          ending_date=? WHERE id=?''',
                                  (status, datetime.now(),
                                    execution_date, ending_date,
                                    job_id))
                except Exception as e:
                    connection.rollback()
                    connection.close()
                    six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])
            connection.commit()
            connection.close()

    def get_job_status(self, job_id, user_id):
        '''
        Returns the job status stored in the database and
        the date of its last update.
        Raise UnknownObjectError if the job_id is not valid or belongs to an
        other user.
        '''
        return self.get_jobs_status([job_id], user_id)[0]

    def get_jobs_status(self, job_ids, user_id):
        '''
        Returns the jobs status stored in the database and
        the date of their last update.
        Raise UnknownObjectError if a job_id is not valid or belongs to an
        other user.
        '''
        self.logger.debug("=> get_jobs_status")
        with self._lock:
            connection = self._connect()
            cursor = connection.cursor()
            self._check_jobs(connection, cursor, job_ids, user_id)

            maxv = sqlite3_max_variable_number()
            nmax = maxv
            if maxv == 0:
                nmax = len(job_ids)
            nchunks = int(math.ceil(float(len(job_ids)) / nmax))
            for chunk in range(nchunks):
                if chunk < nchunks - 1:
                    n = nmax
                else:
                    n = len(job_ids) - chunk * nmax

                status = []
                try:
                    for s, strdate in cursor.execute(
                            '''SELECT status, last_status_update
                            FROM jobs
                            WHERE id IN (%s)''' % ','.join(['?'] * n),
                            job_ids[chunk * nmax:chunk * nmax + n]):
                        s = self._string_conversion(s)
                        date = self._str_to_date_conversion(strdate)
                        status.append((s, date))
                except Exception as e:
                    cursor.close()
                    connection.close()
                    six.reraise(DatabaseError, DatabaseError(e),
                                sys.exc_info()[2])
            cursor.close()
            connection.close()

        return status

    def set_submission_information(self, drmaa_ids, submission_date):
        '''
        Set the submission information of the job and reset information
        related to the job submission (execution_date, ending_date,
        exit_status, exit_value, terminating_signal, resource_usage) .

        *drmaa_ids: dictionary job_id -> drmaa_id
        *submission_date: submission date if the job was submitted
        '''
        self.logger.debug("=> set_submission_information")
        with self._lock:
            connection = self._connect()
            cursor = connection.cursor()
            try:
                for job_id, drmaa_id in six.iteritems(drmaa_ids):
                    cursor.execute('''UPDATE jobs
                            SET drmaa_id=?,
                                submission_date=?,
                                status=?,
                                last_status_update=?,
                                exit_status=?,
                                exit_value=?,
                                terminating_signal=?,
                                resource_usage=?,
                                execution_date=?,
                                ending_date=?
                                WHERE id=?''',
                                  (drmaa_id,
                                   submission_date,
                                   constants.UNDETERMINED,
                                   datetime.now(),
                                   None,
                                   None,
                                   None,
                                   None,
                                   None,
                                   None,
                                   job_id))
            except Exception as e:
                connection.rollback()
                cursor.close()
                connection.close()
                six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])
            connection.commit()
            cursor.close()
            connection.close()

    def set_job_output_params(self, job_id, param_dict):
        '''
        Updates the job output parameters dict.
        '''
        self.logger.debug("=> set_job_output_params")
        with self._lock:
            connection = self._connect()
            try:
                connection.execute(
                    'UPDATE jobs SET output_params=? WHERE id=?',
                    (json.dumps(utils.to_json(param_dict)),
                     job_id))
            except Exception as e:
                connection.rollback()
                connection.close()
                six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])
            connection.commit()
            connection.close()

    def get_job_output_params(self, job_id):
        '''
        Returns the job output parameters dict.
        '''
        self.logger.debug("=> get_job_output_params")
        with self._lock:
            connection = self._connect()
            cursor = connection.cursor()
            try:
                json_sql = cursor.execute(
                    'SELECT output_params FROM jobs WHERE id=?', [job_id])
                jstr = six.next(json_sql)[0]
                if jstr is None:
                    return None
                jdict = utils.from_json(json.loads(jstr))
                return jdict
            finally:
                cursor.close()
                connection.close()

    def updated_job_parameters(self, job_id):
        '''
        '''
        self.logger.debug("=> updated_job_parameters %d" % job_id)
        # get job workflow id
        with self._lock:
            connection = self._connect()
            cursor = connection.cursor()
            cursor2 = connection.cursor()
            try:

                sel = cursor.execute(
                    'SELECT dest_param, src_job_id, src_param, '
                    'pickled_function '
                    'FROM param_links WHERE dest_job_id=?', [job_id])

                param_dict = {}
                jsons = {}
                for dst_param, src_job, src_param, func in sel:
                    # print(dst_param, src_job, src_param)
                    #if dest_param in param_dict:
                    if func:
                        if sys.version_info[0] >= 3:
                            func = pickle.loads(func, encoding='utf-8')
                        else:
                            func = pickle.loads(func)
                    jdict = jsons.get(src_job)
                    if jdict is None:
                        json_sql = cursor2.execute(
                            'SELECT output_params FROM jobs WHERE id=?',
                            [src_job])
                        jstr = six.next(json_sql)[0]
                        if jstr is not None:
                            jdict = utils.from_json(json.loads(jstr))
                        else:
                            jdict = {}
                        jsons[src_job] = jdict
                    if src_param in jdict:
                        param_dict.setdefault(dst_param, []).append(
                            (func, src_param, jdict[src_param]))

            finally:
                cursor2.close()
                cursor.close()
                connection.close()

            return param_dict


    def get_drmaa_job_id(self, job_id):
        '''
        Returns the DRMAA job id associated with the job.
        Returns None if the job_id is not valid.

        @type job_id: C{JobIdentifier}
        @rtype: string
        @return: DRMAA job identifier (job identifier on DRMS if submitted via DRMAA)
        '''
        self.logger.debug("=> get_drmaa_job_id")
        with self._lock:
            connection = self._connect()
            cursor = connection.cursor()
            try:
                sel = cursor.execute(
                    'SELECT drmaa_id FROM jobs WHERE id=?',
                    [job_id])
            except Exception as e:
                cursor.close()
                connection.close()
                six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])

            try:
                drmaa_id = six.next(sel)[0]
            except StopIteration:
                drmaa_id = None

            cursor.close()
            connection.close()
            return drmaa_id

    def get_std_out_err_file_path(self, job_id, user_id):
        '''
        Returns the path of the standard output and error files.
        The job_id must be valid.

        Parameters
        ----------
        job_id: JobIdentifier

        Returns
        -------
        paths: tuple
            (stdout_file_path, stderr_file_path)
        '''
        self.logger.debug("=> get_std_out_err_file_path")
        with self._lock:
            connection = self._connect()
            cursor = connection.cursor()
            try:
                sel = cursor.execute(
                    'SELECT stdout_file, stderr_file FROM jobs WHERE id=?',
                    [job_id])
            except Exception as e:
                cursor.close()
                connection.close()
                six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])

            try:
                result = six.next(sel)
            except StopIteration:
                cursor.close()
                connection.close()
                raise UnknownObjectError("The job id " + repr(job_id)
                                         + " is not valid or does not belong "
                                         "to user " + repr(user_id))

            cursor.close()
            connection.close()
        stdout_file_path = self._string_conversion(result[0])
        stderr_file_path = self._string_conversion(result[1])
        return (stdout_file_path, stderr_file_path)

    def get_job_output_params_file_path(self, job_id, user_id):
        '''
        Returns the path of the output parameters file (if any).
        The job_id must be valid.
        '''
        self.logger.debug("=> get_job_output_params_file_path")
        with self._lock:
            connection = self._connect()
            cursor = connection.cursor()
            try:
                sel = cursor.execute(
                    'SELECT output_params_file FROM jobs WHERE id=?',
                    [job_id])
            except Exception as e:
                cursor.close()
                connection.close()
                six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])

            try:
                result = six.next(sel)
            except StopIteration:
                cursor.close()
                connection.close()
                raise UnknownObjectError("The job id " + repr(job_id)
                                         + " is not valid or does not belong "
                                         "to user " + repr(user_id))

            cursor.close()
            connection.close()
        output_params_file = self._string_conversion(result[0])
        return output_params_file

    def get_job_exit_info(self, job_id, user_id):
        '''
        Returns the job exit informations.

        @type job_id: C{JobIdentifier}
        @rtype: tuple
        @return: (exit_status, exit_value, terminating_signal, resource_usage)
        '''
        self.logger.debug("=> get_job_exit_info")
        with self._lock:
            connection = self._connect()
            cursor = connection.cursor()
            self._check_job(connection, cursor, job_id, user_id)
            try:
                result = six.next(cursor.execute(
                    '''SELECT exit_status,
                              exit_value,
                              terminating_signal,
                              resource_usage
                    FROM jobs WHERE id=?''',
                    [job_id]))
            except Exception as e:
                cursor.close()
                connection.close()
                six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])
            cursor.close()
            connection.close()
        exit_status = self._string_conversion(result[0])
        exit_value = result[1]
        terminating_signal = self._string_conversion(result[2])
        resource_usage = self._string_conversion(result[3])

        return (exit_status, exit_value, terminating_signal, resource_usage)

    def set_jobs_exit_info(self, job_dict):
        self.logger.debug("=> set_jobs_exit_info")
        with self._lock:
            connection = self._connect()
            cursor = connection.cursor()
            try:
                for job_id, job in six.iteritems(job_dict):
                    self.set_job_exit_info(job_id,
                                           job.exit_status,
                                           job.exit_value,
                                           job.terminating_signal,
                                           job.str_rusage,
                                           cursor)
            except Exception as e:
                cursor.close()
                connection.close()
                six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])
            connection.commit()
            cursor.close()
            connection.close()

    def set_job_exit_info(self,
                          job_id,
                          exit_status,
                          exit_value,
                          terminating_signal,
                          resource_usage,
                          external_cursor=None):
        '''
        Record the job exit status in the database.
        The status must be valid (ie a string among the exit job status
        string defined in L{WorkflowDatabaseServer}.

        @type  job_id: C{JobIdentifier}
        @param job_id: job identifier
        @type  exit_status: string
        @param exit_status: exit status string as defined in L{WorkflowDatabaseServer}
        @type  exit_value: int or None
        @param exit_value: if the status is FINISHED_REGULARLY, it contains the operating
        system exit code of the job.
        @type  terminating_signal: string or None
        @param terminating_signal: if the status is FINISHED_TERM_SIG, it contain a representation
        of the signal that caused the termination of the job.
        @type  resource_usage: string
        @param resource_usage: contain the resource usage information of
        the job.
        '''
        with self._lock:
            # TBI if the status is not valid raise an exception ??
            if not external_cursor:
                self.logger.debug("=> set_job_exit_info")
                connection = self._connect()
                cursor = connection.cursor()
            else:
                cursor = external_cursor
            try:
                cursor.execute('''UPDATE jobs SET exit_status=?,
                                      exit_value=?,
                                      terminating_signal=?,
                                      resource_usage=?
                                      WHERE id=?''',
                              (exit_status,
                                exit_value,
                                terminating_signal,
                                resource_usage,
                                job_id)
                                )
            except Exception as e:
                if not external_cursor:
                    connection.rollback()
                    cursor.close()
                    connection.close()
                six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])
            if not external_cursor:
                connection.commit()
                cursor.close()
                connection.close()

    def _string_conversion(self, string):
        # return string
        if string:
            if sys.version_info[0] < 3:
                return string.encode('utf-8')
            else:
                if isinstance(string, bytes):
                    return string.decode('utf-8')
        return string

    def _str_to_date_conversion(self, strdate):
        if strdate:
            if sys.version_info[0] < 3:
                strdate = strdate.encode('utf-8')
            # this is a hack to avoid issues because
            # for an undetermined reason the date may be stored in
            # a different format in the database.
            try:
                date = datetime.strptime(strdate, strtime_format)
            except ValueError:
                date = datetime.strptime(strdate, '%Y-%m-%d')
        else:
            date = None
        return date

    # DATABASE QUERYING ##############################
    # JOBS
    def get_jobs(self, user_id, job_ids=None):
        '''
        Returns the jobs owned by the user or
        specified in the sequence job_ids

        @type user_id: C{UserIdentifier}
        @rtype: sequence of C{JobIdentifier}
        @returns: jobs owned by the user
        '''
        self.logger.debug("=> get_jobs")
        if not job_ids:
            request = '''SELECT id,
                          name,
                          command,
                          submission_date
                    FROM jobs
                    WHERE user_id=? and ( workflow_id ISNULL or workflow_id=-1 )'''
            argument = [user_id]
        else:
            request = '''SELECT id,
                          name,
                          command,
                          submission_date
                  FROM jobs WHERE id IN (? '''
            for i in range(1, len(job_ids)):
                request = request + ",? "
            request = request + ")"
            argument = job_ids

        with self._lock:
            connection = self._connect()
            cursor = connection.cursor()
            result = {}
            try:
                for row in cursor.execute(request, argument):
                    jid, name, command, submission_date = row
                    result[jid] = (self._string_conversion(name),
                                   self._string_conversion(command),
                                   self._str_to_date_conversion(submission_date))
            except Exception as e:
                cursor.close()
                connection.close()
                six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])

            cursor.close()
            connection.close()

            return result

    def nb_running_jobs(self, user_id, queue_name=None):
        '''
        Returns the number of job of the user with the status
        constants.RUNNING or constants.QUEUED_ACTIVE in the queue queue_name.

        Running and queued jobs are added since the use of it it to limit
        the number of jobs that can get running simultaneously.

        Parameters
        ----------
        user_id: UserIdentifier
        queue_name: str or None

        Returns
        -------
        number of jobs: int
        '''
        self.logger.debug("=> nb_running_jobs")
        return self.nb_jobs(user_id, queue_name,
                            [constants.RUNNING, constants.QUEUED_ACTIVE])

    def nb_queued_jobs(self, user_id, queue_name=None):
        '''
        Returns the number of job of the user with the status
        constants.QUEUED_ACTIVE in the queue queue_name.

        Parameters
        ----------
        user_id: UserIdentifier
        queue_name: str or None

        Returns
        -------
        number of jobs: int
        '''
        self.logger.debug("=> nb_queued_jobs")
        return self.nb_jobs(user_id, queue_name, [constants.QUEUED_ACTIVE])

    def nb_jobs(self, user_id, queue_name, status):
        '''
        Returns the number of job of the user with the given statuses
        in the queue queue_name.

        Parameters
        ----------
        user_id: UserIdentifier
        queue_name: str or None
        status: str among constants.JOB_STATUS, or list

        Returns
        -------
        number of jobs: int
        '''
        if not isinstance(status, list) and not isinstance(status, tuple):
            status = [status]
        with self._lock:
            connection = self._connect()
            cursor = connection.cursor()
            try:
                if queue_name != None:
                    count = six.next(cursor.execute(
                        "SELECT count(*) FROM jobs WHERE "
                        "user_id=? and ( status=?"
                        + " or status=?" * len(status) + ") "
                        "and queue=?",
                        [user_id,]
                        + status
                        + [constants.UNDETERMINED,
                           queue_name]))[0]
                else:
                    count = six.next(cursor.execute(
                        "SELECT count(*) FROM jobs WHERE "
                        "user_id=? and ( status=?"
                        + " or status=?" * len(status) + ") "
                        "and queue ISNULL",
                        [user_id,]
                        + status
                        + [constants.UNDETERMINED]))[0]
            except Exception as e:
                cursor.close()
                connection.close()
                six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])

            cursor.close()
            connection.close()
            return count

    def jobs_to_delete_and_kill(self, user_id):
        '''
        Returns the id of the job with the status constants.DELETE_PENDING

        @type user_id: C{UserIdentifier}
        @rtype: sequence of C{JobIdentifier}
        @returns: job with status constants.DELETE_PENDING
        '''
        self.logger.debug("=> jobs_to_delete_and_kill")
        with self._lock:
            connection = self._connect()
            cursor = connection.cursor()
            job_to_delete_ids = []
            job_to_kill_ids = []
            try:
                for row in cursor.execute("SELECT id FROM jobs "
                                          "WHERE user_id=? AND status=?",
                                          [user_id, constants.DELETE_PENDING]):
                    jid = row[0]
                    job_to_delete_ids.append(jid)
                for row in cursor.execute("SELECT id FROM jobs "
                                          "WHERE user_id=? AND status=?",
                                          [user_id, constants.KILL_PENDING]):
                    jid = row[0]
                    job_to_kill_ids.append(jid)
            except Exception as e:
                cursor.close()
                connection.close()
                six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])

            cursor.close()
            connection.close()
            return (job_to_delete_ids, job_to_kill_ids)

    # TRANSFERS
    def get_transfers(self, user_id, transfer_ids=None):
        '''
        Returns the transfers owned by the user or
        specified in the sequence transfer_ids

        @type user_id: C{UserIdentifier}
        @rtype: sequence of engine file path
        @returns: engine file path associated with a transfer owned by the user
        '''
        self.logger.debug("=> get_transfers")
        if not transfer_ids:
            request = '''SELECT id,
                          engine_file_path,
                          client_file_path,
                          expiration_date,
                          client_paths
                    FROM transfers
                    WHERE user_id=? and (workflow_id ISNULL or workflow_id=-1 )'''
            argument = [user_id]
        else:
            request = '''SELECT id,
                          engine_file_path,
                          client_file_path,
                          expiration_date,
                          client_paths
                  FROM transfers WHERE id IN (? '''
            for i in range(1, len(transfer_ids)):
                request = request + ",? "
            request = request + ")"
            argument = transfer_ids

        with self._lock:
            connection = self._connect()
            cursor = connection.cursor()
            result = {}
            try:
                for row in cursor.execute(request, argument):
                    transfer_id, engine_file, client_file_path, \
                        expiration_date, client_paths = row
                    engine_file = self._string_conversion(engine_file)
                    if client_paths:
                        client_paths = self._string_conversion(
                            client_paths).split(file_separator)
                    else:
                        client_paths = None
                    result[transfer_id] = (
                        engine_file,
                        self._string_conversion(client_file_path),
                        self._str_to_date_conversion(
                            expiration_date),
                        client_paths)
            except Exception as e:
                cursor.close()
                connection.close()
                six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])
            cursor.close()
            connection.close()
        return result

    def get_temporaries(self, user_id, temp_ids=None):
        '''
        Returns the temporary paths owned by the user or
        specified in the sequence temp_ids

        @type user_id: C{UserIdentifier}
        @rtype: sequence of temporary path id
        @returns: engine temporary path ids associated with a temporary path owned by the user
        '''
        self.logger.debug("=> get_temporaries")
        if not temp_ids:
            request = '''SELECT temp_path_id,
                          engine_file_path,
                          expiration_date,
                    FROM temporary_paths
                    WHERE user_id=? and (workflow_id ISNULL or workflow_id=-1 )'''
            argument = [user_id]
        else:
            request = '''SELECT temp_path_id,
                          engine_file_path,
                          expiration_date,
                  FROM temporary_paths WHERE temp_path_id IN (? '''
            for i in range(1, len(temp_ids)):
                request = request + ",? "
            request = request + ")"
            argument = temp_ids

        with self._lock:
            connection = self._connect()
            cursor = connection.cursor()
            result = {}
            try:
                for row in cursor.execute(request, argument):
                    temp_path_id, engine_file, expiration_date = row
                    if engine_file:
                        engine_file = self._string_conversion(engine_file)
                    result[temp_path_id] = (
                        engine_file,
                        self._str_to_date_conversion(expiration_date))
            except Exception as e:
                cursor.close()
                connection.close()
                six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])
            cursor.close()
            connection.close()
        return result

    # WORKFLOWS
    def _check_workflow(self, connection, cursor, wf_id, user_id):
        try:
            sel = cursor.execute(
                '''SELECT id
                FROM workflows
                WHERE id=? and
                      user_id=? LIMIT 1''',
                [wf_id, user_id])
        except Exception as e:
            cursor.close()
            connection.close()
            six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])

        try:
            six.next(sel)
        except StopIteration:
            raise UnknownObjectError("The workflow id " + repr(wf_id)
                                     + " is not valid or does not belong to "
                                     "user " + repr(user_id))

    def is_valid_workflow(self, wf_id, user_id):
        self.logger.debug("=> is_valid_workflow")
        with self._lock:
            connection = self._connect()
            cursor = connection.cursor()
            last_status_update = None
            try:
                sel = cursor.execute(
                    '''SELECT
                    last_status_update
                    FROM workflows
                    WHERE id=?''',
                    [wf_id])
            except Exception as e:
                cursor.close()
                connection.close()
                six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])

            try:
                last_status_update = six.next(sel)[0]
                valid = True
            except StopIteration:
                valid = False

            cursor.close()
            connection.close()

            last_status_update = self._str_to_date_conversion(
                last_status_update)
        return (valid, last_status_update)

    def get_workflows(self, user_id, workflow_ids=None):
        '''
        Returns information about the workflows owned by the user or
        specified in the sequence workflow_ids

        @type user_id: C{UserIdentifier}
        @rtype: sequence of workflows id
        '''
        self.logger.debug("=> get_workflows")
        if not workflow_ids:
            request = "SELECT id, name, expiration_date FROM workflows WHERE user_id=?"
            argument = [user_id]
        else:
            request = '''SELECT id, name, expiration_date
                   FROM workflows WHERE id IN (? '''
            for i in range(1, len(workflow_ids)):
                request = request + ",? "
            request = request + ")"
            argument = workflow_ids

        with self._lock:
            self.logger.debug("=> get_workflows, within lock")
            connection = self._connect()
            cursor = connection.cursor()
            result = {}

            try:
                for row in cursor.execute(request, argument):
                    wf_id, name, expiration_date = row
                    result[wf_id] = (self._string_conversion(name),
                                     self._str_to_date_conversion(expiration_date))
            except Exception as e:
                self.logger.exception("=> get_workflows, an exception occurred!")
                cursor.close()
                connection.close()
                six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])
            cursor.close()
            connection.close()
        return result

    def workflows_to_delete_and_kill(self, user_id):
        '''
        Returns the id of the workfows with the status constants.DELETE_PENDING

        Parameters
        ----------
        user_id: UserIdentifier

        Returns
        -------
        workflows: sequence of int
            workflows with status constants.DELETE_PENDING
        '''
        self.logger.debug("=> workflows_to_delete_and_kill")
        with self._lock:
            connection = self._connect()
            cursor = connection.cursor()
            wf_to_delete_ids = []
            wf_to_kill_ids = []
            try:
                for row in cursor.execute("SELECT id FROM workflows "
                                          "WHERE user_id=? AND status=?",
                                          [user_id, constants.DELETE_PENDING]):
                    wf_id = row[0]
                    wf_to_delete_ids.append(wf_id)
                for row in cursor.execute("SELECT id FROM workflows "
                                          "WHERE user_id=? AND status=?",
                                          [user_id, constants.KILL_PENDING]):
                    wf_id = row[0]
                    wf_to_kill_ids.append(wf_id)
            except Exception as e:
                cursor.close()
                connection.close()
                six.reraise(DatabaseError, DatabaseError(e), sys.exc_info()[2])

            cursor.close()
            connection.close()
            return (wf_to_delete_ids, wf_to_kill_ids)


    #
