# -*- coding: utf-8 -*-
"""
Created on Fri Oct 25 17:09:13 2013

@author: laure.hugo@cea.fr
@author: Soizic Laguitton
@organization: U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}

"""
#-----------------------------------------------------------------------------
# Imports
#-----------------------------------------------------------------------------

from __future__ import absolute_import
import sys
import os
from contextlib import contextmanager
import stat
import getpass


#-----------------------------------------------------------------------------
# Functions
#-----------------------------------------------------------------------------

def get_user_id(resource_id, config, interactive=True):
    '''Ask for user id to connect to a resource

    If the resource is remote, ask for a login and a password
        Return the tuple (login, password)
    If the resource is local, return the tuple (None, None)
    '''
    login = None
    password = None
    if config.get_mode() == 'remote':
        sys.stdout.write("Remote connection to %s\n" % resource_id)
        default_login = config.get_login()
        if not default_login:
            default_login = getpass.getuser()
        if interactive:
            sys.stdout.write("Login (default: %s): " % default_login)
            sys.stdout.flush()
            login = sys.stdin.readline()
            login = login.rstrip()
            if not login:
                login = default_login
            sys.stdout.write("Password (blank=ssh key): ")
            sys.stdout.flush()
            password = getpass.getpass("")
            password = password.rstrip()
        else:
            login = default_login
            password = ''
        sys.stdout.write("Login => " + repr(login) + "\n")
    else:
        sys.stdout.write("Local connection to %s\n" % resource_id)
    return (login, password)


@contextmanager
def suppress_stdout(debug=False):
    '''Suppress momentarily the stdout

    Arguments
    ---------
    * debug : boolean
        If False, suppress the stdout of the chosen piece of code
        If True, do nothing, allowing the user to debug

    Examples
    --------
    >>> with suppress_stdout(debug=False):
    ...     print("hello world")

    >>> with suppress_stdout(debug=True):
    ...     print("hello world")
    hello world
    '''
    if debug:
        yield
    else:
        with open(os.devnull, "w") as devnull:
            old_stdout = sys.stdout
            sys.stdout = devnull
            try:
                yield
            finally:
                sys.stdout = old_stdout


def list_contents(path_seq, dir_contents):
    for path in path_seq:
        s = os.stat(path)
        if stat.S_ISDIR(s.st_mode) and os.path.basename(path) != ".svn":
            dir_contents.append(
                "directory " + repr(os.path.basename(path)) + "\n")
            full_path_list = []
            for element in os.listdir(path):
                full_path_list.append(os.path.join(path, element))
            dir_contents = list_contents(full_path_list, dir_contents)
        else:
            if os.path.basename(path) != ".svn":
                dir_contents.append(
                    "file " + repr(os.path.basename(path)) + "\n")
    return dir_contents


def identical_files(filepath1, filepath2):
    with open(filepath1) as file1:
        with open(filepath2) as file2:
            lineNb = 1
            line1 = file1.readline().rstrip('\n')
            line2 = file2.readline().rstrip('\n')
            identical = (line1 == line2)

            while identical and line1:
                line1 = file1.readline().rstrip('\n')
                line2 = file2.readline().rstrip('\n')
                lineNb = lineNb + 1
                identical = (line1 == line2)

            if identical:
                identical = (line1 == line2)
    if not identical:
        return (
            (False, "Files are different, line %d: \n file1: %s\n"
             "%s\nfile2: %s\n%s" %
             (lineNb, filepath1, line1, filepath2, line2))
        )
    else:
        return (True, None)


def check_files(files, files_models, tolerance=0):
    for (index, file) in enumerate(files):
        t = tolerance
        (identical, msg) = identical_files(file, files_models[index])
        if not identical:
            if t <= 0:
                return (identical, msg)
            else:
                t = t - 1
                sys.stdout.write("\n check_files: " + msg + "\n")
    return (True, None)


if __name__ == "__main__":
    import doctest
    doctest.testmod()
