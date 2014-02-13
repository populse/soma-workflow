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

import sys
import os
from contextlib import contextmanager
import stat


#-----------------------------------------------------------------------------
# Functions
#-----------------------------------------------------------------------------

def get_user_id(resource_id, config):
    '''Ask for user id to connect to a resource

    If the resource is remote, ask for a login and a password
        Return the tuple (login, password)
    If the resource is local, return the tuple (None, None)
    '''
    login = None
    password = None
    if config.get_mode() == 'remote':
        sys.stdout.write("Remote connection to %s\n" % resource_id)
        sys.stdout.write("login:")
        login = sys.stdin.readline()
        login = login.rstrip()
        sys.stdout.write("password:")
        password = sys.stdin.readline()
        password = password.rstrip()
        #password = getpass.getpass()
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
    ...     print "hello world"

    >>> with suppress_stdout(debug=True):
    ...     print "hello world"
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
    file1 = open(filepath1)
    file2 = open(filepath2)
    lineNb = 1
    line1 = file1.readline()
    line2 = file2.readline()
    identical = (line1 == line2)

    while identical and line1:
        line1 = file1.readline()
        line2 = file2.readline()
        lineNb = lineNb + 1
        identical = (line1 == line2)

    if identical:
        identical = (line1 == line2)
    if not identical:
        return (
            (False, "%s and %s are different. line %d: \n file1: %s file2:%s" %
             (filepath1, filepath2, lineNb, line1, line2))
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
