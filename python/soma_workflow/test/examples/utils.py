# -*- coding: utf-8 -*-
"""
Created on Thu Oct 17 14:17:27 2013

@author: laure.hugo@cea.fr
"""
#-----------------------------------------------------------------------------
# Imports
#-----------------------------------------------------------------------------

import sys
import os
from contextlib import contextmanager


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


if __name__ == "__main__":
    import doctest
    doctest.testmod()
