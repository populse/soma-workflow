# -*- coding: utf-8 -*-
'''
@author: Soizic Laguitton
@organization: U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''

__docformat__ = "epytext en"

from tempfile import mkstemp
import sys, os, pickle, signal, errno
from subprocess import Popen, STDOUT
from time import sleep
from functools import partial
from soma.jobs import constants


class LocalJobs( object ):
  def __init__( self, directory ):
    self.directory = directory
  

  def create( self, command, stdin=None, stdout=None, stderr=None, description='', working_directory=None ):
    fd, name = mkstemp( dir=self.directory, prefix='job_' )
    os.close( fd )
    status = { 'command': command,
      'stdin': stdin,
      'stdout': stdout,
      'stderr': stderr,
      'description': description,
      'working_directory': working_directory,
      'return_file': None,
      'pid': None,
      'status': constants.NOT_SUBMITTED,
      'return_value': None,
    }
    pickle.dump( status, open( name, 'wb' ) )
    return name


  def start( self, job_id ):
    status_file = os.path.join( self.directory, job_id )
    status = pickle.load( open( status_file )  )
    fd, return_file = mkstemp( dir=self.directory, prefix='return_' )
    os.close( fd )
    status[ 'return_file' ] = return_file
    command = status[ 'command' ]
    
    args = [ sys.executable, '-u', '-c', 'import os; print >> open( "%(return_file)s", "a" ), os.spawnv( os.P_WAIT, "%(program)s", %(args)s )' % { 'return_file': return_file, 'program': command[0], 'args': repr( command ) } ]
    stdin = status[ 'stdin' ]
    if stdin:
      stdin = os.open( stdin, os.O_RDONLY )
    else:
      stdin = os.open( '/dev/null', os.O_RDONLY ) # Unix only !
    stdout = status[ 'stdout' ]
    if stdout:
      stdout = os.open( stdout, os.O_WRONLY | os.O_CREAT )
    else:
      stdout = os.open( '/dev/null', os.O_WRONLY ) # Unix only !
    stderr = status[ 'stderr' ]
    if stderr:
      if stderr == status[ 'stdout' ]:
        stderr = stdout
      else:
        stderr = os.open( stderr, os.O_WRONLY | os.O_CREAT )
    else:
      stderr = os.open( '/dev/null', os.O_WRONLY ) # Unix only !
    cpid = os.fork()
    if cpid:
      os.waitpid( cpid, 0 )
      status = pickle.load( open( status_file )  )
      pid = status[ 'pid' ]
    else:
      popen = Popen( args, stdin=stdin, stdout=stdout, stderr=stderr, cwd=status[ 'working_directory' ], close_fds=True ) # ! Unix only
    # status[ 'pid' ] = popen.pid
    status[ 'pid' ] = os.spawnv( os.P_NOWAITO, args[0], args ) # ! Unix only
    status[ 'status' ] = constants.RUNNING
    pickle.dump( status, open( status_file, 'wb' ) ) 
    if stdin:
      os.close( stdin )
    if stdout:
      os.close( stdout )
    if stderr:
      os.close( stderr )



  def dispose( self, job_id ):
    status_file = os.path.join( self.directory, job_id )
    if os.path.exists( status_file ):
      status = pickle.load( open( status_file )  )
      for name in ( 'stdin', 'stdout', 'stderr', 'return_file' ):
        file_name = status[ name ]
        if file_name and os.path.exists( file_name ):
          os.remove( file_name )
      os.remove( status_file )
  
  
  
  def kill( self, job_id, signal=signal.SIGTERM ):
    signal_sent = False
    status_file = os.path.join( self.directory, job_id )
    status = pickle.load( open( status_file )  )
    if status[ 'pid' ] is not None:
      try:
        kill( status[ 'pid' ], signal )
        signal_sent = True
      except OSError, e:
        if e.errno == errno.ESRCH: # ESRCH <=> 'No such process'
          status[ 'pid' ] = None
          pickle.dump( status, open( status_file, 'wb' ) ) 
        else:
          raise
    return signal_sent
  
  
  def wait( self, job_id, timeout=-1, callback=partial( sleep, 1 ) ):
    status = jobs.status( job_id )
    while status[ 0 ] == constants.RUNNING:
      if callback(): break
      status = jobs.status( job_id )
    return status


  def stop( self, job_id ):
    raise NotImplementedError()
  
    
  def restart( self, job_id ):
    raise NotImplementedError()
  
  
  @staticmethod
  def isRunning( pid ):
    # Check if process still exists
    try:
      os.getpgid( pid ) # Unix only !
    except OSError, e:
      if e.errno == errno.ESRCH: # ESRCH <=> 'No such process'
        return False
      else:
        raise
    return True
  
  
  def status( self, job_id ):
    status_file = os.path.join( self.directory, job_id )
    status = pickle.load( open( status_file )  )
    status_modified = False
    pid = status[ 'pid' ]
    if pid is not None:
      if not self.isRunning( pid ):
        pid = None
        status[ 'pid' ] = None
        status_modified = True
    s = status[ 'status' ]
    if pid:
      if s == constants.NOT_SUBMITTED:
        s = constants.RUNNING
        status[ 'status' ] = s
        status_modified = True
    else:
      if s == constants.RUNNING:
        status_modified = True
        s = constants.EXIT_UNDETERMINED
        f = status[ 'return_file' ]
        if os.path.exists( f ):
          l = open( f ).read()
          if l:
            return_value = int( l )
            if return_value < 0:
              status[ 'return_value' ] = return_value
              s = constants.FINISHED_TERM_SIG
            else:
              status[ 'return_value' ] = return_value
              s = constants.FINISHED_REGULARLY
        status[ 'status' ] = s
    if status_modified:
      pickle.dump( status, open( status_file, 'wb' ) ) 
    return ( s, pid, status[ 'return_value' ] )


if __name__ == '__main__':
  jobs = LocalJobs( '/tmp/jobs' )
  job_id = jobs.create( '/tmp/toto' )
  jobs.start( job_id )
  print jobs.status( job_id )
  print jobs.wait( job_id )

