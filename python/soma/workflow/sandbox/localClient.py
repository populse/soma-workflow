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


def find_executable( name, path=None ):
  '''
  Return the absolute file name for an executable by looking for a name
  in a given path. If path is not given or None, os.environ[ 'PATH' ] 
  is used. If the executable cannot be found, None is returned
  '''
  access_mode = os.F_OK | os.X_OK
  if os.path.isabs( name ):
    if os.access( name, access_mode ) and not os.path.isdir( name ):
      return name
  else:
    if path is None:
      path = os.environ.get( 'PATH', '' )
    path = path.split( os.pathsep )
    for p in path:
      fp = os.path.join( p, name )
      if os.access( fp, access_mode ) and not os.path.isdir( fp ):
        return fp
  return None



def spawn_daemon_unix( executable, command, stdin=None, stdout=None, stderr=None, cwd=None, env=None ):
  '''
  Spawn a completely detached subprocess.
  '''
  abs_executable = find_executable( executable )
  if executable is None:
    raise ValueError( 'Cannot find executable: %s' % executable )
  
  # fork the first time (to make a non-session-leader child process)
  read_pid, write_pid = os.pipe()
  pid = os.fork()
  if pid != 0:
    # parent (calling) process is all done
    os.waitpid( pid, 0 )
    return int( os.read( read_pid, 50 ) )
  else:
    # detach from controlling terminal (to make child a session-leader)
    os.setsid()
    pid = os.fork()
    if pid != 0:
      # child process: Just write the pid of the grandchild process and exit
      os.write( write_pid, str( pid ) )
      os._exit(0)
    else:
      if cwd:
        os.chdir( cwd )
      
      # grandchild process now non-session-leader, detached from parent
      # grandchild process must now close all open files
      try:
        maxfd = os.sysconf( 'SC_OPEN_MAX' )
      except ( AttributeError, ValueError ):
        maxfd = 1024
    
      for fd in xrange( maxfd ):
        try:
          os.close(fd)
        except OSError: # ERROR, fd wasn't open to begin with (ignored)
          pass
    
      if stdin:
        fd = os.open( stdin, os.O_RDONLY )
      else:
        fd = os.open( '/dev/null', os.O_RDONLY )
      os.dup2( fd, 0 )
      os.close( fd )
      if stdout:
        fd = os.open( stdout, os.O_WRONLY | os.O_CREAT )
      else:
        fd = os.open( '/dev/null', os.O_WRONLY ) # Unix only !
      os.dup2( fd, 1 )
      os.close( fd )
      if stderr:
        if stderr == stdout:
          os.dup2( 1, 2 )
        else:
          fd = os.open( stderr, os.O_WRONLY | os.O_CREAT )
          os.dup2( fd, 2 )
          os.close( fd )
      else:
        fd = os.open( '/dev/null', os.O_WRONLY ) # Unix only !
        os.dup2( fd, 2 )
        os.close( fd )

      # and finally let's execute the executable for the daemon!
      print '!command!', command
      try:
        if env is None:
          os.execv( abs_executable, command )
        else:
          os.execve( abs_executable, command, env )
      except Exception, e:
        # oops, we're cut off from the world, let's just give up
        os._exit( 255 )

spawn_daemon = spawn_daemon_unix


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
    executable = find_executable( command[0] )
    if executable is None:
      raise ValueError( 'Cannot find executable: %s' % command[0] )
  
    args = [ sys.executable, '-u', '-c', 'import os; print >> open( "%(return_file)s", "a" ), os.spawnv( os.P_WAIT, "%(program)s", %(args)s )' % { 'return_file': return_file, 'program': executable, 'args': repr( command ) } ]
    status[ 'pid' ] = spawn_daemon( sys.executable, args, stdin=status[ 'stdin' ], stdout=status[ 'stdout' ], stderr=status[ 'stderr' ], cwd=status[ 'working_directory' ] )
    status[ 'status' ] = constants.RUNNING
    pickle.dump( status, open( status_file, 'wb' ) ) 



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
        os.kill( status[ 'pid' ], signal )
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
  job_id = jobs.create( ( '/tmp/toto', 'one', 'two', 'three' ) )
  jobs.start( job_id )
  print jobs.status( job_id )
  jobs.kill( job_id )
  print jobs.wait( job_id )
  jobs.dispose( job_id )

