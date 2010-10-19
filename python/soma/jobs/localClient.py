# -*- coding: utf-8 -*-
'''
@author: Soizic Laguitton
@organization: U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''

__docformat__ = "epytext en"

from tempfile import mkstemp
import os, pickle, signal, errno
from subprocess import Popen, STDOUT
from soma.jobs import constants


class LocalJobs( self ):
  def __init__( self, directory ):
    self.directory = directory
  

  def create( self, command, stdin, stdout, stderr, description, working_directory ):
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
      'status': constants.NOT_SUBMITED,
    }
    pickle.dump( status, open( name, 'wb' ) )
    return name


  def start( self, job_id ):
    status_file = os.path.join( self.directory, job_id )
    status = pickle.load( open( status_file )  )
    fd, return_file = mkstemp( dir=self.directory, prefix='return_' )
    os.close( fd )
    status[ 'return_file' ] = return_file
    args = [ sys.executable, '-c', 'import os; print >> open( "%(return_file)s", "a" ), os.spawnv( os.P_WAIT, "%(program)s", %(args)s )' % { 'return_file': return_file, 'program': self.command[0], 'parameters': repr( command ) } ]
    stdin = status[ 'stdin' ]
    if stdin:
      stdin = os.open( stdin, os.O_RDONLY )
    stdout = status[ 'stdout' ]
    if stdout:
      stdout = os.open( stdout, os.O_WRONLY | os.O_CREAT )
    stderr = status[ 'stderr' ]
    if stderr:
      if stderr == status[ 'stdout' ]:
        stderr = STDOUT
      else:
        stderr os.open( stderr, os.O_WRONLY | os.O_CREAT )
    popen = Popen( args, stdin=stdin, stdout=stdout, stderr=stderr, cwd=status[ 'working_directory' ] )
    status[ 'pid' ] = popen.pid
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
  
  
  def wait( self, job_id, timeout=-1 ):
    raise NotImplementedError()


  def stop( self, job_id ):
    raise NotImplementedError()
  
    
  def restart( self, job_id ):
    raise NotImplementedError()
  
  
  def status( self, job_id ):
    raise NotImplementedError()

