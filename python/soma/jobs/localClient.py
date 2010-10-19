'''
@author: Soizic Laguitton
@organization: U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''

__docformat__ = "epytext en"

from tempfile import mkstemp

i = LocalJob
class LocalJob( self ):
  def __init__( self, directory, localjob_id ):
    self.directory = directory
    self.id = localjob_id
  
  @staticmethod
  def create( directory, command, stdin, stdout, stderr, description, working_directory ):
    fd, name = mkstemp( dir=directory )
    os.close( fd )
    os.open( name, 'wb' )
    self.directory = directory
    self.command = command
    self.stdin = stdin
    self.stdout = stdout
    self.stderr = stderr
    self.description = description
    self.working_directory = working_directory
    self.pid = None
    self.return_file = None
  
  def start( self ):
    
    return_file = ??
    args = [ 'python', '-c', 'import os; print >> open( "%(return_file)s", "a" ), os.spawnv( os.P_WAIT, "%(program)s", %(args)s )' % { 'return_file': return_file, 'program': self.command[0], 'parameters': repr( command ) } ]
