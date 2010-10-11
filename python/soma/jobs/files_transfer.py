# -*- coding: utf-8 -*-

import sys, os, hashlib, pickle, stat, operator

class AsynchronousFileTransfer( object ):
  def __init__( self, file_name ):
    self.file_name = os.path.abspath( file_name )
    self.transfer_file_name = self.file_name + '.soma_transfer'
  
  def initialize( self, file_size, md5_hash=None ):
    # Initialize file
    open( self.file_name, 'wb' )
    # Initialize transfer info file
    transfer_info = ( file_size, md5_hash )
    pickle.dump( transfer_info, open( self.transfer_file_name, 'wb' ) )
    
  
  def status( self ):
    transmited = os.stat( self.file_name ).st_size
    if os.path.exists( self.transfer_file_name ):
      file_size, md5_hash = pickle.load( open( self.transfer_file_name, 'rb' ) )
    else:
      file_size = transmited
    return transmited, file_size
  
  
  def send( self, data ):
    file = open( self.file_name, 'ab' )
    file_size, md5_hash = pickle.load( open( self.transfer_file_name, 'rb' ) )
    file.write( data )
    fs = file.tell()
    file.close()
    if fs > file_size:
      # Reset file
      open( self.file_name, 'wb' )
      raise ValueError( 'Transmitted data exceed expected file size: ' + repr( self.file_name ) )
    elif fs == file_size:
      if md5_hash is not None:
        if hashlib.md5( open( self.file_name, 'rb' ).read() ).hexdigest() != md5_hash:
          # Reset file
          open( self.file_name, 'wb' )
          raise ValueError( 'Transmission error: ' + repr( self.file_name ) )
        else:
          os.remove( self.transfer_file_name )
      else:
        os.remove( self.transfer_file_name )


class AsynchronousDirectoryTransfer( object ):
  def __init__( self, directory_name ):
    self.directory_name = os.path.abspath( directory_name )
    self.transfer_file_name = self.directory_name + '.soma_transfer'

  @staticmethod
  def directory_content( directory, md5_hash=False ):
    result = []
    stack = os.listdir( directory )
    while stack:
      item = stack.pop( 0 )
      if item in ( '.', '..' ): continue
      full_path = os.path.join( directory, item )
      s = os.stat( full_path )
      if stat.S_ISDIR( s.st_mode ):
        content = AsynchronousDirectoryTransfer.directory_content( full_path, md5_hash )
        result.append( ( item, content, None ) )
      else:
        if md5_hash:
          result.append( ( item, s.st_size, hashlib.md5( open( full_path, 'rb' ).read() ).hexdigest() ) )
        else:
          result.append( ( item, s.st_size, None ) )
    return result
  
  
  def initialize( self, content ):
    os.mkdir( self.directory_name )
    files_transfer = {}
    cumulated_file_size = self._initialize_subdirectory( files_transfer, '', content )
    pickle.dump( ( cumulated_file_size, files_transfer ), open( self.transfer_file_name, 'wb' ) )

  
  def _initialize_subdirectory( self, files_transfer, subdirectory, content ):
    cumulated_file_size = 0
    for item, description, md5_hash in content:
      relative_path = os.path.join( subdirectory, item )
      full_path = os.path.join( self.directory_name, relative_path )
      if isinstance( description, list ):
        os.mkdir( full_path )
        cumulated_file_size += self._initialize_subdirectory( files_transfer, relative_path, description )
      else:
        file_transfer = AsynchronousFileTransfer( full_path )
        file_transfer.initialize( file_size=description, md5_hash=md5_hash )
        files_transfer[ relative_path ] = file_transfer
        cumulated_file_size += description
    return cumulated_file_size
  
  
  def status( self ):
    cumulated_file_size, files_transfer = pickle.load( open( self.transfer_file_name, 'rb' ) )
    return ( cumulated_file_size, [ ( f, t.status() ) for f, t in files_transfer.iteritems() ] )
  
  
  def send( self, relative_file_name, data ):
    cumulated_file_size, files_transfer = pickle.load( open( self.transfer_file_name, 'rb' ) )
    files_transfer[ relative_file_name ].send( data )

def test_directory_transfer( source, dest, buffer_size=2**16, md5_hash=False ):
  if not isinstance( dest, AsynchronousDirectoryTransfer ):
    dest = AsynchronousDirectoryTransfer( dest )
    dest.initialize( AsynchronousDirectoryTransfer.directory_content( source, md5_hash=True) )
  
  count = 0
  cumulated_file_size, files_transfer_status = dest.status()
  cumulated_transmissions = reduce( operator.add, (i[1][0] for i in files_transfer_status) )
  while cumulated_transmissions < cumulated_file_size:
    for name, status in files_transfer_status:
      transmited, file_size = status
      if transmited < file_size:
        f = open( os.path.join( source, name ), 'rb' )
        f.seek( transmited )
        dest.send( name, f.read( buffer_size ) )
    cumulated_file_size, files_transfer_status = dest.status()
    cumulated_transmissions = reduce( operator.add, (i[1][0] for i in files_transfer_status) )
    count += 1
    print 'Pass', count, 'transfered', cumulated_transmissions, 'bytes on', cumulated_file_size
    sys.stdout.flush()

  print cumulated_file_size, reduce( operator.add, (i[1][0] for i in files_transfer_status) )
