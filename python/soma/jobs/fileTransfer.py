'''
@author: Yann Cointepas
@author: Soizic Laguitton
@organization: U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''

__docformat__ = "epytext en"





class FileTransfer( object ):

  def __init__( self, jobScheduler):
    '''
    @type  jobScheduler: L{JobScheduler} or proxy of L{JobScheduler} 
    '''
    self.jobScheduler = jobScheduler

  def transferInputFile(self, remote_input_file, disposal_timeout):
    '''
    An unique local path is generated and associated with the remote path. 
    Each remote files is copied to its associated local location.
    When the disposal timout will be past, and no exisiting job will 
    declare using the file as input, the files will be disposed. 
    
    @type  remote_input_file: string 
    @param remote_input_file: remote path of input file
    @type  disposalTimeout: int
    @param disposalTimeout: Number of hours before each local file is considered 
    to have been forgotten by the user. Passed that delay, and if no existing job 
    declares using the file as input, the local file and information 
    related to the transfer are disposed. 
    Default delay is 168 hours (7 days).
    @rtype: string 
    @return: local file path where the file were copied 
    '''
    pass 

  def transferOutputFile(self, local_file):
    '''
    Copy the local file to the associated remote file path. 
    The local file path must belong to the user's transfered files (ie belong to 
    the sequence returned by the L{getTransfers} method). 
    
    @type  local_file: string or sequence of string
    @param local_file: local file path(s) 
    '''
    pass
  
  
import shutil
  
class LocalFileTransfer(FileTransfer):
  
  
    def transferInputFile(self, remote_input_file, disposal_timeout):
     
      local_input_file_path = self.jobScheduler.registerTransfer(remote_input_file, disposal_timeout)
      
      shutil.copy(remote_input_file,local_input_file_path)
      
      return local_input_file_path
    
    
    
    def transferOutputFile(self, local_file):
    
      local_file_path, remote_file_path, expiration_date = self.jobScheduler.getTransferInformation(local_file)
      
      shutil.copy(local_file_path,remote_file_path)
    
    
    

class RemoteFileTransfer(FileTransfer):
  
  
    def transferInputFile(self, remote_input_file, disposal_timeout):
      
      local_input_file_path = self.jobScheduler.registerTransfer(remote_input_file, disposal_timeout)
      
      infile = open(remote_input_file)
      line = readline(infile)
      while line:
          self.jobScheduler.writeLine(line, local_infile_path)
          line = readline(infile)
  
      return local_input_file_path
    
    
    def transferOutputFile(self, local_file):
    
      local_file_path, remote_file_path, expiration_date = self.jobScheduler.getTransferInformation(local_file)
      
      outfile = open(remote_file_path, "w")
      line = self.jobScheduler.readline(local_file_path)
      while line:
          outfile.write(line)
          line = jobSchedulerProxy.readline(local_file_path)



import pexpect

class SCPFileTransfer(FileTransfer):
  
    # WIP !!
  
    def __init__( self, jobScheduler, server_address, login, password):
      FileTransfer.__init__(self, jobScheduler)
      self.__server_address = server_address
      self.__login = login
      self.__password = password # !!! security pb ???
  
    def transferInputFile(self, remote_input_file, disposal_timeout):
      
      local_input_file_path = self.jobScheduler.registerTransfer(remote_input_file, disposal_timeout)
      
      command = "scp %s %s@%s:%s" %(remote_input_file,
                                    self.__login,
                                    self.__server_address,
                                    local_input_file_path)
      child = pexpect.spawn(command) 
      child.expect('.ssword:*')
      child.sendline(self.__password)
      # TBI expect end of transfer child.expect(????)
      return local_input_file_path
    
    
    def transferOutputFile(self, local_file):
      
      local_file_path, remote_file_path, expiration_date = self.jobScheduler.getTransferInformation(local_file)
      
      command = "scp %s@%s:%s %s" %(self.__login,
                                    self.__server_address,
                                    local_file_path,
                                    remote_file_path)
      child = pexpect.spawn(command) 
      child.expect('.ssword:*')
      child.sendline(self.__password)
      # TBI expect end of transfer child.expect(????)