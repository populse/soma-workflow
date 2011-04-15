'''
@author: Soizic Laguitton
@organization: U{IFR 49<http://www.ifr49.org>}
@license: U{CeCILL version 2<http://www.cecill.info/licences/Licence_CeCILL_V2-en.html>}
'''

import os
import hashlib
import stat
import operator
import shutil
import time

class RemoteFileController(object):

  def create_file(self, path):
    f = open(path, 'wb')
    f.close()

  def write(self, path, data):
    f = open(path, 'ab')
    f.write(data)
    fs = f.tell()
    f.close()
    return fs

  def read(self, path, location, buffer_size):
    f = open(path, 'rb')
    f.seek(location)
    data = f.read(buffer_size)
    f.close()
    return data
    

  def get_file_size(self, path):
    if os.path.isfile(path):
      size = os.path.getsize(path)
    else:
      size = 0
    return size

  def is_file(self, path):
    return os.path.isfile(path)

  def is_dir(self, path):
    return os.path.isdir(path)

  def get_dir_size(self, path):
    return Transfer.get_dir_size(path)

  def get_md5_hash(self, path):
    md5_hash = hashlib.md5(open(path, 'rb').read()).hexdigest()
    return md5_hash


  def top_down_dir_list(self, path):
    return Transfer.top_down_dir_list(path)

  def create_dir_structure(self, path, top_down_relalive_path):
    return Transfer.create_dir_structure(path, top_down_relalive_path)




class TransferMonitoring(object):
  
  remote_file_controller = None

  def __init__(self, remote_file_controller):
    self.remote_file_controller = remote_file_controller

    
  def transfer_to_remote_progression(self, path, remote_path):
    '''
    returns a tuple (data size, size of data transfered)
    '''
    #print ">>progress " + repr(path) + " to " + repr(remote_path)
    if os.path.isfile(path):
      r_size = self.remote_file_controller.get_file_size(remote_path)
      size = os.path.getsize(path)
      
    if os.path.isdir(path):
      r_size = self.remote_file_controller.get_dir_size(remote_path)
      size = self.get_dir_size(path)

    #print "<<progress " + repr(r_size) + " " + repr(size)
    return (size, r_size)


  def transfer_from_remote_progression(self, remote_path, path):
    '''
    returns a tuple (data size, size of data transfered)
    '''
    #print ">>progress " + repr(remote_path) + " to " + repr(path)
    if self.remote_file_controller.is_file(remote_path):
      r_size = self.remote_file_controller.get_file_size(remote_path)
      if os.path.isfile(path):
        size = os.path.getsize(path)
      else:
        size = 0

    if self.remote_file_controller.is_dir(remote_path):
      r_size = self.remote_file_controller.get_dir_size(remote_path)
      if os.path.isdir(path):
        size = self.get_dir_size(path)
      else:
        size = 0
    
    #print "<<progress " + repr(r_size) + " " + repr(size)
    return (r_size, size)


  def get_dir_size(self, path):
    return Transfer.get_dir_size(path)
    



class Transfer(object):

  remote_file_controller = None

  def __init__(self, remote_file_controller):
    self.remote_file_controller = remote_file_controller

  def transfer_to_remote(self, path, remote_path, overwrite=False):
    '''
    Transfer a file or a directory to a remote location.

    * path *string*
      Path of the local file or directory

    * remote_path *string*
      Path on the remote file system.

    * overwrite *boolean*
      In case the remote file already exists, the file will be erased 
      if overwrite is set to True. Otherwise, the existing file is 
      considered to be the result of a previous interrupted transfer. 
    '''
    pass

  def transfer_from_remote(self, remote_path, path, overwrite=False):
    '''
    Transfer a file or a directory from a remote location.

    * path *string*
      Path of the local file or directory

    * remote_path *string*
      Path on the remote file system.

    * overwrite *boolean*
      In case the remote file already exists, the file will be erased 
      if overwrite is set to True. Otherwise, the existing file is 
      considered to be the result of a previous interrupted transfer. 
    '''

    pass


  @staticmethod
  def get_dir_size(path):
    if not os.path.isdir(path):
      return 0
    size = 0
    for (directory, dirs, files) in os.walk(path):
      for file in files:
        path = os.path.join(directory, file)
        file_size = os.path.getsize(path)
        size = size + file_size
        #print "size: %0.1f MB cumul: %0.1f MB" %(file_size/(1024*1024.0), size/(1024*1024.0))
    return size

  @staticmethod
  def top_down_dir_list(path):
    abs_path = os.path.abspath(path)
    dir_list = []
    file_path_dict = {}
    for root, dirs, files in os.walk(abs_path):
      r_root = root[len(abs_path)+1:]
      if r_root:
        dir_list.append(r_root)
        file_list = []
        for name in files:
          file_list.append(name)
        file_path_dict[r_root] = file_list
    return (dir_list, file_path_dict)

  @staticmethod
  def create_dir_structure(path, top_down_relalive_path):
    if not os.path.isdir(path):
      os.mkdir(path)
    for dir_path in top_down_relalive_path:
      abs_path = os.path.join(path, dir_path)
      if not os.path.isdir(abs_path):
        os.mkdir(abs_path)



class TransferSCP(Transfer):

  username = None

  hostname = None

  def __init__(self, remote_file_controller, username, hostname):
    super(TransferSCP, self).__init__(remote_file_controller)
    self.username = username
    self.hostname = hostname


  def transfer_to_remote(self, path, remote_path, overwrite=False):
    if os.path.isfile(path):
      scp_cmd = 'scp -C %s "%s@%s:%s"' %(path, 
                                         user, host, remote_path)
      print scp_cmd
      os.system(scp_cmd)

    if os.path.isdir(path):
      scp_cmd = 'scp -C -r %s "%s@%s:%s"' %(path, user, host, remote_path)
      print scp_cmd
      os.system(scp_cmd)
      

  def transfer_from_remote(self, remote_path, path, overwrite=False):
    if self.remote_file_controller.is_file(remote_path):
      scp_cmd = 'scp -C "%s@%s:%s" %s ' %(user, host, remote_path, path)
      print scp_cmd
      os.system(scp_cmd)
      
    if self.remote_file_controller.is_dir(remote_path):
      scp_cmd = 'scp -C -r "%s@%s:%s" %s ' %(user, host, remote_path, path)
      print scp_cmd
      os.system(scp_cmd)
      
  
class TransferLocal(Transfer):
   
  def __init__(self, remote_file_controller):
    super(TransferLocal, self).__init__(remote_file_controller)

  def transfer_to_remote(self, path, remote_path, overwrite=False):
    #print "copy " + repr(path) + " to " + repr(remote_path)
    #time.sleep(4)
    if os.path.isfile(path):
      shutil.copy(path, remote_path)

    if os.path.isdir(path):
      if os.path.isdir(remote_path):
        shutil.rmtree(remote_path)
      shutil.copytree(path, remote_path)
    #time.sleep(4)
      

  def transfer_from_remote(self, remote_path, path, overwrite=False):
    #print "copy " + repr(remote_path) + " to " + repr(path)
    #time.sleep(4)
    if os.path.isfile(remote_path):
      shutil.copy(remote_path, path)
      
    if os.path.isdir(remote_path):
      if os.path.isdir(remote_path):
        shutil.rmtree(path)
      shutil.copytree(remote_path, path)
    #time.sleep(4)


#class TransferParamikoSCP(object):

  #username = None

  #hostname = None

  #password = None

  #def __init__(self, username, hostname, password):
    #import paramiko
    #super(TransferParamiko, self).__init__()

    #self.username = username
    #self.hostname = hostname
    #self.password = password

  #def transfer_to_remote(self, path, remote_path, overwrite=False):
    ##TBI
    #pass

  
  #def transfer_from_remote(self, remote_path, path, overwrite=False):
    ##TBI
    #pass


#class TransferParamiko(object):

  #username = None

  #hostname = None

  #password = None

  #def __init__(self, username, hostname, password):
    #import paramiko
    #super(TransferParamiko, self).__init__()

    #self.login = login
    #self.host = host
    #self.password = password


  #def transfer_to_remote(self, path, remote_path, overwrite=False):
    ##TBI
    #pass


  #def transfer_from_remote(self, remote_path, path, overwrite=False):
    ##TBI
    #pass


class TransferPyro(Transfer):

  def __init__(self, remote_file_controller):
    super(TransferPyro, self).__init__(remote_file_controller)

  def transfer_to_remote(self, 
                         path, 
                         remote_path, 
                         overwrite=False, 
                         buffer_size = 512**2):
    '''
    return Transfered_with_success
    '''
    #print "Pyro copy " + repr(path) + " to " + repr(remote_path)
    if os.path.isfile(path):
      # TBI in case the file were already transfered
      transmitted = 0

      f = open(path, 'rb')
      self.remote_file_controller.create_file(remote_path)
      file_size = os.path.getsize(path)
      if transmitted:
        f.seek(transmitted)
      r_file_size = transmitted
      while r_file_size < file_size:
        r_file_size = self.remote_file_controller.write(remote_path,   
                                                        f.read(buffer_size))
      f.close()
      
      if r_file_size != file_size:
        pass
        #TBI error

      md5_hash = hashlib.md5(open(path, 'rb').read()).hexdigest()
      if md5_hash != self.remote_file_controller.get_md5_hash(remote_path):
        #TBI error
        pass


    elif os.path.isdir(path):
      (dir_list, file_path_dict) = self.top_down_dir_list(path)
      self.remote_file_controller.create_dir_structure(remote_path,
                                                       dir_list)
      for relative_dir_path, file_list in file_path_dict.iteritems():
        dir_path = os.path.join(path, relative_dir_path)
        r_dir_path = os.path.join(remote_path, relative_dir_path)
        for file_name in file_list: 
          remote_file_path = os.path.join(r_dir_path, file_name)
          file_path = os.path.join(dir_path, file_name)
          self.transfer_to_remote(file_path, 
                                  remote_file_path,
                                  overwrite,
                                  buffer_size)
 
    

  def transfer_from_remote(self, 
                           remote_path, 
                           path,
                           overwrite=False, 
                           buffer_size = 512**2):

   #print "Pyro copy " + repr(remote_path) + " to " + repr(path)
   if self.remote_file_controller.is_file(remote_path):
      # TBI in case the file were already transfered
      transmitted = 0

      if transmitted:
        f = open(path, 'ab')
      else:
        f = open(path, 'wb')

      remote_file_size = self.remote_file_controller.get_file_size(remote_path)

      transmitted = os.path.getsize(path)
      data = self.remote_file_controller.read(remote_path,
                                              transmitted,
                                              buffer_size)
      f.write(data)
      file_size = f.tell()
      while data and file_size < remote_file_size:
        data = self.remote_file_controller.read(remote_path,
                                                file_size,
                                                buffer_size)
        f.write(data)
        file_size = f.tell()
      
      f.close()
      

      if file_size != remote_file_size:
        pass
        #TBI error

      md5_hash = hashlib.md5(open(path, 'rb').read()).hexdigest()
      if md5_hash != self.remote_file_controller.get_md5_hash(remote_path):
        #TBI error
        pass


   elif self.remote_file_controller.is_dir(remote_path):
      (dir_list, file_path_dict) = self.remote_file_controller.top_down_dir_list(remote_path)
      self.create_dir_structure(path,
                                dir_list)
      for relative_dir_path, file_list in file_path_dict.iteritems():
        dir_path = os.path.join(path, relative_dir_path)
        r_dir_path = os.path.join(remote_path, relative_dir_path)
        for file_name in file_list: 
          file_path = os.path.join(dir_path, file_name)
          r_file_path = os.path.join(r_dir_path, file_name)
          self.transfer_from_remote(r_file_path,
                                    file_path, 
                                    overwrite,
                                    buffer_size)
    

  def top_down_dir_list(self, path):
    return Transfer.top_down_dir_list(path)

  def create_dir_structure(self, path, top_down_relalive_path):
    return Transfer.create_dir_structure(path, top_down_relalive_path)

