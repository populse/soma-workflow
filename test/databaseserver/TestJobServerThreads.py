from soma.jobs.jobServer import JobServer
from datetime import date
from datetime import timedelta
from soma.jobs.jobDatabase import *
import Pyro.naming, Pyro.core
from soma.pyro import ThreadSafeProxy
from Pyro.errors import NamingError
import threading

expiration_date = date.today() + timedelta(days=7)
database_file = "/volatile/laguitton/job.db"
mode = "pyro_server"

if mode == "normal":
  tmp_file_dir_path = "/neurospin/tmp/Soizic/jobFiles/"
  js=JobServer(database_file, tmp_file_dir_path)

if mode == "pyro_server":
  Pyro.core.initClient()
  locator = Pyro.naming.NameServerLocator()
  ns = locator.getNS(host='is143016')
  
  try:
      URI=ns.resolve('JobServer')
      print 'URI:',URI
  except NamingError,x:
      print 'Couldn\'t find JobServer, nameserver says:',x
      raise SystemExit
  
  js= ThreadSafeProxy( Pyro.core.getProxyForURI( URI ) )
  
  
  
lock = threading.RLock()


def generateLocalFilePath(user_id, remote_file_path = None):
  lock.acquire()
  try:
    result = js.generateLocalFilePath(user_id, remote_file_path)
  finally:
    lock.release()
  return result
    
def addTransfer(local_file_path, remote_file_path, expiration_date, user_id):
  lock.acquire()
  try:
    result = js.addTransfer(local_file_path, remote_file_path, expiration_date, user_id)
  finally:
    lock.release()
  return result

def addJob(user_id,
          expiration_date,
          stdout_file,
          stderr_file,
          join_stderrout=False,
          stdin_file=None,
          name_description=None,
          drmaa_id=None,
          working_directory=None):
  lock.acquire()
  try:
    result = js.addJob(user_id,
              expiration_date,
              stdout_file,
              stderr_file,
              join_stderrout,
              stdin_file,
              name_description,
              drmaa_id,
              working_directory)
  finally:
    lock.release()
  return result
    
def registerInputs(job_id, local_input_files):
  lock.acquire()
  try:
    result = js.registerInputs(job_id, local_input_files)
  finally:
    lock.release()
  return result

def registerOutputs(job_id, local_output_files):
  lock.acquire()
  try:
    result = js.registerOutputs(job_id, local_output_files)
  finally:
    lock.release()
  return result 
    
def removeTransferASAP(local_file_path):
  lock.acquire()
  try:
    result = js.removeTransferASAP(local_file_path)
  finally:
    lock.release()
  return result
    
def deleteJob(job_id):
  lock.acquire()
  try:
    result = js.deleteJob(job_id)
  finally:
    lock.release()
  return result


def simpleTest():
  
  lock.acquire()
  try:
    user_id = js.registerUser("toto")
  finally:
    lock.release()
  
  #job1#
  #input files
  r_file0 = "/remote/file0"
  r_script1  = "/remote/script1"
  r_stdin1 = "/remote/stdin1"
  #output files
  r_file11 = "remote/file11"
  r_file12 = "remote/file12"
  
  l_file0 = generateLocalFilePath(user_id, r_file0)
  l_script1 = generateLocalFilePath(user_id, r_script1)
  l_stdin1 = generateLocalFilePath(user_id, r_stdin1)
  
  addTransfer(l_file0, r_file0, expiration_date, user_id)
  addTransfer(l_script1, r_script1, expiration_date, user_id)
  addTransfer(l_stdin1, r_stdin1, expiration_date, user_id)
  
  l_file11 = generateLocalFilePath(user_id, r_file11)
  l_file12 = generateLocalFilePath(user_id, r_file12)
  
  addTransfer(l_file11, r_file11, expiration_date, user_id)
  addTransfer(l_file12, r_file12, expiration_date, user_id)
  
  l_stdout1 = generateLocalFilePath(user_id)
  l_stderr1 = generateLocalFilePath(user_id)
  
  addTransfer(l_stdout1, None, expiration_date, user_id)
  addTransfer(l_stderr1, None, expiration_date, user_id)
  
  
  job1_id = addJob(user_id, expiration_date, l_stdout1, l_stderr1, l_stdin1, "job1")
  
  registerInputs(job1_id, [l_file0, l_script1, l_stdin1])
  registerOutputs(job1_id, [l_file11, l_file12])
  registerOutputs(job1_id, [l_stdout1, l_stderr1])
  
  
  #printTables(database_file)
  #print "####################JOB1######################"
  #raw_input()
  
  
  #job2 & 3#
  #job2 input files
  #=> l_file11
  #=> l_file0
  r_script2 = "remote/script2"
  r_stdin2 = "remote/stdin2"
  #output files
  r_file2 = "remote/file2"
  
  l_script2 = generateLocalFilePath(user_id, r_script2)
  l_stdin2 = generateLocalFilePath(user_id, r_stdin2)
  
  addTransfer(l_script2, r_script2, expiration_date, user_id)
  addTransfer(l_stdin2, r_stdin2, expiration_date, user_id)
  
  l_file2 = generateLocalFilePath(user_id, r_file2)
  
  addTransfer(l_file2, r_file2, expiration_date, user_id)
  
  l_stdout2 = generateLocalFilePath(user_id)
  l_stderr2 = generateLocalFilePath(user_id)
  
  addTransfer(l_stdout2, None, expiration_date, user_id)
  addTransfer(l_stderr2, None, expiration_date, user_id)
  
  
  job2_id = addJob(user_id, expiration_date, l_stdout2, l_stderr2, l_stdin2, "job2")
  
  registerInputs(job2_id, [l_file0, l_file11, l_script2, l_stdin2])
  registerOutputs(job2_id, [l_file2])
  registerOutputs(job2_id, [l_stdout2, l_stderr2])
  
  #printTables(database_file)
  #print "####################JOB2######################"
  #raw_input()
  
  
  
  #job3 input files
  #=> l_file12
  r_script3 = "remote/script3"
  r_stdin3 = "remote/stdin3"
  #job3 output files
  r_file3 = "remote/file3"
  
  
  l_script3 = generateLocalFilePath(user_id, r_script3)
  l_stdin3 = generateLocalFilePath(user_id, r_stdin3)
  
  addTransfer(l_script3, r_script3, expiration_date, user_id)
  addTransfer(l_stdin3, r_stdin3, expiration_date, user_id)
  
  l_file3 = generateLocalFilePath(user_id, r_file3)
  
  addTransfer(l_file3, r_file3, expiration_date, user_id)
  
  l_stdout3 = generateLocalFilePath(user_id)
  l_stderr3 = generateLocalFilePath(user_id)
  
  addTransfer(l_stdout3, None, expiration_date, user_id)
  addTransfer(l_stderr3, None, expiration_date, user_id)
  
  job3_id = addJob(user_id, expiration_date, l_stdout3, l_stderr3, l_stdin3, "job3")
  
  registerInputs(job3_id, [l_file12, l_script3, l_stdin3])
  registerOutputs(job3_id, [l_file3])
  registerOutputs(job3_id, [l_stdout3, l_stderr3])
  
  #printTables(database_file)
  #print "####################JOB3######################"
  #raw_input()
  
  removeTransferASAP(l_file0)
  removeTransferASAP(l_script1)
  removeTransferASAP(l_stdin1)
  removeTransferASAP(l_file11)
  removeTransferASAP(l_file12)
  
  #printTables(database_file)
  #print "###########DELETE TRANSFERS RELATED TO JOB1######################"
  #raw_input()
  
  deleteJob(job1_id)
  #printTables(database_file)
  #print "####################DELETE JOB1######################"
  #raw_input()
  
  
  removeTransferASAP(l_script2)
  removeTransferASAP(l_stdin2)
  removeTransferASAP(l_file2)
  
  removeTransferASAP(l_script3)
  removeTransferASAP(l_stdin3)
  removeTransferASAP(l_file3)
  
  #printTables(database_file)
  #print "###########DELETE TRANSFERS RELATED TO JOB2&3######################"
  #raw_input()
  
  deleteJob(job2_id)
  #printTables(database_file)
  #print "####################DELETE JOB2######################"
  #raw_input()
  
  
  deleteJob(job3_id)
  #printTables(database_file)
  #print "####################DELETE JOB3######################"
  #raw_input()
  
  
def simpleTestLoop(thread_name):
  for i in range(1, 10):
    print thread_name + " "+ repr(i) + ">>>>>"
    simpleTest()
    print "<<<<<" + thread_name + " " + repr(i) 
  

#printTables(database_file)
    
for it in range(1,15):
  thread_name = "JobServerTestThread" + repr(it)
  thread = threading.Thread(name= thread_name, target= simpleTestLoop, args=(thread_name,))
  thread.daemon = True
  thread.start()
  
simpleTestLoop("MainThread")


