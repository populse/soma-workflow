from sqlite3 import *

'''
Job database construction.
Tables:
  Users
    login or other userId

  Jobs
    jobId
    expiration date
    submission date
    user id
    is stdout requested
    is stderr requested
    stdout file local path
    stdin file local path
    name/description
    drmaa job id
  
  Transfer
    local file path
    remote file path
    transfer date
    expiration date
    userId

  Input/Ouput junction table
    jobId 
    local file path (transferid)
    input or output
'''


def create(database_file):
  connection = connect(database_file)
  cursor = connection.cursor()
  cursor.execute('''CREATE TABLE users (id    INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                                      login VARCHAR(50) NOT NULL UNIQUE)''')
  cursor.execute('''CREATE TABLE jobs (id                INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, 
                                       submission_date   DATE,
                                       user_id           INTEGER NOT NULL CONSTRAINT known_user REFERENCES users (id),
                                       expiration_date   DATE NOT NULL,
                                       stdout_file       VARCHAR(255),
                                       stderr_file       VARCHAR(255),
                                       join_errout       BOOLEAN NOT NULL,
                                       stdin_file        VARCHAR(255),
                                       name_description  VARCHAR(255),
                                       drmaa_id          VARCHAR(255),
                                       working_directory VARCHAR(255))''')

  cursor.execute('''CREATE TABLE transfers (local_file_path  VARCHAR(255) PRIMARY KEY NOT NULL, 
                                            remote_file_path VARCHAR(255),
                                            transfer_date    DATE,
                                            expiration_date  DATE NOT NULL,
                                            user_id          INTEGER NOT NULL CONSTRAINT known_user REFERENCES users (id))''')

  cursor.execute('''CREATE TABLE ios (job_id          INTEGER NOT NULL CONSTRAINT known_job REFERENCES jobs(id),
                                      local_file_path VARCHAR(255) NOT NULL CONSTRAINT known_local_file REFERENCES transfers (local_file_path),
                                      is_input         BOOLEAN NOT NULL,
                                      PRIMARY KEY (job_id, local_file_path))''')
                                      
  cursor.execute('''CREATE TABLE fileCounter (count INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                                              foo INTEGER)''') #!!! FIND A CLEANER WAY !!!
  
  connection.commit()
  connection.close()


def fillWithExampleData(database_file):
  
  myUsers = [ [('sl225510')], 
              [('yc176684')] ]
  
  expirationDate = Date(2010, 02, 26)
  
  myTransfers = [ ('/local/file0_l',     '/remote/file0',     expirationDate, 1),
                ('/local/file11_l',    '/remote/file11',    expirationDate, 1),
                ('/local/file12_l',    '/remote/file12',    expirationDate, 1),
                ('/local/script1_l',    '/remote/script1',  expirationDate, 1),
                ('/local/stdinJob1_l', '/remote/stdinJob1', expirationDate, 1) ]
  

  myJobs = [ ('jobs1', 1, expirationDate,  '/local/stdoutJob1', '/local/stderrJob1', False, '/local/stdinJob1_l', 123) ]
  
  myInputOutputs = [ (1, '/local/file0_l', True),
                   (1, '/local/script1_l', True),
                   (1, '/local/file11_l', True),
                   (1, '/local/file12_l', False) ]
  
  connection = connect(database_file)
  cursor = connection.cursor()
  cursor.executemany('INSERT INTO users (login) VALUES (?)', myUsers)
  cursor.executemany('INSERT INTO transfers (local_file_path, remote_file_path, expiration_date, user_id) VALUES (?, ?, ?, ?)', myTransfers)
  cursor.executemany('INSERT INTO jobs (name_description, user_id, expiration_date, stdout_file, stderr_file, join_errout, stdin_file, drmaa_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)', myJobs)
  cursor.executemany('INSERT INTO ios (job_id, local_file_path, is_input) VALUES (?, ?, ?)', myInputOutputs)
  connection.commit()
  connection.close()



def printTables(database_file):
  
  connection = connect(database_file)
  cursor = connection.cursor()
  
  print "==== users table: ========"
  for row in cursor.execute('SELECT * FROM users'):
    id, login = row
    print 'id=', repr(id).rjust(2), 'login=', repr(login).rjust(7)
    
  print "==== transfers table: ===="
  for row in cursor.execute('SELECT * FROM transfers'):
    local_file_path, remote_file_path, transfer_date, expiration_date, user_id = row
    print '| local_file_path', repr(local_file_path).ljust(25), '| remote_file_path=', repr(remote_file_path).ljust(25) , '| transfer_date=', repr(transfer_date).ljust(7), '| expiration_date=', repr(expiration_date).ljust(7), '| user_id=', repr(user_id).rjust(2), ' |'
  
  print "==== jobs table: ========"
  for row in cursor.execute('SELECT * FROM jobs'):
    print row
    #id, submission_date, user_id, expiration_date, stdout_file, stderr_file, join_errout, stdin_file, name_description, drmaa_id,     working_directory = row
    #print 'id=', repr(id).rjust(3), 'submission_date=', repr(submission_date).rjust(7), 'user_id=', repr(user_id).rjust(3), 'expiration_date' , repr(expiration_date).rjust(7), 'stdout_file', repr(stdout_file).rjust(10), 'stderr_file', repr(stderr_file).rjust(10), 'join_errout', repr(join_errout).rjust(5), 'stdin_file', repr(stdin_file).rjust(10), 'name_description', repr(name_description).rjust(10), 'drmaa_id', repr(drmaa_id).rjust(10), 'working_directory', repr(working_directory).rjust(10)
  
  print "==== ios table: ========="
  for row in cursor.execute('SELECT * FROM ios'):
    job_id, local_file_path, is_input = row
    print '| job_id=', repr(job_id).rjust(2), '| local_file_path=', repr(local_file_path).ljust(25), '| is_input=', repr(is_input).rjust(2), ' |'
  
  
  #print "==== file counter table: ========="
  #for row in cursor.execute('SELECT * FROM fileCounter'):
    #count, foo = row
    #print '| count=', repr(count).rjust(2), '| foo=', repr(foo).ljust(2), ' |'
  
  connection.close()