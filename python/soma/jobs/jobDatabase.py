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
  cursor.execute('CREATE TABLE users (id VARCHAR(50) PRIMARY KEY NOT NULL)')
  cursor.execute('''CREATE TABLE jobs (id               INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, 
                                     submission_date  DATE,
                                     expiration_date  DATE NOT NULL,
                                     user_id          VARCHAR(50) NOT NULL CONSTRAINT known_user REFERENCES users (id),
                                     uses_stdout      BOOLEAN NOT NULL,
                                     uses_stderr      BOOLEAN NOT NULL,
                                     stdout_file      VARCHAR(255),
                                     stderr_file      VARCHAR(255),
                                     stdin_file       VARCHAR(255),
                                     name_description VARCHAR(255),
                                     drmaa_id         VARCHAR (255))''')

  cursor.execute('''CREATE TABLE transfers (local_file_path  VARCHAR(255) PRIMARY KEY NOT NULL, 
                                          remote_file_path VARCHAR(255) NOT NULL,
                                          transfer_date    DATE,
                                          expiration_date  DATE NOT NULL,
                                          user_id          VARCHAR(50) NOT NULL CONSTRAINT known_user REFERENCES users (id))''')

  cursor.execute('''CREATE TABLE ios (job_id          INTEGER NOT NULL CONSTRAINT known_job REFERENCES jobs(id),
                                            local_file_path VARCHAR(255) NOT NULL CONSTRAINT known_local_file REFERENCES transfers (local_file_path),
                                            isInput         BOOLEAN NOT NULL,
                                            PRIMARY KEY (job_id, local_file_path))''')
  connection.commit()
  connection.close()


def fillWithExampleData(database_file):
  
  myUsers = [ [('sl225510')], 
              [('yc176684')] ]
  
  expirationDate = Date(2010, 02, 26)
  
  myTransfers = [ ('/local/file0_l',     '/remote/file0',     expirationDate, "sl225510"),
                ('/local/file11_l',    '/remote/file11',    expirationDate, "sl225510"),
                ('/local/file12_l',    '/remote/file12',    expirationDate, "sl225510"),
                ('/local/script1_l',    '/remote/script1',  expirationDate, "sl225510"),
                ('/local/stdinJob1_l', '/remote/stdinJob1', expirationDate, "sl225510") ]
  

  myJobs = [ ('jobs1', expirationDate, "sl225510", True, True, '/local/stdoutJob1', '/local/stderrJob1', '/local/stdinJob1_l') ]
  
  myInputOutputs = [ (1, '/local/file0_l', True),
                   (1, '/local/script1_l', True),
                   (1, '/local/file11_l', True),
                   (1, '/local/file12_l', False) ]
  
  connection = connect(database_file)
  cursor = connection.cursor()
  cursor.executemany('INSERT INTO users (id) VALUES (?)', myUsers)
  cursor.executemany('INSERT INTO transfers (local_file_path, remote_file_path, expiration_date, user_id) VALUES (?, ?, ?, ?)', myTransfers)
  cursor.executemany('INSERT INTO jobs (name_description, expiration_date, user_id, uses_stdout, uses_stderr, stdout_file, stderr_file, stdin_file) VALUES (?, ?, ?, ?, ?, ?, ?, ?)', myJobs)
  cursor.executemany('INSERT INTO ios (job_id, local_file_path, isInput) VALUES (?, ?, ?)', myInputOutputs)
  connection.commit()
  connection.close()



def printTables(database_file):
  
  connection = connect(database_file)
  cursor = connection.cursor()
  
  print "==== users table: ========"
  for row in cursor.execute('SELECT * FROM users'):
    print row
    
  print "==== transfers table: ===="
  for row in cursor.execute('SELECT * FROM transfers'):
    print row
  
  print "==== jobs table: ========"
  for row in cursor.execute('SELECT * FROM jobs'):
    print row
  
  print "==== ios table: ========="
  for row in cursor.execute('SELECT * FROM ios'):
    print row
   
  connection.close()