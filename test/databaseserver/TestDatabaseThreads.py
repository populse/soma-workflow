from sqlite3 import *
import os
import threading
import time

database_file = "/volatile/laguitton/bidon.db"

if not os.path.isfile(database_file):
  #os.remove(database_file)         
  connection = connect(database_file, timeout = 10, isolation_level = "EXCLUSIVE")
  cursor = connection.cursor()
  cursor.execute('''CREATE TABLE bidon (idb  INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                                        a   VARCHAR(50), 
                                        b   INTEGER)''')
  cursor.close()
  connection.commit()
  connection.close()

def add(a, b, thread_name):
  connection = connect(database_file, timeout = 10, isolation_level = "EXCLUSIVE")
  #print "connection id = " + repr(id(connection)) + " " + thread_name
  cursor = connection.cursor()
  cursor.execute('INSERT INTO bidon (a, b) VALUES (?, ?)', (a, b))
  connection.commit()
  idb = cursor.lastrowid
  cursor.close()
  connection.close()
  return idb

def remove(idb, thread_name):
  connection = connect(database_file, timeout = 10, isolation_level = "EXCLUSIVE")
  #print "connection id = " + repr(id(connection)) + " " + thread_name
  cursor = connection.cursor()
  cursor.execute('DELETE FROM bidon WHERE idb=?', [idb])
  cursor.close()
  connection.commit()
  connection.close()

def update(idb, b, thread_name):
  connection = connect(database_file, timeout = 10, isolation_level = "EXCLUSIVE")
  #print "connection id = " + repr(id(connection)) + " " + thread_name
  cursor = connection.cursor()
  cursor.execute('UPDATE bidon SET b=? WHERE idb=?', (b, idb))
  cursor.close()
  connection.commit()
  connection.close()

def printTable():
  connection = connect(database_file, timeout = 10, isolation_level = "EXCLUSIVE")
  #print "connection id = " + repr(id(connection)) + " " + thread_name
  cursor = connection.cursor()
  print "==== table: ========"
  for row in cursor.execute('SELECT * FROM bidon'):
    idb, a, b = row
    print 'idb=', repr(idb).rjust(3), 'a=', repr(a).rjust(7), 'b=', repr(b).rjust(6)
  cursor.close()
  connection.commit()
  connection.close()

def simpleTest(thread_name):
  ids = []
  for i in range(0,9):
    newId = add("toto" + repr(i), i+1000, thread_name)
    ids.append(newId) 
  #printTable()

  for i in range(0,9,2):
    update(ids[i], i+10000, thread_name)
  #printTable()
    
  for i in range(1,8,2):
    update(ids[i], i+20000, thread_name)
  #printTable()
    
  for i in range(0,9,2):
    remove(ids[i], thread_name)
  #printTable()  
  
  for i in range(1,8,2):
    remove(ids[i], thread_name)
  #printTable()
  #time.sleep(0.5)
   

def simpleTestLoop(thread_name):
  for i in range(1, 20):
    print thread_name + " "+ repr(i) + ">>>>>"
    simpleTest(thread_name)
    print "<<<<<" + thread_name + " " + repr(i) 
    
    
for it in range(1,25):
  thread_name = "JobServerTestThread" + repr(it)
  thread = threading.Thread(name= thread_name, target= simpleTestLoop, args=(thread_name,))
  thread.daemon = True
  thread.start()
  

simpleTestLoop("MainThread")

   

