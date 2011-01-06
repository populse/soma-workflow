from sqlite3 import *
from datetime import datetime
import os

def adapt_datetime(ts):
    return ts.strftime('%Y-%m-%d %H:%M:%S')
#Timestamp(year = ts.year, month= ts.month, day = ts.day, hour = ts.hour, minute = ts.minute, second = ts.second)
#time.mktime(ts.timetuple())

register_adapter(datetime, adapt_datetime)

database_file = "/volatile/laguitton/bidon.db"

if not os.path.isfile(database_file):
  connection = connect(database_file, timeout = 10, isolation_level = "EXCLUSIVE")
  cursor = connection.cursor()
  cursor.execute('''CREATE TABLE bidon (idb  INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                                        a   DATE)''')
  cursor.close()
  connection.commit()
  connection.close()
  
  
def add(date):
  connection = connect(database_file, timeout = 10, isolation_level = "EXCLUSIVE")
  #print "connection id = " + repr(id(connection)) + " " + thread_name
  cursor = connection.cursor()
  print repr(date)
  sqdate = Timestamp(year = date.year, month= date.month, day = date.day, hour = date.hour, minute = date.minute, second = date.second)
  cursor.execute('INSERT INTO bidon (a) VALUES (?)', [sqdate])
  connection.commit()
  idb = cursor.lastrowid
  cursor.close()
  connection.close()
  return idb


def printTable():
  connection = connect(database_file, timeout = 10, isolation_level = "EXCLUSIVE")
  #print "connection id = " + repr(id(connection)) + " " + thread_name
  cursor = connection.cursor()
  print "==== table: ========"
  for row in cursor.execute('SELECT * FROM bidon'):
    print row
    idb, a = row
    date = datetime.strptime(a.encode('utf-8'), "%Y-%m-%d %H:%M:%S")
    print 'idb=', repr(idb), 'date=', repr(date)
  cursor.close()
  connection.commit()
  connection.close()
  
  
add(datetime.now())
printTable()