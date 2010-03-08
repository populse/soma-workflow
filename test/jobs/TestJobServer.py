from soma.jobs.jobServer import JobServer
from datetime import date
from datetime import timedelta
from soma.jobs.jobDatabase import *

db = "job.db"
create(db)
fillWithExampleData(db)


expiration_date = date.today() + timedelta(days=7)

js=JobServer(db)
user_id = js.registerUser("toto")

#job1#
#input files
r_file0 = "/remote/file0"
r_script1  = "/remote/script1"
r_stdin1 = "/remote/stdin1"
#output files
r_file11 = "remote/file11"
r_file12 = "remote/file12"

l_file0 = js.generateLocalFilePath(user_id, r_file0)
l_script1 = js.generateLocalFilePath(user_id, r_script1)
l_stdin1 = js.generateLocalFilePath(user_id, r_stdin1)

js.addTransfer(l_file0, r_file0, expiration_date, user_id)
js.addTransfer(l_script1, r_script1, expiration_date, user_id)
js.addTransfer(l_stdin1, r_stdin1, expiration_date, user_id)

l_file11 = js.generateLocalFilePath(user_id, r_file11)
l_file12 = js.generateLocalFilePath(user_id, r_file12)

js.addTransfer(l_file11, r_file11, expiration_date, user_id)
js.addTransfer(l_file12, r_file12, expiration_date, user_id)

l_stdout1 = js.generateLocalFilePath(user_id)
l_stderr1 = js.generateLocalFilePath(user_id)

js.addTransfer(l_stdout1, None, expiration_date, user_id)
js.addTransfer(l_stderr1, None, expiration_date, user_id)


job1_id = js.addJob(user_id, expiration_date, l_stdout1, l_stderr1, l_stdin1, "job1")

js.registerInputs(job1_id, [l_file0, l_script1, l_stdin1])
js.registerOutputs(job1_id, [l_file11, l_file12])
js.registerOutputs(job1_id, [l_stdout1, l_stderr1])


printTables(db)
print "####################JOB1######################"
raw_input()


#job2 & 3#
#job2 input files
#=> l_file11
#=> l_file0
r_script2 = "remote/script2"
r_stdin2 = "remote/stdin2"
#output files
r_file2 = "remote/file2"

l_script2 = js.generateLocalFilePath(user_id, r_script2)
l_stdin2 = js.generateLocalFilePath(user_id, r_stdin2)

js.addTransfer(l_script2, r_script2, expiration_date, user_id)
js.addTransfer(l_stdin2, r_stdin2, expiration_date, user_id)

l_file2 = js.generateLocalFilePath(user_id, r_file2)

js.addTransfer(l_file2, r_file2, expiration_date, user_id)

l_stdout2 = js.generateLocalFilePath(user_id)
l_stderr2 = js.generateLocalFilePath(user_id)

js.addTransfer(l_stdout2, None, expiration_date, user_id)
js.addTransfer(l_stderr2, None, expiration_date, user_id)


job2_id = js.addJob(user_id, expiration_date, l_stdout2, l_stderr2, l_stdin2, "job2")

js.registerInputs(job2_id, [l_file0, l_file11, l_script2, l_stdin2])
js.registerOutputs(job2_id, [l_file2])
js.registerOutputs(job2_id, [l_stdout2, l_stderr2])

printTables(db)
print "####################JOB2######################"
raw_input()



#job3 input files
#=> l_file12
r_script3 = "remote/script3"
r_stdin3 = "remote/stdin3"
#job3 output files
r_file3 = "remote/file3"


l_script3 = js.generateLocalFilePath(user_id, r_script3)
l_stdin3 = js.generateLocalFilePath(user_id, r_stdin3)

js.addTransfer(l_script3, r_script3, expiration_date, user_id)
js.addTransfer(l_stdin3, r_stdin3, expiration_date, user_id)

l_file3 = js.generateLocalFilePath(user_id, r_file3)

js.addTransfer(l_file3, r_file3, expiration_date, user_id)

l_stdout3 = js.generateLocalFilePath(user_id)
l_stderr3 = js.generateLocalFilePath(user_id)

js.addTransfer(l_stdout3, None, expiration_date, user_id)
js.addTransfer(l_stderr3, None, expiration_date, user_id)

job3_id = js.addJob(user_id, expiration_date, l_stdout3, l_stderr3, l_stdin3, "job3")

js.registerInputs(job3_id, [l_file12, l_script3, l_stdin3])
js.registerOutputs(job3_id, [l_file3])
js.registerOutputs(job3_id, [l_stdout3, l_stderr3])

printTables(db)
print "####################JOB3######################"
raw_input()

js.removeTransferASAP(l_file0)
js.removeTransferASAP(l_script1)
js.removeTransferASAP(l_stdin1)
js.removeTransferASAP(l_file11)
js.removeTransferASAP(l_file12)

printTables(db)
print "###########DELETE TRANSFERS RELATED TO JOB1######################"
raw_input()

js.deleteJob(job1_id)
printTables(db)
print "####################DELETE JOB1######################"
raw_input()


js.removeTransferASAP(l_script2)
js.removeTransferASAP(l_stdin2)
js.removeTransferASAP(l_file2)

js.removeTransferASAP(l_script3)
js.removeTransferASAP(l_stdin3)
js.removeTransferASAP(l_file3)

printTables(db)
print "###########DELETE TRANSFERS RELATED TO JOB2&3######################"
raw_input()

js.deleteJob(job2_id)
printTables(db)
print "####################DELETE JOB2######################"
raw_input()


js.deleteJob(job3_id)
printTables(db)
print "####################DELETE JOB3######################"
raw_input()

