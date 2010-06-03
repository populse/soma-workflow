
import paramiko
import time
import sys
import logging


if __name__=="__main__":
  if len(sys.argv) != 2:
    sys.stdout.write("Argument : password \n")
    sys.exit(1)


submitting_machine = 'gabriel.intra.cea.fr'
login = "sl225510"
password = sys.argv[1]

logging.basicConfig(
      filename = "/home/sl225510/tunnellog",
      format = "%(asctime)s %(threadName)s: %(message)s",
      level = logging.DEBUG)

g_verbose = True

client = paramiko.SSHClient()
client.load_system_host_keys()
#client.set_missing_host_key_policy(paramiko.WarningPolicy)
print '*** Connecting...'
client.connect(hostname = submitting_machine, port=22, username=login, password=password)
stdin, stdout, stderr = client.exec_command("python /home/sl225510/somajobs_test/python/TestTunnelServer.py")
connection_checker_uri = stdout.readline()
print "connection_checker_uri = " + connection_checker_uri + "\n"

client.close()


sys.exit(0)
