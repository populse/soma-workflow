When updating soma-workflow on a cluster, you need to kill it in order to restart it.

This can be done with:
```
  $ # Kill the database server
  $ kill $(ps -ef | grep 'python -m soma_workflow.start_database_server' | grep `whoami`  | grep -v grep | awk '{print $2}')
  $ # Kill engine server
  $ kill $(ps -ef | grep 'python -m soma_workflow.start_workflow_engine' | grep `whoami`  | grep -v grep | awk '{print $2}')
```

The servers will be restarted automatically on next connexion.
