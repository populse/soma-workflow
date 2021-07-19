#!/bin/bash
echo "SOMA_JOB_MPI_BIN" $SOMA_JOB_MPI_BIN "SOMA_JOB_NODE_FILE" $SOMA_JOB_NODE_FILE

. $SOMA_JOB_MPI_BIN/mpirun -machinefile `eval echo $SOMA_JOB_NODE_FILE` -np $1 $2
