#include "mpi.h"
#include <stdio.h> 
#include <string.h>

main(int argc, char ** argv) {
  int my_rank;         /* Rank of process */
  int p;               /* Number of processes */
  int source;          /* Rank of sender */
  int dest;            /* Rank of receiver */
  int tag = 50;        /* Tag for messages */
  char message[100];   /* Storage for the message */
  MPI_Status status;  /* Return status for receive */

  MPI_Init(&argc, &argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
  MPI_Comm_size(MPI_COMM_WORLD, &p);

  if(my_rank != 0) {
    sprintf(message, "Grettings from process %d!", my_rank);
    dest = 0;
    MPI_Send(message, strlen(message)+1, MPI_CHAR, dest, tag, MPI_COMM_WORLD);
  } else {
    for(source = 1; source < p; source ++) {
      MPI_Recv(message, 100, MPI_CHAR, source, tag, MPI_COMM_WORLD, &status);
      printf("%s\n", message);
    }
  }
  MPI_Finalize();
  return 0;
}
