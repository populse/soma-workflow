from mpi4py import MPI

if __name__ == '__main__':

    comm = MPI.COMM_WORLD
    any_source = MPI.ANY_SOURCE
    any_tag = MPI.ANY_TAG
    rank = comm.Get_rank()
    size = comm.size


# master code
if rank == 0:
    import numpy as np
    max_elt = 10
    r_param = np.random.randn(max_elt)

    status = MPI.Status()

    for task in r_param:
        # print 'ask me.'
        send_task = False
        while send_task == False:
            comm.Probe(source=any_source, tag=11, status=status)
            s = status.Get_source()
            data = comm.recv(source=s, tag=11)
            # print "receive %s from %d " % (data, s)
            if data == 'JOB_PLEASE':
                comm.send(task, dest=s, tag=12)
            else:
                print 'bad message from %d' % s
            send_task = True
    for slave in range(1, comm.size):
        comm.send('STOP', dest=slave, tag=13)
    print "### master ends ###"
# slave code
else:
    status = MPI.Status()
    while True:
        # print 'job_please ', rank
        comm.send('JOB_PLEASE', dest=0, tag=11)
        comm.Probe(source=any_source, tag=any_tag, status=status)
        t = status.Get_tag()
        if t != 13:
            data = comm.recv(source=0, tag=12)
            print 'receive %f from the master. ' % data
        else:
            break
