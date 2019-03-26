import mpi4py
from mpi4py import MPI
import sys
size = MPI.COMM_WORLD.Get_size()
rank = MPI.COMM_WORLD.Get_rank()
print("Helloworld! I am process %d of %d.\n" % (rank, size))

# run on terminal 
# $ time mpirun -n 2 python helloworld.py 