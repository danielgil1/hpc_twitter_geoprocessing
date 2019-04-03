#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=4
#SBATCH --time=0-00:04:00
#SBATCH --partition=physical
#SBATCH --account=comp90024

# Load required modules
module load Python/3.5.2-goolf-2015a

# Launch multiple process python code
echo "Searching for hashtags - 1 node 8 cores"
time mpiexec -n 1 python3 main_p.py bigTwitter.json > one_node_eight_cores.out
