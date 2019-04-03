#!/bin/bash
#SBATCH --nodes=2
#SBATCH --ntasks=8
#SBATCH --time=0-00:02:00
#SBATCH --partition=physical
#SBATCH --account=comp90024

# Load required modules
module load Python/3.5.2-goolf-2015a

# Launch multiple process python code
echo "Searching for hashtags"
time mpiexec -n 2 python3 main_p.py bigTwitter.json > two_nodes_eight_cores.out
