#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --time=0-00:06:00
#SBATCH --partition=physical
#SBATCH --account=comp90024

# Load required modules
module load Python/3.5.2-goolf-2015a
# Launch multiple process python code
echo "Searching for hashtags"
time mpiexec -n 1 python3 main_p.py bigTwitter.json > one_node_one_core.out
