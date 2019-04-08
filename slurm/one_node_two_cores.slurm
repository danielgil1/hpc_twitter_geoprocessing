#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=2
#SBATCH --time=0-00:05:00
#SBATCH --partition=physical
#SBATCH --account=comp90024

# Load required modules
module load Python/3.5.2-goolf-2015a

# Launch multiple process python code
time mpiexec python3 main_p.py bigTwitter.json
echo "1 node 2 cores"