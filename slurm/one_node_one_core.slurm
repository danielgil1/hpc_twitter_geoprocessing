#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --time=0-00:06:00
#SBATCH --partition=physical
#SBATCH --account=comp90024

# Load required modules Python/3.6.4-intel-2017.u2
module load Python/3.5.2-goolf-2015a
# Launch multiple process python code
time mpiexec python3 main_p.py bigTwitter.json
echo "1 node 1 core"