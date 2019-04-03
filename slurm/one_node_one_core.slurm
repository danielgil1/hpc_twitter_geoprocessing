#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --time=0-00:00:40
#SBATCH --partition=physical
#SBATCH --account=comp90024

# Load required modules
#module load Python/3.6.4-intel-2018.u4
module load Python/3.5.2-goolf-2015a
# Launch multiple process python code
echo "Searching for hashtags"
time mpiexec -n 1 python3 main_p.py -i smallTwitter.json -m
