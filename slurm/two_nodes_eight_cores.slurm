#!/bin/bash
#SBATCH --nodes=2
#SBATCH --ntasks=8
#SBATCH --time=0-00:02:00
#SBATCH --partition=physical
#SBATCH --account=comp90024

# Load required modules
module load Python/3.5.2-goolf-2015a

# Launch multiple process python code
time mpiexec python3 ../src/main_p.py ../data/bigTwitter.json >> ../output/2_8.out
echo "2 nodes 8 cores - Point-to-Point Communication"
echo "----------------------------------------------"
time mpiexec python3 ../src/main_sg.py ../data/bigTwitter.json >> ../output/2_8.out
echo "2 nodes 8 cores - Collective Communication"