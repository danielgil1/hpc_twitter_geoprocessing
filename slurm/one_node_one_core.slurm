#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --time=0-00:07:00
#SBATCH --partition=physical
#SBATCH --account=comp90024

# Load required modules Python/3.6.4-intel-2017.u2
module load Python/3.5.2-goolf-2015a
# Launch multiple process python code
time mpiexec python3 ../src/main_p.py ../data/bigTwitter.json > ../output/1_1.out
echo "1 node 1 core - Point-to-Point Communication"
echo "----------------------------------------------"
time mpiexec python3 ../src/main_sg.py ../data/bigTwitter.json >> ../output/1_1.out
echo "1 node 1 core - Collective Communication"
