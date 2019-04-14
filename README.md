# hpc_twitter_geoprocessing
HPC - Twitter geoprocessing - Cluster and Cloud Computing

#### src file introduction

main -> First basic sequential approach without MPI. Which is just dataframe approach for small twitter files and a line by line approach for Big Twitter file

main_p -> Point to Point Appcoach using MPI

main_sg -> Collective approach using MPI

To run the program, use a terminal to run in HPC terminal and submit the jobs to the scheduler:

`$ ./slurm/runme.sh`

Output is given in the folder:

`./output`

A report is included here:

`./report/Report.pdf`
