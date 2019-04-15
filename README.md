# hpc_twitter_geoprocessing
HPC - Twitter geoprocessing - Cluster and Cloud Computing

#### Python source code

`./src/main_sequential` -> First basic sequential approach without MPI. Which is just dataframe approach for small twitter files and a line by line approach for Big Twitter file

`./src/main_p` -> Point to Point approach using MPI

`./src/main_sg` -> Collective approach using MPI

#### To run the program, use a terminal to run in HPC terminal and submit the jobs to the scheduler:

`$ ./slurm/runme.sh`  

The program assumes data is located in the folder `$ ./data/`. Each python script takes as parameter the name of the data file which the program will search in the `data`folder  

#### Output is given in the folder:

`./output`

#### A report is included here:

`./report/Report.pdf`

