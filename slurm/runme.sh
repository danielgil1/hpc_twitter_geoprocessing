#!/bin/bash
sbatch one_node_one_core.slurm
sbatch one_node_two_cores.slurm
sbatch one_node_four_cores.slurm
sbatch one_node_eight_cores.slurm
sbatch two_nodes_eight_cores.slurm