#!/usr/bin/env bash

# arg1: the heron executable
# arg2: arguments to executable

#SBATCH --ntasks-per-node=1
#SBATCH --time=00:30:00

module load python

args_to_start_executor=$2
ONE=1
for i in $(seq 1 $SLURM_NNODES); do
    index=`expr $i - $ONE`
    echo "Exec" $1 $index ${@:3}
    srun -lN1 -n1 --nodes=1 --relative=$index $1 $index ${@:2} &
done

echo $SLURM_JOB_ID > slurm-job.pid

wait
