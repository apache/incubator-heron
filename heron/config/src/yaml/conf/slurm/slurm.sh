#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

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
    srun -lN1 -n1 --nodes=1 --relative=$index $1 --shard=$index ${@:2} &
done

echo $SLURM_JOB_ID > slurm-job.pid

wait

