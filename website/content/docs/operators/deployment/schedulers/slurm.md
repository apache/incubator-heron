---
title: Slurm
---

In addition to out-of-the-box schedulers for [Mesos](../mesos) and
[Aurora](../aurora), Heron can also be deployed in a HPC cluster with the Slurm Scheduler.
This allows a researcher to deploy Heron and execute streaming scientific work-flows.

## How Slurm Deployment Works

Using the Slurm scheduler is similar to deploying Heron on other systems in
that you use the [Heron CLI](../../heron-cli) to manage topologies. The
difference is in the configuration and [scheduler
overrides](../../heron-cli#submitting-a-topology) that you provide when
you [submit a topology](../../heron-cli#submitting-a-topology).

A set of default configurations are provided with Heron in the `conf/slurm` directory. 
The default configurations use the file system based state manager. 

When a Heron topology is submitted, the Slurm scheduler allocates the nodes required to 
run the job and starts the Heron processes in those nodes. It uses a `slurm.sh` script found in 
`conf/slum` directory to submit the topoloy as a batch job to the slurm scheduler.

### Useful Configuration files

These are some of the useful configuration files found in `conf/slurm` directory.

#### scheduler.yaml

This configuration file specifies the scheduler implementation to use and 
properties for that scheduler.

* `heron.local.working.directory` &mdash; The shared directory to be used as
  Heron's sandbox directory.
  
#### statemgr.yaml

This is the configuration for the state manager. 

* `heron.class.state.manager` &mdash; Specifies the state manager. 
   By default it uses the local state manager. Refer the `conf/localzk/statemgr.yaml` for zookeeper
   based state manager configurations.
   
#### slurm.sh
   
This is the script used by the scheduler to submit the Heron job to the Slurm scheduler. You can
change this file for specific slurm settings like time, account.     

### Example Submission Command 

Here is an example command to submit the MultiSpoutExclamationTopology that comes with Heron.

```bash
$ heron submit slurm HERON_HOME/heron/examples/heron-examples.jar com.twitter.heron.examples.MultiSpoutExclamationTopology Name    
```

## Example Kill Command 

To kill the topology you can use the kill command with the cluster name and topolofy name.

```bash
$ heron kill cluster_name Topology_name
```
