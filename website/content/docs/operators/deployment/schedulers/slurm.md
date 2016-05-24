---
title: Slurm Cluster (Experimental)
---

In addition to out-of-the-box schedulers for [Mesos](../mesos) and
[Aurora](../aurora), Heron can also be deployed in a HPC cluster with the Slurm Scheduler.
This allows a researcher to deploy Heron and execute streaming scientific work-flows.

## How Slurm Deployment Works

Using the Slurm scheduler is similar to deploying Heron on other systems in
that you use the [Heron](../../heron-cli) cli to manage topologies. Again the
difference is in the configuration. 

A set of default configuration files are provided with Heron in the [conf/slurm]
(https://github.com/twitter/heron/tree/master/heron/config/src/yaml/conf/slurm) directory. 
The default configuration uses the local file system based state manager. It is
possible that the local file system is mounted using NFS.

When a Heron topology is submitted, the Slurm scheduler allocates the nodes required to 
run the job and starts the Heron processes in those nodes. It uses a `slurm.sh` script found in 
[conf/slum](https://github.com/twitter/heron/tree/master/heron/config/src/yaml/conf/slurm)
directory to submit the topoloy as a batch job to the slurm scheduler.

## Slurm Scheduler Configuration

You can instruct Heron to use slurm scheduler by modifying the `scheduler.yaml`
config file. You'll need to specify the following:

* `heron.class.scheduler` --- Indicates the class to be loaded for slurm scheduler.
You should set this to `com.twitter.heron.scheduler.slurm.SlurmScheduler`

* `heron.class.launcher` --- This specifies the class to be loaded for launching
topologies. You should set this to `com.twitter.heron.scheduler.slurm.SlurmLauncher`

* `heron.scheduler.local.working.directory` --- The shared directory to be used as
Heron sandbox directory.

* `heron.package.core.uri` --- Indicates the location of the heron core binary package.
The local scheduler uses this URI to download the core package to the working directory.

* `heron.directory.sandbox.java.home` --- This is used to specify the java home to
be used when running topologies in the containers. You could use `${JAVA_HOME}` which
means pick up the value set in the bash environment variable $JAVA_HOME.

* `heron.scheduler.is.service` --- This config is used to indicate whether the scheduler
is a service. In the case of Slurm, it should be set to `False`.

### Example Slurm Scheduler Configuration

```yaml
# scheduler class for distributing the topology for execution
heron.class.scheduler: com.twitter.heron.scheduler.slurm.SlurmScheduler

# launcher class for submitting and launching the topology
heron.class.launcher: com.twitter.heron.scheduler.slurm.SlurmLauncher

# working directory for the topologies
heron.scheduler.local.working.directory: ${HOME}/.herondata/topologies/${CLUSTER}/${TOPOLOGY}

# location of java - pick it up from shell environment
heron.directory.sandbox.java.home: ${JAVA_HOME}

# Invoke the IScheduler as a library directly
heron.scheduler.is.service: False
```

## Slurm Script `slurm.sh`
   
The script `slurm.sh` is used by the scheduler to submit the Heron job to the Slurm scheduler. 
You can change this file for specific slurm settings like time, account. You need a copy of 
this script to reside along with `scheduler.yaml` and other configuration files in the cluster
configuration.
