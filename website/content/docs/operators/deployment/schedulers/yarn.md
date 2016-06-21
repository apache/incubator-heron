---
title: Apache Hadoop YARN Cluster (Experimental)
---

In addition to out-of-the-box scheduler for [Aurora](../aurora), Heron can also be deployed on a
YARN cluster with the YARN Scheduler. YARN
scheduler is implemented using Apache REEF framework.

**Key features** of YARN scheduler:

* **Heterogeneous container allocation:** YARN scheduler will request heterogeneous containers from
YARN Resource Manager. In other words the topology will not use more resources than what is needed.

* **Container reuse:** REEF framework allows YARN scheduler to retain containers
across events like topology restarts.

## How topology deployment on YARN Cluster Works

Using the YARN scheduler is similar to deploying Heron on other clusters, i.e. using the Heron cli.
This document assumes that Hadoop client is installed and configured.

YARN scheduler uses REEF framework. Following steps are executed when a Heron topology is submitted,

1. REEF client copies `Heron Core package` and `topology package` on the distributed file system.
1. It then starts YARN Application Master (AM) for the topology.
1. The AM subsequently invokes `Heron Scheduler` in the same process.
1. This is followed by container allocation for Topology Master and workers. As a result `N+2`
containers are allocated for each topology.

### Configure Hadoop classpath

1. Command `hadoop classpath` provides a list of jars needed to submit a hadoop job. Copy all jars
to `HERON_INSTALL_DIR/lib/scheduler`.
  * Do not copy commons-cli jar if it is older than version 1.3.1
1. Create a jar containing core-site.xml and yarn-site.xml. Add this jar to
`HERON_INSTALL_DIR/lib/scheduler` too

> YARN scheduler needs Hadoop and YARN jars in the classpath. Heron currently does not allow
> customizing client classpath. Hadoop and YARN jars are version dependent and should not be
> bundled with Heron distribution. So, this manual step is needed till this Heron issue
> ([271] (https://github.com/twitter/heron/issues/271)) is fixed.

### Configure YARN scheduler

A set of default configuration files are provided with Heron in the [conf/yarn]
(https://github.com/twitter/heron/tree/master/heron/config/src/yaml/conf/yarn) directory.
The default configuration uses the local state manager. This will work with single-node local
YARN installation only. Zookeeper based state management will be needed for topology
deployment on multi-node YARN cluster.

1. Custom Heron Launcher for YARN: `YarnLauncher`
1. Custom Heron Scheduler for YARN: `YarnScheduler`
1. State manager for multi-node deployment:
`com.twitter.heron.statemgr.zookeeper.curator.CuratorStateManager`
1. `YarnLauncher` performs the job of uploader also. So `NullUploader` is used.

## Topology management

### Topology Submission
**Command**

`heron submit yarn heron-examples.jar com.twitter.heron.examples.AckingTopology AckingTopology`

**Sample Output**

```
INFO: Launching topology 'AckingTopology'
...
...
Powered by
     ___________  ______  ______  _______
    /  ______  / /  ___/ /  ___/ /  ____/
   /     _____/ /  /__  /  /__  /  /___
  /  /\  \     /  ___/ /  ___/ /  ____/
 /  /  \  \   /  /__  /  /__  /  /
/__/    \__\ /_____/ /_____/ /__/

...
...
com.twitter.heron.scheduler.yarn.ReefClientSideHandlers INFO:  Topology AckingTopology is running, jobId AckingTopology.
```

**Verification**

Visit YARN http console or execute `yarn application -list` on yarn client host.

```
Total number of applications (application-types: [] and states: [SUBMITTED, ACCEPTED, RUNNING]):1
                Application-Id	    Application-Name	    Application-Type	      User	     Queue	             State	       Final-State	       Progress	                       Tracking-URL
application_1466548964728_0004	      AckingTopology	                YARN	     heron	   default	           RUNNING	         UNDEFINED	             0%	                                N/A
```

### Topology termination
**Command**

`heron kill yarn AckingTopology`


###Log File location###
Assuming HDFS as the file system, Heron logs and REEF logs can be found in the following locations
1. Logs generated when YARN AM starts:
`<LOG_DIR>/userlogs/application_1466548964728_0004/container_1466548964728_0004_01_000001/driver.stderr`

1. Scheduler logs are created on the first/AM container:
`<NM_LOCAL_DIR>/usercache/heron/appcache/application_1466548964728_0004/container_1466548964728_0004_01_000001/log-files`

1. Logs generated when TMaster starts in its container:
`<LOG_DIR>/userlogs/application_1466548964728_0004/container_1466548964728_0004_01_000002/evaluator.stderr`

1. TMaster logs are created on the second container owned by the topology app:
`<NM_LOCAL_DIR>/usercache/heron/appcache/application_1466548964728_0004/container_1466548964728_0004_01_000002/log-files`

1. Worker logs are created on the remaining containers in Node's local directory


## Work in Progress

1. YARN Scheduler will restart any failed workers and TM processes. However AM HA is not
 supported yet. As a result AM failure will result in topology failure.
 Issue: [#949](https://github.com/twitter/heron/issues/949)
1. TMaster and Scheduler are started in separate containers. Increased network latency can result
 in warning or failures. Issue: [#951] (https://github.com/twitter/heron/issues/951)
