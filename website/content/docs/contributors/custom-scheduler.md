---
title: Implementing a Custom Scheduler
---

To run a Heron topology, you’ll need to set up a scheduler that is responsible 
for topology management. Note: one scheduler is managing only one topology, 
for the purpose of better isolation. Heron currently supports the following schedulers out of the box:

* [Aurora](../../operators/deployment/schedulers/aurora)
* [Local scheduler](../../operators/deployment/schedulers/local)
* [Slurm scheduler](../../operators/deployment/schedulers/slurm)

If you'd like to run Heron on a not-yet-supported system, such as
[Amazon ECS](https://aws.amazon.com/ecs/), you can create your own scheduler
using Heron's spi, as detailed in the
sections below.

Java is currently the only supported language for custom schedulers. This may
change in the future.

## Java Setup

In order to create a custom scheduler, you need to import the `heron-spi`
library into your project.

#### Maven

```xml
<dependency>
  <groupId>com.twitter.heron</groupId>
  <artifactId>heron-spi</artifactId>
  <version>{{% heronVersion %}}</version>
</dependency>
```

#### Gradle

```groovy
dependencies {
  compile group: "com.twitter.heron", name: "heron-spi", version: "{{% heronVersion %}}"
}
```

## Interfaces

Creating a custom scheduler involves implementing each of the following Java
interfaces:

Interface | Role | Examples
:-------- |:---- |:--------
[`IPacking`](/api/com/twitter/heron/spi/packing/IPacking.html) | Defines the algorithm used to generate physical plan for a topology. | [RoundRobin](/api/com/twitter/heron/packing/roundrobin/RoundRobinPacking.html)
[`ILauncher`](/api/com/twitter/heron/spi/scheduler/ILauncher.html) | Defines how the scheduler is launched | [Aurora](/api/com/twitter/heron/scheduler/aurora/AuroraLauncher.html), [local](/api/com/twitter/heron/scheduler/local/LocalLauncher.html)
[`IScheduler`](/api/com/twitter/heron/spi/scheduler/IScheduler.html) | Defines the scheduler object used to construct topologies | [local](/api/com/twitter/heron/scheduler/local/LocalScheduler.html)
[`IUploader`](/api/com/twitter/heron/spi/uploader/IUploader.html) | Uploads the topology to a shared location accessible to the runtime environment of the topology | [local](/api/com/twitter/heron/uploader/localfs/LocalFileSystemUploader.html) [hdfs](/api/com/twitter/heron/uploader/hdfs/HdfsUploader.html) [s3](/api/com/twitter/heron/uploader/s3/S3Uploader.html)

Heron provides a number of built-in implementations out of box.

## Running the Scheduler

To run the a custom scheduler, the implementation of the interfaces above must be specified in the [config](../../operators/deployment/configuration).
By default, the heron-cli looks for configurations under `${HERON_HOME}/conf/`. The location can be overridden using option `--config-path`. 
Below is an example showing the command for [topology
submission](../../operators/heron-cli#submitting-a-topology):

```bash
$ heron submit [cluster-name-storing-your-new-config]/[role]/[env] \
    --config-path [config-folder-path-storing-your-new-config] \
    /path/to/topology/my-topology.jar \
    biz.acme.topologies.MyTopology 
```

The implementation for each of the interfaces listed above must be on Heron's
[classpath](https://docs.oracle.com/javase/tutorial/essential/environment/paths.html). 


