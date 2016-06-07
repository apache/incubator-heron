---
title: Implementing a Custom Scheduler
---

To run a Heron cluster, you'll need to set up a scheduler that is responsible
for cluster management. Heron currently supports followings schedulers out of the box:

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
  <version>{{.Site.Params.versions.heronspi}}</version>
</dependency>
```
#### Gradle

```groovy
dependencies {
  compile group: "com.twitter.heron", name: "heron-spi", version: "{{.Site.Params.versions.heronspi}}"
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

Your implementation of those interfaces will need to be on Heron's
[classpath](https://docs.oracle.com/javase/tutorial/essential/environment/paths.html)
when you [compile Heron](../../developers/compiling).

Those interfaces and implementation are independent; you can choose and combine them to fit your use scenarios.

## Trying Out Your Scheduler

Once you've implemented a custom scheduler (or other components), you'll need to specify implementation of all interfaces above
in the [config](../../operators/deployment/configuration) . You'll also need to point to the config folder when submitting a topology via heron-cli. Here's an example [topology
submission](../../operators/heron-cli#submitting-a-topology) command:

```bash
$ heron submit [cluster-name-storing-your-new-config]/[role]/[env] \
    --config-path [config-folder-path-storing-your-new-config] \
    /path/to/topology/my-topology.jar \
    biz.acme.topologies.MyTopology 
```
