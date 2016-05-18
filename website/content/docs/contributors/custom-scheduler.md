---
title: Implementing a Custom Scheduler
---

To run a Heron cluster, you'll need to set up a scheduler that is responsible
for cluster management. Heron supports three schedulers out of the box:

* [Mesos](../../operators/deployment/schedulers/mesos)
* [Aurora](../../operators/deployment/schedulers/aurora)
* [Local scheduler](../../operators/deployment/schedulers/local)

If you'd like to run Heron on a not-yet-supported system, such as
[YARN](https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html)
or [Amazon ECS](https://aws.amazon.com/ecs/), you can create your own scheduler
using Heron's [scheduler API](/api/scheduler/index.html), as detailed in the
sections below.

Java is currently the only supported language for custom schedulers. This may
change in the future.

## Java Setup

In order to create a custom scheduler, you need to import the `scheduler`
library into your project.

#### Maven

```xml
<dependency>
  <groupId>com.twitter.heron</groupId>
  <artifactId>scheduler-api</artifactId>
  <version>{{.Site.Params.versions.schedulerapi}}</version>
</dependency>
```
#### Gradle

```groovy
dependencies {
  compile group: "com.twitter.heron", name: "scheduler-api", version: "{{.Site.Params.versions.schedulerapi}}"
}
```

## Interfaces

Creating a custom scheduler involves implementing each of the following Java
interfaces:

Interface | Role | Examples
:-------- |:---- |:--------
[`IConfigLoader`](/api/com/twitter/heron/spi/scheduler/IConfigLoader.html) | Parsing and loading of configuration for the scheduler | [Aurora](/api/scheduler/com/twitter/heron/scheduler/aurora/AuroraConfigLoader.html), [Mesos](/api/scheduler/com/twitter/heron/scheduler/mesos/MesosConfigLoader.html), [local](/api/scheduler/com/twitter/heron/scheduler/local/LocalConfigLoader.html)
[`ILauncher`](/api/com/twitter/heron/spi/scheduler/ILauncher.html) | Defines how the scheduler is launched | [Aurora](/api/scheduler/com/twitter/heron/scheduler/aurora/AuroraLauncher.html), [Mesos](/api/scheduler/com/twitter/heron/scheduler/mesos/MesosLauncher.html), [local](/api/scheduler/com/twitter/heron/scheduler/local/LocalLauncher.html)
[`IRuntimeManager`](/api/com/twitter/heron/spi/scheduler/IRuntimeManager.html) | Handles runtime tasks such as activating topologies, killing topologies, etc. | [Aurora](/api/scheduler/com/twitter/heron/scheduler/aurora/AuroraTopologyRuntimeManager.html), [Mesos](/api/scheduler/com/twitter/heron/scheduler/mesos/MesosTopologyRuntimeManager.html), [local](/api/scheduler/com/twitter/heron/scheduler/local/LocalTopologyRuntimeManager.html)
[`IScheduler`](/api/com/twitter/heron/spi/scheduler/IScheduler.html) | Defines the scheduler object used to construct topologies | [Mesos](/api/scheduler/com/twitter/heron/scheduler/mesos/MesosScheduler.html), [local](/api/scheduler/com/twitter/heron/scheduler/local/LocalScheduler.html)
[`IUploader`](/api/com/twitter/heron/spi/scheduler/IUploader.html) | Uploads the topology to a shared location that must be accessible to the runtime environment of the topology | [Aurora](), [Mesos](), [local](/api/scheduler/com/twitter/heron/scheduler/local/LocalUploader.html)

Your implementation of those interfaces will need to be on Heron's
[classpath](https://docs.oracle.com/javase/tutorial/essential/environment/paths.html)
when you [compile Heron](../../developers/compiling).

## Loading Configuration

You can set up a configuration loader for a custom scheduler by implementing the
[`IConfig`](/api/com/twitter/heron/spi/scheduler/IConfig.html)
interface. You can use this interface to load configuration from any source
you'd like, e.g. YAML files, JSON files, or a web service.

If you'd like to load configuration from files using the same syntax as Heron's
default configuration files for the Aurora, Mesos, and local schedulers (in
`heron/cli2/src/python`), you can implement the
[`DefaultConfigLoader`](/api/scheduler/com/twitter/heron/scheduler/util/DefaultConfigLoader.html)
interface.

## Configurable Parameters

At the very least, your configuration loader will need to be able to load the
class names (as strings) for your implementations of the components listed
above, as you can see from the interface definition for
[`IConfigLoader`](/api/com/twitter/heron/spi/scheduler/IConfigLoader.html).

## Trying Out Your Scheduler

Once you've implemented a custom configuration loader, you'll need to specify
your loader by class using the `--config-loader` flag. If your loader relies on
a configuration file, specify the path of that file using the `--config-file`
flag. Here's an example [topology
submission](../../operators/heron-cli#submitting-a-topology) command:

```bash
$ heron-cli submit "topology.debug:true" \
    /path/to/topology/my-topology.jar \
    biz.acme.topologies.MyTopology \
    --config-file=/path/to/config/my_scheduler.conf \
    --config-loader=biz.acme.config.MyConfigLoader
```
