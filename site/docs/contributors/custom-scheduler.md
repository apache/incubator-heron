# Implementing a Custom Scheduler

To run a Heron cluster, you'll need to set up a scheduler that is responsible
for cluster management. Heron supports three schedulers out of the box:

* [Mesos](../operators/deployment/mesos.html)
* [Aurora](../operators/deployment/aurora.html)
* [Local scheduler](../operators/deployment/local.html)

If you'd like to run Heron on a not-yet-supported system, such as
[YARN](https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html)
or [Amazon ECS](https://aws.amazon.com/ecs/), you can create your own scheduler
using Heron's [scheduler API](http://heronproject.github.io/scheduler-api/) for
Java, as detailed in the sections below.

## Java Setup

In order to create a custom scheduler, you need to import the `scheduler`
library into your project.

#### Maven

<pre><code class="lang-xml">&lt;dependency&gt;
  &lt;groupId>com.twitter.heron&lt;/groupId&gt;
  &lt;artifactId>scheduler&lt;/artifactId&gt;
  &lt;version&gt;{{book.scheduler_api_version}}&lt;/version&gt;
&lt;/dependency&gt;</code></pre>

#### Gradle

<pre><code class="lang-groovy">dependencies {
  compile group: "com.twitter.heron", name: "scheduler", version: "{{book.scheduler_api_version}}"
}</code></pre>

## Interfaces

Creating a custom scheduler involves implementing each of the following Java
interfaces:

Interface | Role | Examples
:-------- |:---- |:--------
[`IConfig`](http://heronproject.github.io/scheduler-api/com/twitter/scheduler/api/IConfig) | Parsing and loading of configuration for the scheduler | [Aurora](http://heronproject.github.io/scheduler-api/com/twitter/heron/scheduler/aurora/AuroraConfigLoader), [Mesos](http://heronproject.github.io/scheduler-api/com/twitter/heron/scheduler/mesos/MesosConfigLoader), [local](http://heronproject.github.io/scheduler-api/com/twitter/heron/scheduler/local/LocalConfigLoader)
[`ILauncher`](http://heronproject.github.io/scheduler-api/com/twitter/scheduler/api/ILauncher) | Defines how the scheduler is launched | [Aurora](http://heronproject.github.io/scheduler-api/com/twitter/heron/scheduler/aurora/AuroraLauncher), [Mesos](http://heronproject.github.io/scheduler-api/com/twitter/heron/scheduler/mesos/MesosLauncher), [local](http://heronproject.github.io/scheduler-api/com/twitter/heron/scheduler/local/LocalLauncher)
`IRuntimeManager` | Handles runtime tasks such as activating topologies, killing topologies, etc. | [Aurora](http://heronproject.github.io/scheduler-api/com/twitter/heron/scheduler/aurora/AuroraTopologyRuntimeManager), [Mesos](http://heronproject.github.io/scheduler-api/com/twitter/heron/scheduler/mesos/MesosTopologyRuntimeManager), [local](http://heronproject.github.io/scheduler-api/com/twitter/heron/scheduler/local/LocalTopologyRuntimeManager)
`IScheduler` | Defines the scheduler object used to construct topologies | [Mesos](http://heronproject.github.io/scheduler-api/com/twitter/heron/scheduler/mesos/MesosScheduler), [local](http://heronproject.github.io/scheduler-api/com/twitter/heron/scheduler/local/LocalScheduler)
`IUploader` | Uploads the topology to a shared location that must be accessible to the runtime environment of the topology |

## Loading Configuration

You can set up a configuration loader for a custom scheduler by implementing the
[`IConfig`](http://heronproject.github.io/scheduler-api/com/twitter/heron/scheduler/api/IConfig)
interface. You can use this interface to load configuration from any source
you'd like.

If you'd like to load configuration from files using the `.conf`-style syntax
[`DefaultConfigLoader`](http://heronproject.github.io/scheduler-api/com/twitter/heron/scheduler/util/DefaultConfigLoader)

## Testing Your Scheduler



Once you've implemented a scheduler in Java, you can try it out by submitting a
topology and specifying your scheduler using the 

