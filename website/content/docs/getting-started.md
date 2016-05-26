---
title: Quick Start Guide
description: Run a single-node Heron cluster on your laptop
---

The easiest way to get started learning Heron is to install and run pre-compiled
Heron binaries, which are currently available for:

* Mac OS X
* Ubuntu >= 14.04

## Step 1 --- Download Heron binaries using installation scripts

Go to the [releases page](https://github.com/twitter/heron/releases) for Heron
and download two installation scripts for your platform. The names of the
scripts have this form:

* `heron-client-install-{{% heronVersion %}}-PLATFORM.sh`
* `heron-tools-install-{{% heronVersion %}}-PLATFORM.sh`

The installation scripts for Mac OS X (`darwin`), for example, would be named
`heron-client-install-{{% heronVersion %}}-darwin.sh` and
`heron-tools-install-{{% heronVersion %}}-darwin.sh`.

Once you've downloaded the scripts, run the Heron client script with the
`--user` flag set:

```bash
$ chmod +x heron-client-install-VERSION-PLATFORM.sh
$ ./heron-client-install-VERSION-PLATFORM.sh --user
Heron client installer
----------------------

Uncompressing......
Heron is now installed!

Make sure you have "/usr/local/bin" in your path.
# etc
```

To add `/usr/local/bin` to your path, run:

```bash
$ export PATH=$PATH:/usr/local/bin
```

Now run the script for Heron tools (setting the `--user` flag):

```bash
$ chmod +x heron-tools-install-VERSION-PLATFORM.sh
$ ./heron-tools-install-VERSION-PLATFORM.sh --user
Heron tools installer
---------------------

Uncompressing......
Heron Tools is now installed!
# etc
```

## Step 2 --- Launch an example topology

If you set the `--user` flag when running the installation scripts, some example
topologies will be installed in your `~/.heron/examples` directory. You can
launch an example [topology](../concepts/topologies) locally (on your machine)
using the [Heron CLI tool](../operators/heron-cli):

```bash
$ heron submit local \
  ~/.heron/examples/heron-examples.jar \ # The path of the topology's jar file
  com.twitter.heron.examples.ExclamationTopology \ # The topology's Java class
  ExclamationTopology # The name of the topology
```

This will *submit* the topology to your locally running Heron cluster but it
won't *activate* the topology. That will be explored in step 5 below.

## Step 3 --- Start Heron Tracker

The [Heron Tracker](../operators/heron-tracker) is a web service that
continuously gathers information about your Heron cluster. You can launch the
tracker by running the `heron-tracker` command (which is already installed):

```bash
$ heron-tracker
... Running on port: 8888
... Using config file: /Users/USERNAME/.herontools/conf/heron_tracker.yaml
```

You can reach Heron Tracker in your browser at http://localhost:8888.

## Step 4 --- Start Heron UI

[Heron UI](../operators/heron-ui) is a user interface that uses Heron Tracker to
provide detailed visual representations of your Heron topologies. To launch
Heron UI:

```bash
$ heron-ui
... Running on port: 8889
... Using tracker url: http://localhost:8888
```

You can open Heron UI in your browser at http://localhost:8889.

## Step 5 --- Explore topology management commands

In step 2 you submitted a topology to your local cluster. The `heron` CLI tool
also enables you to activate, deactivate, and kill topologies and more.

```bash
$ heron activate local ExclamationTopology
$ heron deactivate local ExclamationTopology
$ heron kill local ExclamationTopology
```

For more info on these commands, read about [topology
lifecycles](../concepts/topologies#topology-lifecycle).

To list the available CLI commands, run `heron` by itself:

```bash
usage: heron <command> <options> ...

Available commands:
    activate           Activate a topology
    deactivate         Deactivate a topology
    help               Prints help for commands
    kill               Kill a topology
    restart            Restart a topology
    submit             Submit a topology
    version            Print version of heron-cli

For detailed documentation, go to http://heronstreaming.io
```

To invoke help output for a command, run `heron help COMMAND`. Here's an
example:

```bash
$ heron help submit
usage: heron submit [options] cluster/[role]/[environ] topology-file-name topology-class-name [topology-args]

Required arguments:
  cluster/[role]/[env]  Cluster, role, and environ to run topology
  topology-file-name    Topology jar/tar/zip file
  topology-class-name   Topology class name

Optional arguments:
  --config-path (a string; path to cluster config; default: "/Users/$USER/.heron/conf")
  --config-property (key=value; a config key and its value; default: [])
  --deploy-deactivated (a boolean; default: "false")
  -D DEFINE             Define a system property to pass to java -D when
                        running main.
  --verbose (a boolean; default: "false")
```

## Step 6 --- Explore other example topologies

The source code for the example topologies can be found
[on
GitHub]({{% githubMaster %}}/heron/examples/src/java/com/twitter/heron/examples).
The included example topologies:

* `AckingTopology.java` --- A topology with acking enabled.
* `ComponentJVMOptionsTopology.java` --- A topology that supplies JVM options
  for each component.
* `CustomGroupingTopology.java` --- A topology that implements custom grouping.
* `ExclamationTopology.java` --- A spout that emits random words to a bolt that
  then adds an explanation mark.
* `MultiSpoutExclamationTopology.java` --- a topology with multiple spouts.
* `MultiStageAckingTopology.java` --- A three-stage topology. A spout emits to a
  bolt that then feeds to another bolt.
* `TaskHookTopology.java` --- A topology that uses a task hook to subscribe t
   event notifications.

### Next Steps

* [Upgrade Storm topologies](../upgrade-storm-to-heron) with simple `pom.xml`
  changes
* [Deploy topologies](../operators/deployment) in clustered, scheduler-driven
  environments (such as on [Aurora](../operators/deployment/schedulers/aurora)
  and [locally](../operators/deployment/schedulers/local))
* [Develop topologies](../concepts/architecture) for Heron
