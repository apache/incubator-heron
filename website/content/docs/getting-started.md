---
title: Quick Start Guide
description: Run a single-node Heron cluster on your laptop
aliases:
  - /docs/install.html
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
...
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
...
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

INFO: Launching topology 'ExclamationTopology'
...
[2016-06-07 16:44:07 -0700] com.twitter.heron.scheduler.local.LocalLauncher INFO: \
For checking the status and logs of the topology, use the working directory \
/Users/{username}/.herondata/topologies/local/{role}/ExclamationTopology # working directory

INFO: Topology 'ExclamationTopology' launched successfully
INFO: Elapsed time: 3.409s.
```

Note the logged working directory will be useful for troubleshooting.

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

You can reach Heron Tracker in your browser at [http://localhost:8888](http://localhost:8888)
and see something like this ![alt tag](http://twitter.github.io/heron/img/heron-tracker.png)

## Step 4 --- Start Heron UI

[Heron UI](../operators/heron-ui) is a user interface that uses Heron Tracker to
provide detailed visual representations of your Heron topologies. To launch
Heron UI:

```bash
$ heron-ui
... Running on port: 8889
... Using tracker url: http://localhost:8888
```

You can open Heron UI in your browser at [http://localhost:8889](http://localhost:8889)
and see something like this ![alt tag](http://twitter.github.io/heron/img/heron-ui.png)

## Step 5 --- Explore topology management commands

In step 2 you submitted a topology to your local cluster. The `heron` CLI tool
also enables you to activate, deactivate, and kill topologies and more.

```bash
$ heron activate local ExclamationTopology
$ heron deactivate local ExclamationTopology
$ heron kill local ExclamationTopology
```

Upon successful actions, a message similar to the following will appear:

```bash
INFO: Successfully activated topology 'ExclamationTopology'
INFO: Elapsed time: 1.980s.
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
* `TaskHookTopology.java` --- A topology that uses a task hook to subscribe to
   event notifications.

## Frequently Asked Questions

### 1. How do I know if a topology has been successfully submitted?

- The following message will appear upon successful submission:

```bash
  INFO: Topology 'ExclamationTopology' launched successfully
```
- Note even if the topology is submitted successfully, it could still fail to
start some component. For example, `stmgr` may fail to start due to unfulfilled
dependencies. To troubleshoot, refer to question 3 and the debugging example below.

### 2. How do I know if a topology is running normally?
- Heron Tracker and Heron UI provides information about the Heron cluster and
topologies. Refer to step 3 and 4 above.

### 3. How can I debug if a topology failed to start?
- `~/.herondata/topologies/{cluster}/{role}/{topologyName}/heron-executor.stdout`
and `~/.herondata/topologies/{cluster}/{role}/{topologyName}/log-files/` contain
helpful information about what went wrong.
- Installation of `libunwind` is required. Upgrade of `gcc` and `glibc` might be needed.
- Killing the topology via `heron kill` and resubmitting it via `heron submit` might also help.
  - If `heron kill` returned error, the topology can still be killed by running
  `killall` to kill associated running process and `rm -rf ~/.herondata/` to clean up the state.

### A debugging example
This example illustrates in detail how to investigate failures. Note the error
messages may vary case by case, but this example may provide some general guidance 
for troubleshooting.

```bash
$ heron activate local ExclamationTopology

...

ERROR: Failed to activate topology 'ExclamationTopology'
INFO: Elapsed time: 1.883s.
```
In `~/.herondata/topologies/{cluster}/{role}/{topologyName}/heron-executor.stdout`, there is an error message:
```bash
2016-06-07 14:42:13 Running stmgr-1 process as ./heron-core/bin/heron-stmgr ExclamationTopology \
ExclamationTopology8b1ba199-530a-4425-b903-3f3e5b97d34e ExclamationTopology.defn LOCALMODE \
/Users/{username}/.herondata/repository/state/local stmgr-1 \
container_1_word_2,container_1_exclaim1_1 65424 65428 65427 ./heron-conf/heron_internals.yaml

...

2016-06-07 14:42:13 stmgr-1 exited with status ...
```

Something is wrong with `stmgr-1`. To investigate further, under the working directory,
run the command seen at `heron-executor.stdout` directly:

```bash
$ ./heron-core/bin/heron-stmgr ExclamationTopology \
ExclamationTopology8b1ba199-530a-4425-b903-3f3e5b97d34e ExclamationTopology.defn LOCALMODE \
/Users/{username}/.herondata/repository/state/local stmgr-1 \
container_1_word_2,container_1_exclaim1_1 65424 65428 65427 ./heron-conf/heron_internals.yaml

./heron-core/bin/heron-stmgr: \
error while loading shared libraries: \
libunwind.so.8: cannot open shared object file: No such file or directory
```

This shows `libunwind` might be missing or not installed correctly. Installing `libunwind` and re-running the topology fixes the problem.

### Next Steps

* [Upgrade Storm topologies](../upgrade-storm-to-heron) with simple `pom.xml`
  changes
* [Deploy topologies](../operators/deployment) in clustered, scheduler-driven
  environments (such as on [Aurora](../operators/deployment/schedulers/aurora)
  and [locally](../operators/deployment/schedulers/local))
* [Develop topologies](../concepts/architecture) for Heron
