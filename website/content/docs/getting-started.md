---
title: Quick Start Guide
description: Run a single-node Heron cluster on your laptop
aliases:
  - /docs/install.html
---

> The current version of Heron is **{{% heronVersion %}}**

The easiest way to get started learning Heron is to install the Heron client tools, which are currently available for:

* [MacOS](#macos-homebrew)
* [Ubuntu >= 14.04](#using-installation-scripts)
* [CentOS](#using-installation-scripts)

For other platforms, you need to build from source. Please refer to the [guide to compiling Heron]
(../developers/compiling/compiling).

## Step 1 --- Download the Heron tools

Heron tools can be installed on [MacOS](#macos-homebrew) using [Homebrew](https://brew.sh) and on Linux using [installation scripts](#using-installation-scripts).

> You can install using [installation scripts](#using-installation-scripts) on MacOS as well.

## MacOS/Homebrew

The easiest way to get started with Heron on MacOS is using [Homebrew](https://brew.sh). There are three tools currently available:

* The Heron command-line client tools:

    ```shell
    $ brew install heron-client
    ```

    This will install both the [`heron`](../operators/heron-cli) and [`heron-explorer`](../operators/heron-explorer) tools.

* The [Heron UI](../operators/heron-ui) dashboard:

    ```shell
    $ brew install heron-ui
    ```

* The [Heron Tracker](../operators/heron-tracker), which powers Heron UI:

    ```shell
    $ brew install heron-tracker
    ```

For this getting started tutorial we recommend installing all three tools.

## Using installation scripts

To install Heron binaries directly, using installation scripts, go to Heron's [releases page](https://github.com/twitter/heron/releases) on GitHub
and see a full listing of Heron releases for each available platform. The installation scripts for Mac OS X (`darwin`), for example, are named
`heron-client-install-{{% heronVersion %}}-darwin.sh` and
`heron-tools-install-{{% heronVersion %}}-darwin.sh`.

Download both the `client` and `tools` installation scripts for your platform either from the releases page or using [wget](https://www.gnu.org/software/wget/).

Here's an example for Ubuntu:

```bash
$ wget https://github.com/twitter/heron/releases/download/{{% heronVersion %}}/heron-client-install-{{% heronVersion %}}-ubuntu.sh
$ wget https://github.com/twitter/heron/releases/download/{{% heronVersion %}}/heron-tools-install-{{% heronVersion %}}-ubuntu.sh
```

Once you've downloaded the scripts, make the scripts executable using [chmod](https://en.wikipedia.org/wiki/Chmod):

```bash
$ chmod +x heron-*.sh
```

> Both installation scripts will install executables in the `~/bin` folder. You should add that folder to your `PATH` using `export PATH=~/bin:$PATH`.

Now run the [Heron client](../operators/heron-cli) installation script with the `--user` flag set:

```bash
$ ./heron-client-install-{{% heronVersion %}}--PLATFORM.sh --user
Heron client installer
----------------------

Uncompressing......
Heron is now installed!
```

Now run the script for Heron tools (again setting the `--user` flag):

```bash
$ ./heron-tools-install-{{% heronVersion %}}-PLATFORM.sh --user
Heron tools installer
---------------------

Uncompressing......
Heron Tools is now installed!
```

To check that Heron is successfully installed, run `heron version`:

```bash
$ heron version
heron.build.version : {{% heronVersion %}}
heron.build.time : Sat Aug  6 12:35:47 PDT {{% currentYear %}}
heron.build.timestamp : 1470512147000
heron.build.host : ${HOSTNAME}
heron.build.user : ${USERNAME}
heron.build.git.revision : 26bb4096130a05f9799510bbce6c37a69a7342ef
heron.build.git.status : Clean
```

## Step 2 --- Launch an example topology

> #### Note for MacOS users

> If you want to run topologies locally on MacOS, you may need to add your
> hostname to your `/etc/hosts` file under `localhost`. Here's an example line:
> `127.0.0.1 localhost My-Mac-Laptop.local`. You can fetch your hostname by simply
> running `hostname` in your shell.

If you set the `--user` flag when running the installation scripts, some example
topologies will be installed in your `~/.heron/examples` directory. You can
launch an example [topology](../concepts/topologies) locally (on your machine)
using the [Heron CLI tool](../operators/heron-cli):

```bash
# Submit ExclamationTopology locally in deactivated mode.
$ heron submit local \
~/.heron/examples/heron-examples.jar \
com.twitter.heron.examples.ExclamationTopology \
ExclamationTopology \
--deploy-deactivated
```

The output should look something like this:

```bash
INFO: Launching topology 'ExclamationTopology'

...

INFO: Topology 'ExclamationTopology' launched successfully
INFO: Elapsed time: 3.409s.
```

This will *submit* the topology to your locally running Heron cluster but it
won't *activate* the topology because the `--deploy-deactivated` flag was set. Activating the topology will be explored in step 5 below.

Note that the output shows whether the topology has been launched successfully as well the working directory for the topology.

To check what's under the working directory, run:
```bash
$ ls -al ~/.herondata/topologies/local/${ROLE}/ExclamationTopology
-rw-r--r--   1 username  role     2299 Jun  7 16:44 ExclamationTopology.defn
-rw-r--r--   1 username  role        5 Jun  7 16:44 container_1_exclaim1_1.pid
-rw-r--r--   1 username  role        5 Jun  7 16:44 container_1_word_2.pid
drwxr-xr-x  11 username  role      374 Jun  7 16:44 heron-conf
drwxr-xr-x   4 username  role      136 Dec 31  1969 heron-core
-rwxr-xr-x   1 username  role  2182564 Dec 31  1969 heron-examples.jar
-rw-r--r--   1 username  role        5 Jun  7 16:44 heron-executor-0.pid
-rw-r--r--   1 username  role        0 Jun  6 13:33 heron-executor.stderr
-rw-r--r--   1 username  role    17775 Jun  7 16:44 heron-executor.stdout
-rw-r--r--   1 username  role        5 Jun  7 16:44 heron-shell-0.pid
-rw-r--r--   1 username  role        5 Jun  7 16:44 heron-tmaster.pid
drwxr-xr-x  25 username  role      850 Jun  7 16:44 log-files
-r--r--r--   1 username  role     4506 Jun  8 12:05 metrics.json.metricsmgr-0.0
-rw-r--r--   1 username  role        5 Jun  7 16:44 metricsmgr-0.pid
-r-xr-xr-x   1 username  role      279 Dec 31  1969 release.yaml
-rw-r--r--   1 username  role        5 Jun  7 16:44 stmgr-1.pid
```

All instances' log files can be found in `log-files` under the working directory:

```bash
$ ls -al ~/.herondata/topologies/local/${ROLE}/ExclamationTopology/log-files
total 1018440
-rw-r--r--   1 username  role   94145427 Jun  8 12:06 container_1_exclaim1_1.log.0
-rw-r--r--   1 username  role   75675435 Jun  7 16:44 container_1_word_2.log.0
-rw-r--r--   1 username  role  187401024 Jun  8 12:06 gc.container_1_exclaim1_1.log
-rw-r--r--   1 username  role  136318451 Jun  8 12:06 gc.container_1_word_2.log
-rw-r--r--   1 username  role      11039 Jun  8 11:16 gc.metricsmgr.log
-rw-r--r--   1 username  role        300 Jun  7 16:44 heron-shell.log
-rw-r--r--   1 username  role      29631 Jun  7 16:44 heron-ExclamationTopology-scheduler.log.0
-rw-r--r--   1 username  role    2382215 Jun  7 15:16 heron-stmgr-stmgr-1.username.log.INFO
-rw-r--r--   1 username  role       5976 Jun  7 16:44 heron-tmaster-ExclamationTopology2da9ee6b-c919-4e59-8cb0-20a865f6fd7e.username.log.INFO
-rw-r--r--   1 username  role   12023368 Jun  8 12:06 metricsmgr-0.log.0

```

## Step 3 --- Start Heron Tracker

The [Heron Tracker](../operators/heron-tracker) is a web service that
continuously gathers information about your Heron cluster. You can launch the
tracker by running the `heron-tracker` command (which is already installed):

```bash
$ heron-tracker
... Running on port: 8888
... Using config file: $HOME/.herontools/conf/heron_tracker.yaml
```

You can reach Heron Tracker in your browser at [http://localhost:8888](http://localhost:8888)
and see something like the following upon successful submission of the topology:
![alt tag](/img/heron-tracker.png)

To explore Heron Tracker, please refer to [Heron Tracker Rest API](../operators/heron-tracker-api)

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
and see something like this upon successful submission of the topology:
![alt tag](/img/heron-ui.png)

To play with Heron UI, please refer to [Heron UI Usage Guide](../developers/ui-guide)
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
  --config-path (a string; path to cluster config; default: "$HOME/.heron/conf")
  --config-property (key=value; a config key and its value; default: [])
  --deploy-deactivated (a boolean; default: "false")
  -D DEFINE             Define a system property to pass to java -D when
                        running main.
  --verbose (a boolean; default: "false")
```

## Step 6 --- Explore other example topologies

The source code for the example topologies can be found
[on
GitHub]({{% githubMaster %}}/examples/src/java/com/twitter/heron/examples).
The included example topologies:

* `AckingTopology.java` --- A topology with acking enabled.
* `ComponentJVMOptionsTopology.java` --- A topology that supplies JVM options
  for each component.
* `CustomGroupingTopology.java` --- A topology that implements custom grouping.
* `ExclamationTopology.java` --- A spout that emits random words to a bolt that
  then adds an exclamation mark.
* `MultiSpoutExclamationTopology.java` --- a topology with multiple spouts.
* `MultiStageAckingTopology.java` --- A three-stage topology. A spout emits to a
  bolt that then feeds to another bolt.
* `TaskHookTopology.java` --- A topology that uses a task hook to subscribe to
   event notifications.

## Troubleshooting
In case of any issues, please refer to [Quick Start Troubleshooting](../getting-started-troubleshooting).

### Next Steps

* [Migrate Storm topologies](../migrate-storm-to-heron) to Heron with simple `pom.xml`
  changes
* [Deploy topologies](../operators/deployment) in clustered, scheduler-driven
  environments (such as on [Aurora](../operators/deployment/schedulers/aurora)
  and [locally](../operators/deployment/schedulers/local))
* [Develop topologies](../concepts/architecture) for Heron
