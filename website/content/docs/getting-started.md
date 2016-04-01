---
date: 2016-02-28T13:10:21-08:00
title: Getting Started
---

Run Topologies Locally using pre-compiled Heron binaries (Mac OSX, Ubuntu >= 14.04, Centos7)

### Step 1 - Download pre-compiled Heron binaries with install scripts

Navigate to [Twitter Heron Releases](https://github.com/twitter/heron/releases)

Download heron-client-install and heron-tools-install for your platform:
Example for Mac OSX (darwin):

**heron-client-install-0.13.1-darwin.sh**

**heron-tools-install-0.13.1-darwin.sh**

Run Install scripts
```bash
$ chmod +x heron-client-install-0.13.1-darwin.sh
$ ./heron-client-install-0.13.1-darwin.sh --user
Uncompressing......
Heron is now installed!
Make sure you have "/Users/USERNAME/bin" in your path.

$ chmod +x heron-tools-install-0.13.1-darwin.sh
$ ./heron-tools-install-0.13.1-darwin.sh --user
Uncompressing......
Heron Tools is now installed!
Make sure you have "/Users/USERNAME/bin" in your path.

```

### Step 2 - Launch an example topology

Launch an example [topology](../concepts/topologies) to **local cluster** using submit:

```bash
$ heron submit local ~/.heron/examples/heron-examples.jar com.twitter.heron.examples.AckingTopology AckingTopology
```

### Step 3 - Start Heron Tracker

Open a new terminal window and launch [heron-tracker](../operators/heron-tracker):
```bash
$ heron-tracker
... Running on port: 8888
```
In local browser, Heron tracker is available on http://localhost:8888


### Step 4 - Start Heron UI

Open a new terminal window and launch UI:
```bash
$ heron-ui
... Running on port: 8889
```
In local browser, Heron UI is available on http://localhost:8889

### Step 5 - Explore activate, deactivate, kill topology commands

```bash
$ heron activate local AckingTopology
$ heron deactivate local AckingTopology
$ heron kill local AckingTopology
```
Explore [managing topologies with Heron CLI](../operators/heron-cli)
and Heron-cli3 syntax:
```bash
usage: heron submit [options] cluster/[role]/[environ] topology-file-name topology-class-name [topology-args]


usage: heron <command> <options> ...

Available commands:
    activate           Activate a topology
    deactivate         Deactivate a topology
    help               Prints help for commands
    kill               Kill a topology
    ps                 List all topologies
    restart            Restart a topology
    submit             Submit a topology
    version            Print version of heron-cli

For detailed documentation, go to http://heron.github.io
```
### Step 6 - Explore other example Topologies

AckingTopology.java  | This is a basic example of a Heron topology with acking enable.

ExclamationTopology.java | This is a basic example of a Heron topology.

MultiSpoutExclamationTopology.java | This is a basic example of a Heron topology.

MultiStageAckingTopology.java | This is three stage topology. Spout emits to bolt to bolt. 

TaskHookTopology.java | This is a basic Task Hook Heron topology.

CustomGroupingTopology.java | This is a basic example of a Heron topology.

ComponentJVMOptionsTopology.java | This is a basic example of a Heron topology.

Example (ExclamationTopology):
```bash
$ heron submit local ~/.heron/examples/heron-examples.jar com.twitter.heron.examples.ExclamationTopology ExclamationTopology
```


### Next Steps - Deploying or Developing

[Deploying Existing Topologies](../operators/deployment/README) in clustered, scheduler-driven environments (Aurora, Mesos, Local)

[Developing Topologies](../concepts/architecture) with the Architecture of Heron

