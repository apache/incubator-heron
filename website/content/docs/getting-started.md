---
date: 2016-02-28T13:10:21-08:00
title: Getting Started
---

Run Topologies using pre-compiled Heron binaries

### Step 1 - Download pre-compiled Heron binaries

Navigate to home directory, download Heron tar.gz, untar
TODO ADD LOCATION 
```bash
$ cd ~/bin
$ wget TODO-LOCATION/heron.tar.gz && wget TODO-LOCATION/heron-tools.tar.gz
$ tar xzvf heron.tar.gz && tar xzvf heron-tools.tar.gz
$ ls -al

heron-cl3 heron-tracker heron-ui
```

### Step 2 - Launch an example topology

Launch an example [topology](../concepts/topologies) to **local cluster** using submit:

```bash
$ heron-cli3 submit local ~/.heron/examples/heron-examples.jar com.twitter.heron.examples.AckingTopology AckingTopology
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
$ heron-cli3 activate local AckingTopology
$ heron-cli3 deactivate local AckingTopology
$ heron-cli3 kill local AckingTopology
```
Explore [managing topologies with Heron CLI](../operators/heron-cli)
and Heron-cli3 syntax:
```bash
usage: heron-cli3 submit [options] cluster/[role]/[environ] topology-file-name topology-class-name [topology-args]


usage: heron-cli3 <command> [command-options] ...
Available commands:
    activate            Activate a topology
    classpath           Print class path of heron-cli
    deactivate          Deactivate a topology
    help                Prints help for commands
    kill                Kill a topology
    restart             Restart a topology
    submit              Submit a topology
    version             Print version of heron-cli
```

