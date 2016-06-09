---
title: Troubleshooting Guide
---

### Overview

This guide provides basic help for issues frequently encountered from [Getting Started page](../getting-started).

* [Frequently seen issues](#frequent)
* [A troubleshooting example](#example)

<a name="frequent"></a>
## Frequently seen issues

#### Topology does not launch

The following message will appear:

```bash
$ heron submit ... ExclamationTopology

...

ERROR: Failed to launch topology {TopologyName} because... Bailing out...
INFO: Elapsed time: 4.146s.
```

#### Topology launches but does not start

Even if the topology is submitted successfully, it could still fail to
start some component. For example, `stmgr` may fail to start due to unfulfilled
dependencies. 

The following message will appear:

```bash
$ heron activate local ExclamationTopology

...

ERROR: Failed to activate topology 'ExclamationTopology'
INFO: Elapsed time: 1.883s.
```

#### What to do

1. Go to

        ~/.herondata/topologies/{cluster}/{role}/{TopologyName}/heron-executor.stdout

    and see if TMaster or any Stream Managers have failed, or if any specific commands have failed.
    
2. If a specific command has failed, try running it directly in the terminal.
   If TMaster or a Stream Manager has failed, visit that component's logs, 
   which can be found in
    
        ~/.herondata/topologies/{cluster}/{role}/{TopologyName}/log-files/

3. In general, killing the topology via 
        
        heron kill 

    and resubmitting it via
     
        heron submit

    might also help. 
    If `heron kill` returned error, the topology can still be killed by running 
    `killall` to kill associated running process and `rm -rf ~/.herondata/` 
    to clean up the state.

<a name="example"></a>

## A troubleshooting example

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

This shows `libunwind` might be missing or not installed correctly. Installing 
`libunwind` and re-running the topology fixes the problem.
