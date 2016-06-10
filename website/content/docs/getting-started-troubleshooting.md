---
title: Quick Start Troubleshooting
---

This guide provides basic help for issues frequently encountered when deploying topologies.

### 1. How can I get more debugging information?

Enable the `--verbose` flag to see more debugging information, for example

```bash
heron submit ... ExclamationTopology --verbose        
```

### 2. Why does the topology launch successfully but fail to start?

Even if the topology is submitted successfully, it could still fail to
start some component. For example, TMaster may fail to start due to unfulfilled
dependencies. 

For example, the following message can appear:

```bash
$ heron activate local ExclamationTopology

...

[2016-05-27 12:02:38 -0600] com.twitter.heron.common.basics.FileUtils SEVERE: \
Failed to read from file.
java.nio.file.NoSuchFileException: \
/home//.herondata/repository/state/local/pplans/ExclamationTopology

...

[2016-05-27 12:02:38 -0600] com.twitter.heron.spi.utils.TMasterUtils SEVERE: \
Failed to get physical plan for topology ExclamationTopology

... 

ERROR: Failed to activate topology 'ExclamationTopology'
INFO: Elapsed time: 1.883s.
```

#### What to do

* This file will show if any specific components have failed to start.
    
    ```bash
    ~/.herondata/topologies/{cluster}/{role}/{TopologyName}/heron-executor.stdout
    ```
    
    For example, there may be errors when trying to spawn a Stream Manager process in the file:
    
    ```bash
    Running stmgr-1 process as ./heron-core/bin/heron-stmgr ExclamationTopology \
    ExclamationTopology0a9c6550-7f3d-44fb-97ea-5c779fac6924 ExclamationTopology.defn LOCALMODE \
    /Users/${USERNAME}/.herondata/repository/state/local stmgr-1 \
    container_1_word_2,container_1_exclaim1_1 58106 58110 58109 ./heron-conf/heron_internals.yaml
    2016-06-09 16:20:28:  stdout: 
    2016-06-09 16:20:28:  stderr: error while loading shared libraries: libunwind.so.8: \
    cannot open shared object file: No such file or directory
    ```

    Then fix it correspondingly.
    
### 3. Why does the process fail during runtime? 

If a component (e.g., TMaster or Stream Manager) has failed during runtime, visit the component's logs in
    
```bash
~/.herondata/topologies/{cluster}/{role}/{TopologyName}/log-files/
```

### 4. How to force kill and clean up a topology?

In general, it suffices to run:
        
```bash
heron kill ...
```

If returned error, the topology can still be killed by running 
    `kill pid` to kill all associated running process and `rm -rf ~/.herondata/` 
    to clean up the state.
