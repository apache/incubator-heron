---
id: version-0.20.0-incubating-getting-started-troubleshooting-guide
title: Troubleshooting Guide
sidebar_label: Troubleshooting Guide
original_id: getting-started-troubleshooting-guide
---
<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

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

[2016-05-27 12:02:38 -0600] org.apache.heron.common.basics.FileUtils SEVERE: \
Failed to read from file.
java.nio.file.NoSuchFileException: \
/home//.herondata/repository/state/local/pplans/ExclamationTopology

...

[2016-05-27 12:02:38 -0600] org.apache.heron.spi.utils.TMasterUtils SEVERE: \
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

* It is also possible that the host has an issue with resolving localhost.
To check, run the following command in a shell.

    ```bash
    $ python -c "import socket; print socket.gethostbyname(socket.gethostname())"
    Traceback (most recent call last):
      File "<string>", line 1, in <module>
    socket.gaierror: [Errno 8] nodename nor servname provided, or not known
    ```

    If the output looks like a normal IP address, such as `127.0.0.1`,
    you don't have this issue.
    If the output is similar to the above, you need to modify the `/etc/hosts`
    file to correctly resolve localhost, as shown below.

    1. Run the following command, whose output is your computer's hostname.

        ```bash
        $ python -c "import socket; print socket.gethostname()"
        ```

    2. Open the `/etc/hosts` file as superuser and find a line containing

        ```bash
        127.0.0.1	localhost
        ```

    3. Append your hostname after the word "localhost" on the line.
    For example, if your hostname was `tw-heron`, then the line should
    look like the following:

        ```bash
        127.0.0.1   localhost   tw-heron
        ```

    4. Save the file. The change should usually be reflected immediately,
    although rebooting might be necessary depending on your platform.

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
