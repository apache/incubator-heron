---
id: version-0.20.0-incubating-user-manuals-heron-shell
title: Heron Shell
sidebar_label: Heron Shell
original_id: user-manuals-heron-shell
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

Heron shell helps debugging a heron topology. It is an HTTP server that runs as a
separate process in every container.  It exposes many utilities through REST APIs.
These utilities are described below in more details.

The port to connect to heron shell for each container is stored in the physical
plan. Heron tracker picks up this port and connects to shell. See the next
section for more details.

## Shell Utilities

### Log files

This is probably the most useful utility of shell. Since relative paths to log
files are known, these paths are directly embedded in UI through Tracker. This
makes the logs available in the browser. The log files start at the end and
users can scroll up a page at a time, or download them locally.

### Browse

A container is expected to run in a sandbox directory. All the files
under this directory are accessible through shell. This can be used to download
the jars or tars for a topology, or browse through, download, or even view the
files online. Viewing and downloading log files is one special case of browse.

### Pid of a process

Each instance in the topology runs as a separate JVM process. This allows us to
monitor each spout or bolt instance in isolation. To run more sophisticated
operations on the process as mentioned in this list below, we need to know the
process id of the process running as that instance.

### Jstack of a process

This utility runs the `jstack` command on the JVM process that is running an
instance. The result is passed back through the REST API which can be viewed
directly in the browser.

### Jmap of a process

This utility runs `jmap` on the process running one instance to capture a heap
dump. Since a heap dump can be huge, the dump file is created on the host that
is running the container. This file can be downloaded using the info that is
returned as the response to this endpoint. The dumps are also accessible through
the "browse" utility.

### Memory histogram of a process

This utility runs `jmap` with `-histo` option, to output the memory
histogram for the JVM process of an instance. The resulting histogram is passed
back as part of the response, and can be viewed directly in the browser.
