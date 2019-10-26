---
id: version-0.20.0-incubating-uploaders-local-fs
title: Local File System
sidebar_label: Local File System
original_id: uploaders-local-fs
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

When you submit a topology to Heron, the topology jars will be uploaded to a
stable location. The submitter will provide this location to the scheduler and
it will pass it to the executor each container. Heron can use a local file
system as a stable storage for topology jar distribution.

There are a few things you should be aware of local file system uploader:

* Local file system uploader is mainly used in conjunction with local scheduler.

* It is ideal, if you want to run Heron in a single server, laptop or an edge device.

* Useful for Heron developers for local testing of the components.

### Local File System Uploader Configuration

You can make Heron aware of the local file system uploader by modifying the
`uploader.yaml` config file specific for the Heron cluster. You'll need to specify
the following for each cluster:

* `heron.class.uploader` --- Indicate the uploader class to be loaded. You should set this
to `org.apache.heron.uploader.localfs.LocalFileSystemUploader`

* `heron.uploader.localfs.file.system.directory` --- Provides the name of the directory where
the topology jar should be uploaded. The name of the directory should be unique per cluster
You could use the Heron environment variables `${CLUSTER}` that will be substituted by cluster
name. If this is not set, `${HOME}/.herondata/repository/${CLUSTER}/${ROLE}/${TOPOLOGY}` will be set as default.

### Example Local File System Uploader Configuration

Below is an example configuration (in `uploader.yaml`) for a local file system uploader:

```yaml
# uploader class for transferring the topology jar/tar files to storage
heron.class.uploader: org.apache.heron.uploader.localfs.LocalFileSystemUploader

# name of the directory to upload topologies for local file system uploader
heron.uploader.localfs.file.system.directory: ${HOME}/.herondata/topologies/${CLUSTER}
```
