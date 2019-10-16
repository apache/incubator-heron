---
id: version-0.20.0-incubating-uploaders-hdfs
title: HDFS
sidebar_label: HDFS
original_id: uploaders-hdfs
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

With Heron, you have the option to use HDFS as stable storage for user submitted
topology jars. Since HDFS replicates the data, it provides a scalable
mechanism for distributing the user topology jars. This is desirable when
the job runs in a distributed cluster and requires several hundred containers to
run.

There are a few things you should be aware of HDFS uploader:

* It requires hadoop client be installed in the machine where the topology is being submitted

### HDFS Uploader Configuration

You can make Heron use HDFS uploader by modifying the `uploader.yaml` config file specific
for the Heron cluster. You'll need to specify the following for each cluster:

* `heron.class.uploader` --- Indicate the uploader class to be loaded. You should set this
to `org.apache.heron.uploader.hdfs.HdfsUploader`

* `heron.uploader.hdfs.config.directory` --- Specifies the directory of the config files
for hadoop. This is used by hadoop client to upload the topology jar

* `heron.uploader.hdfs.topologies.directory.uri` --- URI of the directory name for uploading
topology jars. The name of the directory should be unique per cluster, if they are sharing the
storage. In those cases, you could use the Heron environment variable `${CLUSTER}` that will be
substituted by cluster name for distinction.

### Example HDFS Uploader Configuration

Below is an example configuration (in `uploader.yaml`) for a HDFS uploader:

```yaml
# uploader class for transferring the topology jar/tar files to storage
heron.class.uploader: org.apache.heron.uploader.hdfs.HdfsUploader

# Directory of config files for hadoop client to read from
heron.uploader.hdfs.config.directory: /home/hadoop/hadoop

# name of the directory to upload topologies for HDFS uploader
heron.uploader.hdfs.topologies.directory.uri: hdfs://heron/topologies/${CLUSTER}
```
