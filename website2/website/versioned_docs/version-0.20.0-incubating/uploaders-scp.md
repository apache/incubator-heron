---
id: version-0.20.0-incubating-uploaders-scp
title: Secure Copy (SCP)
sidebar_label: Secure Copy (SCP)
original_id: uploaders-scp
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

For small clusters with simple setups that doesn't have a HDFS like file system, the SCP uploader
can be used to manage the package files. This uploader uses the `scp` linux command to upload the
files to a node accessible by all the worker nodes in the cluster. Then a scheduler like Aurora can
use the `scp` command again to download the content to each of the worker machines.

SCP Uploader requirements

* SCP uploader requires the `scp` and `ssh` linux utilities installed. Also it is better to have
passwordless ssh configured between the shared node and worker nodes in the cluster.

### SCP Uploader Configuration

You can make Heron use SCP uploader by modifying the `uploader.yaml` config file specific
for the Heron cluster. You'll need to specify the following for each cluster:

* `heron.class.uploader` --- Indicate the uploader class to be loaded. You should set this to
org.apache.heron.uploader.scp.ScpUploader
* `heron.uploader.scp.command.options` --- Part of the SCP command where you specify custom options.
i.e "-i ~/.ssh/id_rsa"
* `heron.uploader.scp.command.connection` --- The user name and host pair to be used by the SCP command.
i.e "user@host"
* `heron.uploader.ssh.command.options` --- Part of the SSH command where you specify custom options.
i.e "-i ~/.ssh/id_rsa"
* `heron.uploader.ssh.command.connection` --- The user name and host pair to be used by the SSH command.
i.e "user@host"
* `heron.uploader.scp.dir.path` --- The directory to be used to uploading the package.

### Example SCP Uploader Configuration

Below is an example configuration (in `uploader.yaml`) for a SCP uploader:

```yaml
# uploader class for transferring the topology jar/tar files to storage
heron.class.uploader:         org.apache.heron.uploader.scp.ScpUploader
# This is the scp command options that will be used by the uploader, this can be used to
# specify custom options such as the location of ssh keys.
heron.uploader.scp.command.options:   "-i ~/.ssh/id_rsa"
# The scp connection string sets the remote user name and host used by the uploader.
heron.uploader.scp.command.connection:   "user@host"

# The ssh command options that will be used when connecting to the uploading host to execute
# command such as delete files, make directories.
heron.uploader.ssh.command.options:   "-i ~/.ssh/id_rsa"
# The ssh connection string sets the remote user name and host used by the uploader.
heron.uploader.ssh.command.connection:   "user@host"

# the directory where the file will be uploaded, make sure the user has the necessary permissions
# to upload the file here.
heron.uploader.scp.dir.path:   ${HOME}/heron/repository/${CLUSTER}/${ROLE}/${TOPOLOGY}
```

The uploader will use SSH to create the entire directory structure specified in `heron.uploader.scp.dir.path` 
by running `mkdir -p` before using SCP to upload the topology package.


Below is an example `scp` command configuration in the `heron.aurora` file. The cmdline is run by every node
in the cluster to **download** the topology package.

```bash
fetch_user_package = Process(
  name = 'fetch_user_package',
  cmdline = 'scp -i ~/.ssh/id_rsa user@host:%s %s && tar zxf %s' % (heron_topology_jar_uri, \
      topology_package_file, topology_package_file)
)
```