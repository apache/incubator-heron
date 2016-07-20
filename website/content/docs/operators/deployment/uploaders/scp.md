---
title: Setting Up SCP Uploader
---

For small clusters with simple setups that doesn't have a HDFS like file system, the SCP uploader
can be used to manage the package files. This uploader uses the scp linux command to upload the files
to a node accessible by all the worker nodes in the cluster. Then a scheduler like Aurora can
use the scp command again to download the content to each of the worker machines.

SCP Uploader requirements

* SCP uploader requires the scp and ssh utilities installed. Also it is better to have
passwordless ssh configured between the shared node and worker nodes in the cluster.

### SCP Uploader Configuration

You can make Heron use SCP uploader by modifying the `uploader.yaml` config file specific
for the Heron cluster. You'll need to specify the following for each cluster:

* `heron.class.uploader` --- Indicate the uploader class to be loaded. You should set this to
com.twitter.heron.uploader.scp.ScpUploader
* `heron.uploader.scp.command.options` --- Part of the SCP command where you specify the username,
host and key options. i.e "-i ~/.ssh/id_rsa user@localhost"
* `heron.uploader.ssh.command.options` --- Part of the SCP command where you specify the username,
host and key options. i.e "-i ~/.ssh/id_rsa user@localhost"
* `heron.uploader.scp.dir.path` --- The directory to be used to uploading the package.

### Example SCP Uploader Configuration

Below is an example configuration (in `uploader.yaml`) for a SCP uploader:

```yaml
# uploader class for transferring the topology jar/tar files to storage
heron.class.uploader:         com.twitter.heron.uploader.scp.ScpUploader
# This is the scp command options that will be used by the uploader, this has to be customized to
# reflect the user name, hostname and ssh keys if required.
heron.uploader.scp.command.options:   "-i ~/.ssh/id_rsa user@host"
# The ssh command options that will be used to connect to the uploading host to execute
# command such as delete files, make directories.
heron.uploader.ssh.command.options:   "-i ~/.ssh/id_rsa user@host"
# the directory where the file will be uploaded, make sure the user has the necessary permissions
# to upload the file here.
heron.uploader.scp.dir.path:   ${HOME}/heron/repository/${CLUSTER}/${ROLE}/${TOPOLOGY}
```

Below is an example scp command configuration in the `heron.aurora` file.

```bash
fetch_user_package = Process(
  name = 'fetch_user_package',
  cmdline = 'scp -i ~/.ssh/id_rsa user@host:%s %s && tar zxf %s' % (heron_topology_jar_uri, \
      topology_package_file, topology_package_file)
)
```