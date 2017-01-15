---
title: Setting Up Local File System Uploader
---

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
to `com.twitter.heron.uploader.localfs.LocalFileSystemUploader`

* `heron.uploader.localfs.file.system.directory` --- Provides the name of the directory where
the topology jar should be uploaded. The name of the directory should be unique per cluster
You could use the Heron environment variables `${CLUSTER}` that will be substituted by cluster
name.

### Example Local File System Uploader Configuration

Below is an example configuration (in `uploader.yaml`) for a local file system uploader:

```yaml
# uploader class for transferring the topology jar/tar files to storage
heron.class.uploader: com.twitter.heron.uploader.localfs.LocalFileSystemUploader

# name of the directory to upload topologies for local file system uploader
heron.uploader.localfs.file.system.directory: ${HOME}/.herondata/topologies/${CLUSTER}
```
