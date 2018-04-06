---
title: Setting Up Http Uploader
---
When a topology is submitted to Heron, the topology jars will be uploaded to a stable location. 
The submitter will provide this location to the scheduler and it will pass it to the each 
container. Heron can use a Http uploader to upload topology jar distribution to a stable 
Http location.

### Http Uploader Configuration

You can make Heron aware of the Http uploader by modifying the `uploader.yaml` config file specific 
for the Heron cluster. You’ll need to specify the following for each cluster:

* `heron.class.uploader` — Indicate the uploader class to be loaded. You should set this 
to `org.apache.heron.uploader.http.HttpUploader`

* `heron.uploader.http.uri` — Provides the name of the URI where the topology jar should be 
uploaded.

### Example Http Uploader Configuration

Below is an example configuration (in `uploader.yaml`) for a Http uploader:

```yaml
# uploader class for transferring the topology jar/tar files to storage
heron.class.uploader: org.apache.heron.uploader.http.HttpUploader

heron.uploader.http.uri: http://localhost:9000/api/v1/file/upload
```

Also Heron's API server can be used as a file server for the HttpUploader to upload topology 
package/jars as follows:

```
${HOME}/.heron/bin/heron-apiserver 
--cluster standalone 
--base-template standalone 
-D heron.statemgr.connection.string=<zookeeper_host:zookeeper_port> 
-D heron.nomad.scheduler.uri=<scheduler_uri> 
-D heron.class.uploader=org.apache.heron.uploader.http.HttpUploader
--verbose
```

Also Http Server that topology package/jars are uploaded needs to return an URI upon upload 
so that Heron will know the location to download in the future.