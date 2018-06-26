---
title: Running Heron via Raw Execs on Nomad
---

Below are instructions on how to to run Heron on Nomad via raw execs.  In this mode, Heron executors will run as raw processes on the host machines. 

The advantages of this mode is that it is incredibly lightweight and likely do not require sudo privileges to setup and run.  However in this mode, the setup procedure may be a little more complex compared to running via docker since there are more things to consider.  Also in resource allocation is considered but not enforced.

## Requirements

When setting up your Nomad cluster, the following are required:

* The [Heron CLI tool](../../../heron-cli) must be installed on each machine used to deploy Heron topologies
* Python 2.7, Java 7 or 8, and [curl](https://curl.haxx.se/) must be installed on every machine in the cluster
* A [ZooKeeper cluster](https://zookeeper.apache.org)

## Configuring Heron settings

Before running Heron via Nomad, you'll need to configure some settings. Once you've [installed Heron](../../../../getting-started), all of the configurations you'll need to modify will be in the `~/.heron/conf/nomad` diredctory.

First, make sure that the `heron.nomad.driver` is set to "raw_exec" in `~/.heron/conf/nomad/scheduler.yaml` e.g.

```yaml
heron.nomad.driver: "raw_exec"
```

You'll need to use a topology uploader to deploy topology packages to nodes in your cluster. You can use one of the following uploaders:

* The HTTP uploader in conjunction with Heron's [API server](../../../heron-api-server). The Heron API server acts like a file server to which users can upload topology packages. The API server distributes the packages, along with the Heron core package, to the relevant machines. You can also use the API server to submit your Heron topology to Nomad (described [below](#deploying-with-the-api-server)) <!-- TODO: link to upcoming HTTP uploader documentation -->
* [Amazon S3](../../uploaders/s3). Please note that the S3 uploader requires an AWS account.
* [SCP](../../uploaders/scp). Please note that the SCP uploader requires SSH access to nodes in the cluster.

You can modify the `heron.class.uploader` parameter in `~/.heron/conf/nomad/uploader.yaml` to choose an uploader.

In addition, you must update the `heron.statemgr.connection.string` parameter in the `statemgr.yaml` file in `~/.heron/conf/nomad` to your ZooKeeper connection string. Here's an example:

```yaml
heron.statemgr.connection.string: 127.0.0.1:2181
```

Then, update the `heron.nomad.scheduler.uri` parameter in `scheduler.yaml` to the URL of the Nomad server to which you'll be submitting jobs. Here's an example:

```yaml
heron.nomad.scheduler.uri: http://127.0.0.1:4646
```

You may also want to configure where Heron will store files on your machine if you're running Nomad locally (in `scheduler.yaml`). Here's an example:

```yaml
heron.scheduler.local.working.directory: ${HOME}/.herondata/topologies/${CLUSTER}/${ROLE}/${TOPOLOGY_ID}
```

> Heron uses string interpolation to fill in the missing values for `CLUSTER`, `ROLE`, etc.

## Distributing Heron core

The Heron core package needs to be made available for every machine in the cluster to download. You'll need to provide a URI for the Heron core package. Here are the currently supported protocols:

* `file://` (local FS)
* `http://` (HTTP)

You can do this in one of several ways:

* Use the Heron API server to distribute `heron-core.tar.gz` (see [here](../../heron-api-server) for more info)
* Copy `heron-core.tar.gz` onto every node in the cluster
* Mount a network drive to every machine in the cluster that contains 
* Upload `heron-core.tar.gz` to an S3 bucket and expose an HTTP endpoint
* Upload `heron-core.tar.gz` to be hosted on a file server and expose an HTTP endpoint

> A copy of `heron-core.tar.gz` is located at `~/.heron/dist/heron-core.tar.gz` on the machine on which you installed the Heron CLI.

You'll need to set the URL for `heron-core.tar.gz` in the `client.yaml` configuration file in `~/.heron/conf/nomad`. Here are some examples:

```yaml
# local filesystem
heron.package.core.uri: file:///path/to/heron/heron-core.tar.gz

# from a web server
heron.package.core.uri: http://some.webserver.io/heron-core.tar.gz
```

## Submitting Heron topologies to the Nomad cluster

You can submit Heron topologies to a Nomad cluster via the [Heron CLI tool](../../../heron-cli):

```bash
$ heron submit nomad \
  <topology package path> \
  <topology classpath> \
  <topology CLI args>
```

Here's an example:

```bash
$ heron submit nomad \
  ~/.heron/examples/heron-streamlet-examples.jar \           # Package path
  org.apache.heron.examples.api.WindowedWordCountTopology \ # Topology classpath
  windowed-word-count                                        # Args passed to topology
```

## Deploying with the API server

The advantage of running the [Heron API Server](../../../heron-api-server) is that it can act as a file server to help you distribute topology package files and submit jobs to Nomad, so that you don't need to modify the configuration files mentioned above.  By using Heron’s API Server, you can set configurations such as the URI of ZooKeeper and the Nomad server once and not need to configure each machine from which you want to submit Heron topologies.

## Running the API server

You can run the Heron API server on any machine that can be reached by machines in your Nomad cluster via HTTP. Here's a command you can use to run the API server:

```bash
$ ~/.heron/bin/heron-apiserver \
  --cluster nomad \
  --base-template nomad \
  -D heron.statemgr.connection.string=<ZooKeeper URI> \
  -D heron.nomad.scheduler.uri=<Nomad URI> \
  -D heron.class.uploader=org.apache.heron.uploader.http.HttpUploader \
  --verbose
```

You can also run the API server in Nomad itself, but you will need to have a local copy of the Heron API server executable on every machine in the cluster. Here's an example Nomad job for the API server:

```hcl
job "apiserver" {
  datacenters = ["dc1"]
  type = "service"
  group "apiserver" {
    count = 1
    task "apiserver" {
      driver = "raw_exec"
      config {
        command = <heron_apiserver_executable>
        args = [
        "--cluster", "nomad",
        "--base-template", "nomad",
        "-D", "heron.statemgr.connection.string=<zookeeper_uri>",
        "-D", "heron.nomad.scheduler.uri=<scheduler_uri>",
        "-D", "heron.class.uploader=org.apache.heron.uploader.http.HttpUploader",
        "--verbose"]
      }
      resources {
        cpu    = 500 # 500 MHz
        memory = 256 # 256MB
      }
    }
  }
}
```

Make sure to replace the following:

* `<heron_apiserver_executable>` --- The local path to where the [Heron API server](../../../heron-api-server) executable is located (usually `~/.heron/bin/heron-apiserver`)
* `<zookeeper_uri>` --- The URI for your ZooKeeper cluster
* `<scheduler_uri>` --- The URI for your Nomad server

## Using the Heron API server to distribute Heron topology packages

Heron users can upload their Heron topology packages to the Heron API server using the HTTP uploader by modifying the `uploader.yaml` file to including the following:

```yaml
# uploader class for transferring the topology jar/tar files to storage
heron.class.uploader:    org.apache.heron.uploader.http.HttpUploader
heron.uploader.http.uri: http://localhost:9000/api/v1/file/upload
```

The [Heron CLI](../../../heron-cli) will take care of the upload. When the topology is starting up, the topology package will be automatically downloaded from the API server.

## Using the API server to distribute the Heron core package

Heron users can use the Heron API server to distribute the Heron core package. When running the API server, just add this argument:

```bash
--heron-core-package-path <path to Heron core>
```

Here's an example:

```bash
$ ~/.heron/bin/heron-apiserver \
  --cluster nomad \
  --base-template nomad \
  --download-hostname 127.0.0.1 \
  --heron-core-package-path ~/.heron/dist/heron-core.tar.gz \
  -D heron.statemgr.connection.string=127.0.0.1:2181 \
  -D heron.nomad.scheduler.uri=127.0.0.1:4647 \
  -D heron.class.uploader=org.apache.heron.uploader.http.HttpUploader \
  --verbose
```

Then change the `client.yaml` file in `~/.heron/conf/nomad` to the following:

```yaml
heron.package.use_core_uri: true
heron.package.core.uri:     http://localhost:9000/api/v1/file/download/core
```

## Using the API server to submit Heron topologies

Users can submit topologies using the [Heron CLI](../../../heron-cli) by specifying a service URL to the API server. Here's the format of that command:

```bash
$ heron submit nomad \
  --service-url=<Heron API server URL> \
  <topology package path> \
  <topology classpath> \
  <topology args>
```

Here's an example:

```bash
$ heron submit nomad \
  --service-url=http://localhost:9000 \
  ~/.heron/examples/heron-streamlet-examples.jar \
  org.apache.heron.examples.api.WindowedWordCountTopology \
  windowed-word-count
```

## Integration with Consul for metrics
Each Heron executor part of a Heron topology serves metrics out of a port randomly generated by Nomad.  Thus, Consul is needed for service discovery for users to determine which port the Heron executor is serving the metrics out of.
Every Heron executor will automatically register itself as a service with Consul given that there is a Consul cluster running. The port Heron will be serving metrics will be registered with Consul.

The service will be registered with the name with the following format:

```yaml
metrics-heron-<TOPOLOGY_NAME>-<CONTAINER_INDEX>
```

Each heron executor registered with Consul will be tagged with

```yaml
<TOPOLOGY_NAME>-<CONTAINER_INDEX>
```

To add additional tags, please add specify them in a comma delimited list via

```yaml
heron.nomad.metrics.service.additional.tags
```

in `scheduler.yaml`. For example:

```yaml
heron.nomad.metrics.service.additional.tags: "prometheus,metrics,heron"
```

Users can then configure Prometheus to scrape metrics for each Heron executor based on these tags
