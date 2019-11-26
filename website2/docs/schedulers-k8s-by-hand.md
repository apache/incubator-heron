---
id: schedulers-k8s-by-hand
title: Kubernetes by hand
sidebar_label:  Kubernetes by hand
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

> This document shows you how to install Heron on Kubernetes in a step-by-step, "by hand" fashion. An easier way to install Heron on Kubernetes is to use the [Helm](https://helm.sh) package manager. For instructions on doing so, see [Heron on Kubernetes with Helm](schedulers-k8s-with-helm)).

Heron supports deployment on [Kubernetes](https://kubernetes.io/) (sometimes called **k8s**). Heron deployments on Kubernetes use Docker as the containerization format for Heron topologies and use the Kubernetes API for scheduling.

You can use Heron on Kubernetes in multiple environments:

* Locally using [Minikube](#minikube)
* In the cloud on [Google Container Engine](#google-container-engine) (GKE)
* In [any other](#general-kubernetes-clusters) Kubernetes cluster

## Requirements

In order to run Heron on Kubernetes, you will need:

* A Kubernetes cluster with at least 3 nodes (unless you're running locally on [Minikube](#minikube))
* The [`kubectl`](https://kubernetes.io/docs/tasks/tools/install-kubectl/) CLI tool installed and set up to communicate with your cluster
* The [`heron`](getting-started-local-single-node) CLI tool

Any additional requirements will depend on where you're running Heron on Kubernetes.

## How Heron on Kubernetes Works

When deploying to Kubernetes, each Heron container is deployed as a Kubernetes
[pod](https://kubernetes.io/docs/concepts/workloads/pods/pod/) inside of a Docker container. If there
are 20 containers that are going to be deployed with a topoology, for example, then there will be 20 pods
deployed to your Kubernetes cluster for that topology.

## Minikube

[Minikube](https://kubernetes.io/docs/getting-started-guides/minikube/) enables you to run a Kubernetes cluster locally on a single machine.

### Requirements

To run Heron on Minikube you'll need to [install Minikube](https://kubernetes.io/docs/getting-started-guides/minikube/#installation) in addition to the other requirements listed [above](#requirements).

### Starting Minikube

First you'll need to start up Minikube using the `minikube start` command. We recommend starting Minikube with:

* at least 7 GB of memory
* 5 CPUs
* 20 GB of storage

This command will accomplish precisely that:

```bash
$ minikube start \
  --memory=7168 \
  --cpus=5 \
  --disk-size=20G
```

### Starting components

There are a variety of Heron components that you'll need to start up separately *and in order*. Make sure that the necessary pods are up and in the `RUNNING` state before moving on to the next step. You can track the progress of the pods using this command:

```bash
$ kubectl get pods -w
```

#### ZooKeeper

Heron uses [ZooKeeper](https://zookeeper.apache.org) for a variety of coordination- and configuration-related tasks. To start up ZooKeeper on Minikube:

```bash
$ kubectl create -f https://raw.githubusercontent.com/apache/incubator-heron/master/deploy/kubernetes/minikube/zookeeper.yaml
```

#### BookKeeper

When running Heron on Kubernetes, [Apache BookKeeper](https://bookkeeper.apache.org) is used for things like topology artifact storage. You can start up BookKeeper using this command:

```bash
$ kubectl create -f https://raw.githubusercontent.com/apache/incubator-heron/master/deploy/kubernetes/minikube/bookkeeper.yaml
```

#### Heron tools

The so-called "Heron tools" include the [Heron UI](user-manuals-heron-ui) and the [Heron Tracker](user-manuals-heron-tracker-runbook). To start up the Heron tools:

```bash
$ kubectl create -f https://raw.githubusercontent.com/apache/incubator-heron/master/deploy/kubernetes/minikube/tools.yaml
```

#### Heron API server

The Heron API server is the endpoint that the Heron CLI client uses to interact with the other components of Heron. To start up the Heron API server on Minikube:

```bash
$ kubectl create -f https://raw.githubusercontent.com/apache/incubator-heron/master/deploy/kubernetes/minikube/apiserver.yaml
```

### Managing topologies

Once all of the [components](#starting-components) have been successfully started up, you need to open up a proxy port to your Minikube Kubernetes cluster using the [`kubectl proxy`](https://kubernetes.io/docs/tasks/access-kubernetes-api/http-proxy-access-api/) command:

```bash
$ kubectl proxy -p 8001
```

> Note: All of the following Kubernetes specific urls are valid with the Kubernetes 1.10.0 release.

Now, verify that the Heron API server running on Minikube is available using curl:

```bash
$ curl http://localhost:8001/api/v1/namespaces/default/services/heron-apiserver:9000/proxy/api/v1/version
```

You should get a JSON response like this:

```json
{
  "heron.build.git.revision" : "ddbb98bbf173fb082c6fd575caaa35205abe34df",
  "heron.build.git.status" : "Clean",
  "heron.build.host" : "ci-server-01",
  "heron.build.time" : "Sat Mar 31 09:27:19 UTC 2018",
  "heron.build.timestamp" : "1522488439000",
  "heron.build.user" : "release-agent",
  "heron.build.version" : "0.17.8"
}
```

Success! You can now manage Heron topologies on your Minikube Kubernetes installation. To submit an example topology to the cluster:

```bash
$ heron submit kubernetes \
  --service-url=http://localhost:8001/api/v1/namespaces/default/services/heron-apiserver:9000/proxy \
  ~/.heron/examples/heron-api-examples.jar \
  org.apache.heron.examples.api.AckingTopology acking
```

You can also track the progress of the Kubernetes pods that make up the topology. When you run `kubectl get pods` you should see pods with names like `acking-0` and `acking-1`.

Another option is to set the service URL for Heron using the `heron config` command:

```bash
$ heron config kubernetes set service_url \
  http://localhost:8001/api/v1/namespaces/default/services/heron-apiserver:9000/proxy
```

That would enable you to manage topologies without setting the `--service-url` flag.

### Heron UI

The [Heron UI](user-manuals-heron-ui) is an in-browser dashboard that you can use to monitor your Heron [topologies](heron-topology-concepts). It should already be running in Minikube.

You can access [Heron UI](user-manuals-heron-ui) in your browser by navigating to http://localhost:8001/api/v1/namespaces/default/services/heron-ui:8889/proxy/topologies.

## Google Container Engine

You can use [Google Container Engine](https://cloud.google.com/container-engine/) (GKE) to run Kubernetes clusters on [Google Cloud Platform](https://cloud.google.com/).

### Requirements

To run Heron on GKE, you'll need to create a Kubernetes cluster with at least three nodes. This command would create a three-node cluster in your default Google Cloud Platform zone and project:

```bash
$ gcloud container clusters create heron-gke-cluster \
  --machine-type=n1-standard-4 \
  --num-nodes=3
```

You can specify a non-default zone and/or project using the `--zone` and `--project` flags, respectively.

Once the cluster is up and running, enable your local `kubectl` to interact with the cluster by fetching your GKE cluster's credentials:

```bash
$ gcloud container clusters get-credentials heron-gke-cluster
Fetching cluster endpoint and auth data.
kubeconfig entry generated for heron-gke-cluster.
```

Finally, you need to create a Kubernetes [secret](https://kubernetes.io/docs/concepts/configuration/secret) that specifies the Cloud Platform connection credentials for your service account. First, download your Cloud Platform credentials as a JSON file, say `key.json`. This command will download your credentials:

```bash
$ gcloud iam service-accounts create key.json \
  --iam-account=YOUR-ACCOUNT
```

### Topology artifact storage

Heron on Google Container Engine supports two static file storage options for topology artifacts:

* [Google Cloud Storage](#google-cloud-storage-setup)
* [BookKeeper](#bookkeeper-setup)

#### Google Cloud Storage setup

If you're running Heron on GKE, you can use either [Google Cloud Storage](https://cloud.google.com/storage/) or [Apache BookKeeper](https://bookkeeper.apache.org) for topology artifact storage.

> If you'd like to use BookKeeper instead of Google Cloud Storage, skip to the [BookKeeper](#bookkeeper-setup) section below.

To use Google Cloud Storage for artifact storage, you'll need to create a [Google Cloud Storage](https://cloud.google.com/storage/) bucket. Here's an example bucket creation command using [`gsutil`'](https://cloud.google.com/storage/docs/gsutil):

```bash
$ gsutil mb gs://my-heron-bucket
```

Cloud Storage bucket names must be globally unique, so make sure to choose a bucket name carefully. Once you've created a bucket, you need to create a Kubernetes [ConfigMap](https://kubernetes.io/docs/tasks/configure-pod-container/configmap/) that specifies the bucket name. Here's an example:

```bash
$ kubectl create configmap heron-apiserver-config \
  --from-literal=gcs.bucket=BUCKET-NAME
```

> You can list your current service accounts using the `gcloud iam service-accounts list` command.

Then you can create the secret like this:

```bash
$ kubectl create secret generic heron-gcs-key \
  --from-file=key.json=key.json
```

Once you've created a bucket, a `ConfigMap`, and a secret, you can move on to [starting up](#starting-components) the various components of your Heron installation.

### Starting components

There are a variety of Heron components that you'll need to start up separately *and in order*. Make sure that the necessary pods are up and in the `RUNNING` state before moving on to the next step. You can track the progress of the pods using this command:

```bash
$ kubectl get pods -w
```

#### ZooKeeper

Heron uses [ZooKeeper](https://zookeeper.apache.org) for a variety of coordination- and configuration-related tasks. To start up ZooKeeper on your GKE cluster:

```bash
$ kubectl create -f https://raw.githubusercontent.com/apache/incubator-heron/master/deploy/kubernetes/gcp/zookeeper.yaml
```

#### BookKeeper setup

> If you're using [Google Cloud Storage](#google-cloud-storage-setup) for topology artifact storage, skip to the [Heron tools](#heron-tools-gke) section below.

To start up an [Apache BookKeeper](https://bookkeeper.apache.org) cluster for Heron:

```bash
$ kubectl create -f https://raw.githubusercontent.com/apache/incubator-heron/master/deploy/kubernetes/gcp/bookkeeper.yaml
```

#### Heron tools <a id="heron-tools-gke"></a>

The so-called "Heron tools" include the [Heron UI](user-manuals-heron-ui) and the [Heron Tracker](user-manuals-heron-tracker-runbook). To start up the Heron tools:

```bash
$ kubectl create -f https://raw.githubusercontent.com/apache/incubator-heron/master/deploy/kubernetes/gcp/tools.yaml
```

#### Heron API server

The [Heron API server](deployment-api-server) is the endpoint that the [Heron CLI client](user-manuals-heron-cli) uses to interact with the other components of Heron. Heron on Google Container Engine has two separate versions of the Heron API server that you can run depending on which artifact storage system you're using ([Google Cloud Storage](#google-cloud-storage-setup) or [Apache BookKeeper](#bookkeeper-setup)).

If you're using Google Cloud Storage:

```bash
$ kubectl create -f https://raw.githubusercontent.com/apache/incubator-heron/master/deploy/kubernetes/gcp/gcs-apiserver.yaml
```

If you're using Apache BookKeeper:

```bash
$ kubectl create -f https://raw.githubusercontent.com/apache/incubator-heron/master/deploy/kubernetes/gcp/bookkeeper-apiserver.yaml
```

### Managing topologies

Once all of the [components](#starting-components) have been successfully started up, you need to open up a proxy port to your GKE Kubernetes cluster using the [`kubectl proxy`](https://kubernetes.io/docs/tasks/access-kubernetes-api/http-proxy-access-api/) command:

```bash
$ kubectl proxy -p 8001
```
> Note: All of the following Kubernetes specific urls are valid with the Kubernetes 1.10.0 release.

Now, verify that the Heron API server running on GKE is available using curl:

```bash
$ curl http://localhost:8001/api/v1/namespaces/default/services/heron-apiserver:9000/proxy/api/v1/version
```

You should get a JSON response like this:

```json
{
  "heron.build.git.revision" : "bf9fe93f76b895825d8852e010dffd5342e1f860",
  "heron.build.git.status" : "Clean",
  "heron.build.host" : "ci-server-01",
  "heron.build.time" : "Sun Oct  1 20:42:18 UTC 2017",
  "heron.build.timestamp" : "1506890538000",
  "heron.build.user" : "release-agent1",
  "heron.build.version" : "0.16.2"
}
```

Success! You can now manage Heron topologies on your GKE Kubernetes installation. To submit an example topology to the cluster:

```bash
$ heron submit kubernetes \
  --service-url=http://localhost:8001/api/v1/namespaces/default/services/heron-apiserver:9000/proxy \
  ~/.heron/examples/heron-api-examples.jar \
  org.apache.heron.examples.api.AckingTopology acking
```

You can also track the progress of the Kubernetes pods that make up the topology. When you run `kubectl get pods` you should see pods with names like `acking-0` and `acking-1`.

Another option is to set the service URL for Heron using the `heron config` command:

```bash
$ heron config kubernetes set service_url \
  http://localhost:8001/api/v1/namespaces/default/services/heron-apiserver:9000/proxy
```

That would enable you to manage topologies without setting the `--service-url` flag.

### Heron UI

The [Heron UI](user-manuals-heron-ui) is an in-browser dashboard that you can use to monitor your Heron [topologies](heron-topology-concepts). It should already be running in your GKE cluster.

You can access [Heron UI](user-manuals-heron-ui) in your browser by navigating to http://localhost:8001/api/v1/namespaces/default/services/heron-ui:8889/proxy/topologies.

## General Kubernetes clusters

Although [Minikube](#minikube) and [Google Container Engine](#google-container-engine) provide two easy ways to get started running Heron on Kubernetes, you can also run Heron on any Kubernetes cluster. The instructions in this section are tailored to non-Minikube, non-GKE Kubernetes installations.

### Requirements

To run Heron on a general Kubernetes installation, you'll need to fulfill the [requirements](#requirements) listed at the top of this doc. Once those requirements are met, you can begin starting up the various [components](#starting-components) that comprise a Heron on Kubernetes installation.

### Starting components

There are a variety of Heron components that you'll need to start up separately *and in order*. Make sure that the necessary pods are up and in the `RUNNING` state before moving on to the next step. You can track the progress of the pods using this command:

```bash
$ kubectl get pods -w
```

#### ZooKeeper

Heron uses [ZooKeeper](https://zookeeper.apache.org) for a variety of coordination- and configuration-related tasks. To start up ZooKeeper on your Kubernetes cluster:

```bash
$ kubectl create -f https://raw.githubusercontent.com/apache/incubator-heron/master/deploy/kubernetes/general/zookeeper.yaml
```

#### BookKeeper

When running Heron on Kubernetes, [Apache BookKeeper](https://bookkeeper.apache.org) is used for things like topology artifact storage (unless you're running on GKE). You can start up BookKeeper using this command:

```bash
$ kubectl create -f https://raw.githubusercontent.com/apache/incubator-heron/master/deploy/kubernetes/general/bookkeeper.yaml
```

#### Heron tools

The so-called "Heron tools" include the [Heron UI](user-manuals-heron-ui) and the [Heron Tracker](user-manuals-heron-tracker-runbook). To start up the Heron tools:

```bash
$ kubectl create -f https://raw.githubusercontent.com/apache/incubator-heron/master/deploy/kubernetes/general/tools.yaml
```

#### Heron API server

The Heron API server is the endpoint that the Heron CLI client uses to interact with the other components of Heron. To start up the Heron API server on your Kubernetes cluster:

```bash
$ kubectl create -f https://raw.githubusercontent.com/apache/incubator-heron/master/deploy/kubernetes/general/apiserver.yaml
```

### Managing topologies

Once all of the [components](#starting-components) have been successfully started up, you need to open up a proxy port to your GKE Kubernetes cluster using the [`kubectl proxy`](https://kubernetes.io/docs/tasks/access-kubernetes-api/http-proxy-access-api/) command:

```bash
$ kubectl proxy -p 8001
```

> Note: All of the following Kubernetes specific urls are valid with the Kubernetes 1.10.0 release.

Now, verify that the Heron API server running on GKE is available using curl:

```bash
$ curl http://localhost:8001/api/v1/namespaces/default/services/heron-apiserver:9000/proxy/api/v1/version
```

You should get a JSON response like this:

```json
{
  "heron.build.git.revision" : "ddbb98bbf173fb082c6fd575caaa35205abe34df",
  "heron.build.git.status" : "Clean",
  "heron.build.host" : "ci-server-01",
  "heron.build.time" : "Sat Mar 31 09:27:19 UTC 2018",
  "heron.build.timestamp" : "1522488439000",
  "heron.build.user" : "release-agent",
  "heron.build.version" : "0.17.8"
}
```

Success! You can now manage Heron topologies on your GKE Kubernetes installation. To submit an example topology to the cluster:

```bash
$ heron submit kubernetes \
  --service-url=http://localhost:8001/api/v1/namespaces/default/services/heron-apiserver:9000/proxy \
  ~/.heron/examples/heron-api-examples.jar \
  org.apache.heron.examples.api.AckingTopology acking
```

You can also track the progress of the Kubernetes pods that make up the topology. When you run `kubectl get pods` you should see pods with names like `acking-0` and `acking-1`.

Another option is to set the service URL for Heron using the `heron config` command:

```bash
$ heron config kubernetes set service_url \
  http://localhost:8001/api/v1/namespaces/default/services/heron-apiserver:9000/proxy
```

That would enable you to manage topologies without setting the `--service-url` flag.

### Heron UI

The [Heron UI](user-manuals-heron-ui) is an in-browser dashboard that you can use to monitor your Heron [topologies](heron-topology-concepts). It should already be running in your GKE cluster.

You can access [Heron UI](user-manuals-heron-ui) in your browser by navigating to http://localhost:8001/api/v1/namespaces/default/services/heron-ui:8889/proxy.

## Heron on Kubernetes configuration

You can configure Heron on Kubernetes using a variety of YAML config files, listed in the sections below.

### client.yaml

#### Configuration for the `heron` CLI tool.

| name                          | description                                             | default                                                   |
|-------------------------------|---------------------------------------------------------|-----------------------------------------------------------|
| heron.package.core.uri        | Location of the core Heron package                      | file:///vagrant/.herondata/dist/heron-core-release.tar.gz |
| heron.config.is.role.required | Whether a role is required to submit a topology         | False                                                     |
| heron.config.is.env.required  | Whether an environment is required to submit a topology | False                                                     |

### heron_internals.yaml

#### Configuration for a wide variety of Heron components, including logging, each topology's stream manager and topology master, and more.

| name                                                               | description                                                                                                                                         | default   |
|--------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|-----------|
| heron.logging.directory                                            | The relative path to the logging directory                                                                                                          | log-files |
| heron.logging.maximum.size.mb                                      | The maximum log file size (in MB)                                                                                                                   | 100       |
| heron.logging.maximum.files                                        | The maximum number of log files                                                                                                                     | 5         |
| heron.check.tmaster.location.interval.sec                          | The interval, in seconds, after which to check if the topology master location has been fetched or not                                              | 120       |
| heron.logging.prune.interval.sec                                   | The interval, in seconds, at which to prune C++ log files                                                                                           | 300       |
| heron.logging.flush.interval.sec                                   | The interval, in seconds, at which to flush C++ log files                                                                                           | 10        |
| heron.logging.err.threshold                                        | The threshold level at which to log errors                                                                                                          | 3         |
| heron.metrics.export.interval.sec                                  | The interval, in seconds, at which different components export metrics to the metrics manager                                                       | 60        |
| heron.metrics.max.exceptions.per.message.count                     | The maximum count of exceptions in one `MetricPublisherPublishMessage` protobuf message                                                             | 1024      |
| heron.streammgr.cache.drain.frequency.ms                           | The frequency, in milliseconds, at which to drain the tuple cache in the stream manager                                                             | 10        |
| heron.streammgr.stateful.buffer.size.mb                            | The sized-based threshold (in MB) for buffering data tuples waiting for checkpoint markers before giving up                                         | 100       |
| heron.streammgr.cache.drain.size.mb                                | The sized-based threshold (in MB) for draining the tuple cache                                                                                      | 100       |
| heron.streammgr.xormgr.rotatingmap.nbuckets                        | For efficient acknowledgements                                                                                                                      | 3         |
| heron.streammgr.mempool.max.message.number                         | The max number of messages in the memory pool for each message type                                                                                 | 512       |
| heron.streammgr.client.reconnect.interval.sec                      | The reconnect interval to other stream managers (in seconds) for the stream manager client                                                          | 1         |
| heron.streammgr.client.reconnect.tmaster.interval.sec              | The reconnect interval to the topology master (in seconds) for the stream manager client                                                            | 10        |
| heron.streammgr.client.reconnect.tmaster.max.attempts              | The max reconnect attempts to tmaster for stream manager client                                                                                     | 30        |
| heron.streammgr.network.options.maximum.packet.mb                  | The maximum packet size (in MB) of the stream manager's network options                                                                             | 10        |
| heron.streammgr.tmaster.heartbeat.interval.sec                     | The interval (in seconds) at which to send heartbeats                                                                                               | 10        |
| heron.streammgr.connection.read.batch.size.mb                      | The maximum batch size (in MB) for the stream manager to read from socket                                                                           | 1         |
| heron.streammgr.connection.write.batch.size.mb                     | Maximum batch size (in MB) for the stream manager to write to socket                                                                                | 1         |
| heron.streammgr.network.backpressure.threshold                     | The number of times Heron should wait to see a buffer full while enqueueing data before declaring the start of backpressure                         | 3         |
| heron.streammgr.network.backpressure.highwatermark.mb              | The high-water mark on the number (in MB) that can be left outstanding on a connection                                                              | 100       |
| heron.streammgr.network.backpressure.lowwatermark.mb               | The low-water mark on the number (in MB) that can be left outstanding on a connection                                                               |           |
| heron.tmaster.metrics.collector.maximum.interval.min               | The maximum interval (in minutes) for metrics to be kept in the topology master                                                                     | 180       |
| heron.tmaster.establish.retry.times                                | The maximum number of times to retry establishing connection with the topology master                                                               | 30        |
| heron.tmaster.establish.retry.interval.sec                         | The interval at which to retry establishing connection with the topology master                                                                     | 1         |
| heron.tmaster.network.master.options.maximum.packet.mb             | Maximum packet size (in MB) of topology master's network options to connect to stream managers                                                      | 16        |
| heron.tmaster.network.controller.options.maximum.packet.mb         | Maximum packet size (in MB) of the topology master's network options to connect to scheduler                                                        | 1         |
| heron.tmaster.network.stats.options.maximum.packet.mb              | Maximum packet size (in MB) of the topology master's network options for stat queries                                                               | 1         |
| heron.tmaster.metrics.collector.purge.interval.sec                 | The interval (in seconds) at which the topology master purges metrics from socket                                                                   | 60        |
| heron.tmaster.metrics.collector.maximum.exception                  | The maximum number of exceptions to be stored in the topology metrics collector, to prevent out-of-memory errors                                    | 256       |
| heron.tmaster.metrics.network.bindallinterfaces                    | Whether the metrics reporter should bind on all interfaces                                                                                          | False     |
| heron.tmaster.stmgr.state.timeout.sec                              | The timeout (in seconds) for the stream manager, compared with (current time - last heartbeat time)                                                 | 60        |
| heron.metricsmgr.network.read.batch.time.ms                        | The maximum batch time (in milliseconds) for the metrics manager to read from socket                                                                | 16        |
| heron.metricsmgr.network.read.batch.size.bytes                     | The maximum batch size (in bytes) to read from socket                                                                                               | 32768     |
| heron.metricsmgr.network.write.batch.time.ms                       | The maximum batch time (in milliseconds) for the metrics manager to write to socket                                                                 | 32768     |
| heron.metricsmgr.network.options.socket.send.buffer.size.bytes     | The maximum socket send buffer size (in bytes)                                                                                                      | 6553600   |
| heron.metricsmgr.network.options.socket.received.buffer.size.bytes | The maximum socket received buffer size (in bytes) for the metrics manager's network options                                                        | 8738000   |
| heron.metricsmgr.network.options.maximum.packetsize.bytes          | The maximum packet size that the metrics manager can read                                                                                           | 1048576   |
| heron.instance.network.options.maximum.packetsize.bytes            | The maximum size of packets that Heron instances can read                                                                                           | 10485760  |
| heron.instance.internal.bolt.read.queue.capacity                   | The queue capacity (num of items) in bolt for buffer packets to read from stream manager                                                            | 128       |
| heron.instance.internal.bolt.write.queue.capacity                  | The queue capacity (num of items) in bolt for buffer packets to write to stream manager                                                             | 128       |
| heron.instance.internal.spout.read.queue.capacity                  | The queue capacity (num of items) in spout for buffer packets to read from stream manager                                                           | 1024      |
| heron.instance.internal.spout.write.queue.capacity                 | The queue capacity (num of items) in spout for buffer packets to write to stream manager                                                            | 128       |
| heron.instance.internal.metrics.write.queue.capacity               | The queue capacity (num of items) for metrics packets to write to metrics manager                                                                   | 128       |
| heron.instance.network.read.batch.time.ms                          | Time based, the maximum batch time in ms for instance to read from stream manager per attempt                                                       | 16        |
| heron.instance.network.read.batch.size.bytes                       | Size based, the maximum batch size in bytes to read from stream manager                                                                             | 32768     |
| heron.instance.network.write.batch.time.ms                         | Time based, the maximum batch time (in milliseconds) for the instance to write to the stream manager per attempt                                    | 16        |
| heron.instance.network.write.batch.size.bytes                      | Size based, the maximum batch size in bytes to write to stream manager                                                                              | 32768     |
| heron.instance.network.options.socket.send.buffer.size.bytes       | The maximum socket's send buffer size in bytes                                                                                                      | 6553600   |
| heron.instance.network.options.socket.received.buffer.size.bytes   | The maximum socket's received buffer size in bytes of instance's network options                                                                    | 8738000   |
| heron.instance.set.data.tuple.capacity                             | The maximum number of data tuple to batch in a HeronDataTupleSet protobuf                                                                           | 1024      |
| heron.instance.set.data.tuple.size.bytes                           | The maximum size in bytes of data tuple to batch in a HeronDataTupleSet protobuf                                                                    | 8388608   |
| heron.instance.set.control.tuple.capacity                          | The maximum number of control tuple to batch in a HeronControlTupleSet protobuf                                                                     | 1024      |
| heron.instance.ack.batch.time.ms                                   | The maximum time in ms for a spout to do acknowledgement per attempt, the ack batch could also break if there are no more ack tuples to process     | 128       |
| heron.instance.emit.batch.time.ms                                  | The maximum time in ms for an spout instance to emit tuples per attempt                                                                             | 16        |
| heron.instance.emit.batch.size.bytes                               | The maximum batch size in bytes for an spout to emit tuples per attempt                                                                             | 32768     |
| heron.instance.execute.batch.time.ms                               | The maximum time in ms for an bolt instance to execute tuples per attempt                                                                           | 16        |
| heron.instance.execute.batch.size.bytes                            | The maximum batch size in bytes for an bolt instance to execute tuples per attempt                                                                  | 32768     |
| heron.instance.state.check.interval.sec                            | The time interval for an instance to check the state change, for example, the interval a spout uses to check whether activate/deactivate is invoked | 5         |
| heron.instance.force.exit.timeout.ms                               | The time to wait before the instance exits forcibly when uncaught exception happens                                                                 | 2000      |
| heron.instance.reconnect.streammgr.interval.sec                    | Interval in seconds to reconnect to the stream manager, including the request timeout in connecting                                                 | 5         |
| heron.instance.reconnect.streammgr.interval.sec                    | Interval in seconds to reconnect to the stream manager, including the request timeout in connecting                                                 | 60        |
| heron.instance.reconnect.metricsmgr.interval.sec                   | Interval in seconds to reconnect to the metrics manager, including the request timeout in connecting                                                | 5         |
| heron.instance.reconnect.metricsmgr.times                          | Interval in seconds to reconnect to the metrics manager, including the request timeout in connecting                                                | 60        |
| heron.instance.metrics.system.sample.interval.sec                  | The interval in second for an instance to sample its system metrics, for instance, CPU load.                                                        | 10        |
| heron.instance.slave.fetch.pplan.interval.sec                      | The time interval (in seconds) at which Heron instances fetch the physical plan from slaves                                                         | 1         |
| heron.instance.acknowledgement.nbuckets                            | For efficient acknowledgement                                                                                                                       | 10        |
| heron.instance.tuning.expected.bolt.read.queue.size                | The expected size on read queue in bolt                                                                                                             | 8         |
| heron.instance.tuning.expected.bolt.write.queue.size               | The expected size on write queue in bolt                                                                                                            | 8         |
| heron.instance.tuning.expected.spout.read.queue.size               | The expected size on read queue in spout                                                                                                            | 512       |
| heron.instance.tuning.expected.spout.write.queue.size              | The exepected size on write queue in spout                                                                                                          | 8         |
| heron.instance.tuning.expected.metrics.write.queue.size            | The expected size on metrics write queue                                                                                                            | 8         |
| heron.instance.tuning.current.sample.weight                        |                                                                                                                                                     | 0.8       |
| heron.instance.tuning.interval.ms                                  | Interval in ms to tune the size of in & out data queue in instance                                                                                  | 100       |

### packing.yaml

| name                          | description                                             | default                                               |
|-------------------------------|---------------------------------------------------------|-------------------------------------------------------|
| heron.class.packing.algorithm | Packing algorithm for packing instances into containers | org.apache.heron.packing.roundrobin.RoundRobinPacking |

### scheduler.yaml

| name                              | description                                                 | default                                                   |
|-----------------------------------|-------------------------------------------------------------|-----------------------------------------------------------|
| heron.class.scheduler             | scheduler class for distributing the topology for execution | org.apache.heron.scheduler.kubernetes.KubernetesScheduler |
| heron.class.launcher              | launcher class for submitting and launching the topology    | org.apache.heron.scheduler.kubernetes.KubernetesLauncher  |
| heron.directory.sandbox.java.home | location of java - pick it up from shell environment        | $JAVA_HOME                                                |
| heron.kubernetes.scheduler.uri    | The URI of the Kubernetes API                               |                                                           |
| heron.scheduler.is.service        | Invoke the IScheduler as a library directly                 | false                                                     |
| heron.executor.docker.image       | docker repo for executor                                    | heron/heron:latest                                        |

### stateful.yaml

| name                            | description                                            | default                                                         |
|---------------------------------|--------------------------------------------------------|-----------------------------------------------------------------|
| heron.statefulstorage.classname | The type of storage to be used for state checkpointing | org.apache.heron.statefulstorage.localfs.LocalFileSystemStorage |

### statemgr.yaml

| name                                           | description                                                           | default                                                         |
|------------------------------------------------|-----------------------------------------------------------------------|-----------------------------------------------------------------|
| heron.class.state.manager                      | local state manager class for managing state in a persistent fashion  | org.apache.heron.statemgr.zookeeper.curator.CuratorStateManager |
| heron.statemgr.connection.string               | local state manager connection string                                 |                                                                 |
| heron.statemgr.root.path                       | path of the root address to store the state in a local file system    | /heron                                                          |
| heron.statemgr.zookeeper.is.initialize.tree    | create the zookeeper nodes, if they do not exist                      | True                                                            |
| heron.statemgr.zookeeper.session.timeout.ms    | timeout in ms to wait before considering zookeeper session is dead    | 30000                                                           |
| heron.statemgr.zookeeper.connection.timeout.ms | timeout in ms to wait before considering zookeeper connection is dead | 30000                                                           |
| heron.statemgr.zookeeper.retry.count           | timeout in ms to wait before considering zookeeper connection is dead | 10                                                              |
| heron.statemgr.zookeeper.retry.interval.ms     | duration of time to wait until the next retry                         | 10000                                                           |

### uploader.yaml

| name                         | description                                                                             | default                                 |
|------------------------------|-----------------------------------------------------------------------------------------|-----------------------------------------|
| heron.class.uploader         | uploader class for transferring the topology files (jars, tars, PEXes, etc.) to storage | org.apache.heron.uploader.s3.S3Uploader |
| heron.uploader.s3.bucket     | S3 bucket in which topology assets will be stored (if AWS S3 is being used)             |                                         |
| heron.uploader.s3.access_key | AWS access key (if AWS S3 is being used)                                                |                                         |
| heron.uploader.s3.secret_key | AWS secret access key (if AWS S3 is being used)                                         |                                         |