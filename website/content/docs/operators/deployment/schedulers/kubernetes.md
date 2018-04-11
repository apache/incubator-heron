---
title: Heron on Kubernetes by hand
description: Run Heron on the foremost open source container orchestration platform
new: true
---

> This document shows you how to install Heron on Kubernetes in a step-by-step, "by hand" fashion. An easier way to install Heron on Kubernetes is to use the [Helm](https://helm.sh) package manager. For instructions on doing so, see [Heron on Kubernetes with Helm](../kubernetes-helm)).

Heron supports deployment on [Kubernetes](https://kubernetes.io/) (sometimes called **k8s**). Heron deployments on Kubernetes use Docker as the containerization format for Heron topologies and use the Kubernetes API for scheduling.

You can use Heron on Kubernetes in multiple environments:

* Locally using [Minikube](#minikube)
* In the cloud on [Google Container Engine](#google-container-engine) (GKE)
* In [any other](#general-kubernetes-clusters) Kubernetes cluster

## Requirements

In order to run Heron on Kubernetes, you will need:

* A Kubernetes cluster with at least 3 nodes (unless you're running locally on [Minikube](#minikube))
* The [`kubectl`](https://kubernetes.io/docs/tasks/tools/install-kubectl/) CLI tool installed and set up to communicate with your cluster
* The [`heron`](../../../../getting-started) CLI tool

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

The so-called "Heron tools" include the [Heron UI](../../../heron-ui) and the [Heron Tracker](../../../heron-tracker). To start up the Heron tools:

```bash
$ kubectl create -f https://raw.githubusercontent.com/apache/incubator-heron/master/deploy/kubernetes/minikube/tools.yaml
```

#### Heron API server

The Heron API server is the endpoint that the Heron CLI client uses to interact with the other components of Heron. To start up the Heron API server on Minikube:

```bash
$ kubectl create -f https://raw.githubusercontent.com/apache/incubator-heron/master/deploy/kubernetes/minikube/apiserver.yaml
```

### Managing topologies

Once all of the [components](#components) have been successfully started up, you need to open up a proxy port to your Minikube Kubernetes cluster using the [`kubectl proxy`](https://kubernetes.io/docs/tasks/access-kubernetes-api/http-proxy-access-api/) command:

```bash
$ kubectl proxy -p 8001
```

Now, verify that the Heron API server running on Minikube is available using curl:

```bash
$ curl http://localhost:8001/api/v1/proxy/namespaces/default/services/heron-apiserver:9000/api/v1/version
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

Success! You can now manage Heron topologies on your Minikube Kubernetes installation. To submit an example topology to the cluster:

```bash
$ heron submit kubernetes \
  --service-url=http://localhost:8001/api/v1/proxy/namespaces/default/services/heron-apiserver:9000 \
  ~/.heron/examples/heron-api-examples.jar \
  org.apache.heron.examples.api.AckingTopology acking
```

You can also track the progress of the Kubernetes pods that make up the topology. When you run `kubectl get pods` you should see pods with names like `acking-0` and `acking-1`.

Another option is to set the service URL for Heron using the `heron config` command:

```bash
$ heron config kubernetes set service_url \
  http://localhost:8001/api/v1/proxy/namespaces/default/services/heron-apiserver:9000
```

That would enable you to manage topologies without setting the `--service-url` flag.

### Heron UI

The [Heron UI](../../../heron-ui) is an in-browser dashboard that you can use to monitor your Heron [topologies](../../../../concepts/topologies). It should already be running in Minikube.

You can access [Heron UI](../../../heron-ui) in your browser by navigating to http://localhost:8001/api/v1/proxy/namespaces/default/services/heron-ui:8889.

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

The so-called "Heron tools" include the [Heron UI](../../../heron-ui) and the [Heron Tracker](../../../heron-tracker). To start up the Heron tools:

```bash
$ kubectl create -f https://raw.githubusercontent.com/apache/incubator-heron/master/deploy/kubernetes/gcp/tools.yaml
```

#### Heron API server

The [Heron API server](../../../heron-api-server) is the endpoint that the [Heron CLI client](../../../heron-cli) uses to interact with the other components of Heron. Heron on Google Container Engine has two separate versions of the Heron API server that you can run depending on which artifact storage system you're using ([Google Cloud Storage](#google-cloud-storage-setup) or [Apache BookKeeper](#bookkeeper-setup)).

If you're using Google Cloud Storage:

```bash
$ kubectl create -f https://raw.githubusercontent.com/apache/incubator-heron/master/deploy/kubernetes/gcp/gcs-apiserver.yaml
```

If you're using Apache BookKeeper:

```bash
$ kubectl create -f https://raw.githubusercontent.com/apache/incubator-heron/master/deploy/kubernetes/gcp/bookkeeper-apiserver.yaml
```

### Managing topologies

Once all of the [components](#components) have been successfully started up, you need to open up a proxy port to your GKE Kubernetes cluster using the [`kubectl proxy`](https://kubernetes.io/docs/tasks/access-kubernetes-api/http-proxy-access-api/) command:

```bash
$ kubectl proxy -p 8001
```

Now, verify that the Heron API server running on GKE is available using curl:

```bash
$ curl http://localhost:8001/api/v1/proxy/namespaces/default/services/heron-apiserver:9000/api/v1/version
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
  --service-url=http://localhost:8001/api/v1/proxy/namespaces/default/services/heron-apiserver:9000 \
  ~/.heron/examples/heron-api-examples.jar \
  org.apache.heron.examples.api.AckingTopology acking
```

You can also track the progress of the Kubernetes pods that make up the topology. When you run `kubectl get pods` you should see pods with names like `acking-0` and `acking-1`.

Another option is to set the service URL for Heron using the `heron config` command:

```bash
$ heron config kubernetes set service_url \
  http://localhost:8001/api/v1/proxy/namespaces/default/services/heron-apiserver:9000
```

That would enable you to manage topologies without setting the `--service-url` flag.

### Heron UI

The [Heron UI](../../../heron-ui) is an in-browser dashboard that you can use to monitor your Heron [topologies](../../../../concepts/topologies). It should already be running in your GKE cluster.

You can access [Heron UI](../../../heron-ui) in your browser by navigating to http://localhost:8001/api/v1/proxy/namespaces/default/services/heron-ui:8889.

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

The so-called "Heron tools" include the [Heron UI](../../../heron-ui) and the [Heron Tracker](../../../heron-tracker). To start up the Heron tools:

```bash
$ kubectl create -f https://raw.githubusercontent.com/apache/incubator-heron/master/deploy/kubernetes/general/tools.yaml
```

#### Heron API server

The Heron API server is the endpoint that the Heron CLI client uses to interact with the other components of Heron. To start up the Heron API server on your Kubernetes cluster:

```bash
$ kubectl create -f https://raw.githubusercontent.com/apache/incubator-heron/master/deploy/kubernetes/general/apiserver.yaml
```

### Managing topologies

Once all of the [components](#components) have been successfully started up, you need to open up a proxy port to your GKE Kubernetes cluster using the [`kubectl proxy`](https://kubernetes.io/docs/tasks/access-kubernetes-api/http-proxy-access-api/) command:

```bash
$ kubectl proxy -p 8001
```

Now, verify that the Heron API server running on GKE is available using curl:

```bash
$ curl http://localhost:8001/api/v1/proxy/namespaces/default/services/heron-apiserver:9000/api/v1/version
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
  --service-url=http://localhost:8001/api/v1/proxy/namespaces/default/services/heron-apiserver:9000 \
  ~/.heron/examples/heron-api-examples.jar \
  org.apache.heron.examples.api.AckingTopology acking
```

You can also track the progress of the Kubernetes pods that make up the topology. When you run `kubectl get pods` you should see pods with names like `acking-0` and `acking-1`.

Another option is to set the service URL for Heron using the `heron config` command:

```bash
$ heron config kubernetes set service_url \
  http://localhost:8001/api/v1/proxy/namespaces/default/services/heron-apiserver:9000
```

That would enable you to manage topologies without setting the `--service-url` flag.

### Heron UI

The [Heron UI](../../../heron-ui) is an in-browser dashboard that you can use to monitor your Heron [topologies](../../../../concepts/topologies). It should already be running in your GKE cluster.

You can access [Heron UI](../../../heron-ui) in your browser by navigating to http://localhost:8001/api/v1/proxy/namespaces/default/services/heron-ui:8889.

## Heron on Kubernetes configuration

You can configure Heron on Kubernetes using a variety of YAML config files, listed in the sections below.

### client.yaml

{{< configtable "kubernetes" "client" >}}

### heron_internals.yaml

{{< configtable "kubernetes" "heron_internals" >}}

### packing.yaml

{{< configtable "kubernetes" "packing" >}}

### scheduler.yaml

{{< configtable "kubernetes" "scheduler" >}}

### stateful.yaml

{{< configtable "kubernetes" "stateful" >}}

### statemgr.yaml

{{< configtable "kubernetes" "statemgr" >}}

### uploader.yaml

{{< configtable "kubernetes" "uploader" >}}