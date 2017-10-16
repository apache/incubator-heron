---
title: Kubernetes
new: true
---

Heron supports deployment on [Kubernetes](https://kubernetes.io/) (sometimes called **k8s**). Heron deployments on Kubernetes 
are done via Docker (containerization) and the Kubernetes API (scheduling).

You can use Heron on Kubernetes in multiple environments:

* Locally using [Minikube](#minikube)
* In the cloud on [Google Container Engine](#google-container-engine) (GKE)

## Requirements

In order to run Heron on Kubernetes, you will need:

* A Kubernetes cluster with at least 3 nodes
* The [`kubectl`](https://kubernetes.io/docs/tasks/tools/install-kubectl/) CLI tool installed and set up to communicate with your cluster
* The [`heron`](../../../../getting-started) CLI tool

Any additional requirements will depend on where you're running Heron on Kubernetes.

## How Heron on Kubernetes Works

When deploying to Kubernetes, each Heron container is deployed as a Kubernetes
[pod](https://kubernetes.io/docs/concepts/workloads/pods/pod/) inside of a Docker container. If there
are 20 containers that are going to be deployed with a topoology, for example, then there will be 20 pods
deployed to your Kubernetes cluster for that topology.

## Setting up Heron on Kubernetes

You can set up Heron on Kubernetes by 

### Installing components

#### ZooKeeper

Heron uses [ZooKeeper](https://zookeeper.apache.org) for a variety of coordination- and configuration-related tasks. You 

```bash
$ kubectl create -f https://raw.githubusercontent.com/twitter/heron/master/deploy/kubernetes/general/zookeeper.yaml
```

```bash
$ kubectl get pods -w -l app=heron
```

#### BookKeeper

```bash
$ kubectl create -f https://raw.githubusercontent.com/twitter/heron/master/deploy/kubernetes/general/bookkeeper.yaml
```

> BookKeeper is currently the only [state manager](../../../../concepts/topologies) for Heron that's supported when running
> Heron on Kubernetes.

### Heron tools

```bash
$ kubectl create -f https://raw.githubusercontent.com/twitter/heron/master/deploy/kubernetes/general/tools.yaml
```

### Heron API server

```bash
$ kubectl create -f https://raw.githubusercontent.com/twitter/heron/master/deploy/kubernetes/general/apiserver.yaml
```

## Accessing the running Heron installation

### The Heron CLI tool

```bash
$ kubectl proxy -p 8001
```

```bash
$ heron submit kubernetes \
  --service-url=http://localhost:8001/api/v1/proxy/namespaces/default/services/heron-apiserver:9000
```

```bash
$ heron config kubernetes set service_url \
  http://localhost:8001/api/v1/proxy/namespaces/default/services/heron-apiserver:9000
```

```bash
$ curl http://localhost:8001/api/v1/proxy/namespaces/default/services/heron-apiserver:9000/api/v1/version
```

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

```bash
$ heron submit kubernetes \
  --service-url=http://localhost:8001/api/v1/proxy/namespaces/default/services/heron-apiserver:9000 \
  ~/.heron/examples/heron-api-examples.jar \
  com.twitter.heron.examples.api.AckingTopology \
  AckingTopology
```

### Heron UI

The [Heron UI](../../../heron-ui) is an in-browser dashboard that you can use to monitor your Heron [topologies](../../../../concepts/topologies)

You can access [Heron UI](../../../heron-ui) in your browser by navigating to http://localhost:8001/api/v1/proxy/namespaces/default/services/heron-ui:8889.


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
$ kubectl get pods -w -l app=heron
```

#### ZooKeeper

Heron uses [ZooKeeper](https://zookeeper.apache.org) for a variety of coordination- and configuration-related tasks. To start up ZooKeeper on Minikube:

```bash
$ kubectl create -f https://raw.githubusercontent.com/twitter/heron/master/deploy/kubernetes/minikube/zookeeper.yaml
```

#### BookKeeper

When running Heron on Kubernetes, [Apache BookKeeper](https://bookkeeper.apache.org) is used for things like topology artifact storage. You can start up BookKeeper using this command:

```bash
$ kubectl create -f https://raw.githubusercontent.com/twitter/heron/master/deploy/kubernetes/minikube/bookkeeper.yaml
```

#### Heron tools

The so-called "Heron tools" include the [Heron UI](../../../heron-ui) and the [Heron Tracker](../../../heron-tracker).

#### Heron API server

The Heron API server is the endpoint that the Heron CLI client uses to interact with the other components of Heron. To start up the Heron API server on Minikube:

```bash
$ kubectl create -f https://raw.githubusercontent.com/twitter/heron/master/deploy/kubernetes/minikube/apiserver.yaml
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
  com.twitter.heron.examples.api.AckingTopology acking
```

You can also track the progress of the Kubernetes pods that make up the topology. When you run `kubectl get pods` you should see pods with names like `acking-0` and `acking-1`.

Another option is to set the service URL for Heron using the `heron config` command:

```bash
$ heron config kubernetes set service_url \
  http://localhost:8001/api/v1/proxy/namespaces/default/services/heron-apiserver:9000
```

That would enable you to manage topologies without setting the `--service-url` flag.

### Heron UI

[Heron UI](../../../heron-ui) should already be running on Minikube. To access it, navigate to http://localhost:8001/api/v1/proxy/namespaces/default/services/heron-ui:8889.