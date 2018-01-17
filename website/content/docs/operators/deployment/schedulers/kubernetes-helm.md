---
title: Heron on Kubernetes with Helm
description: The fastest and easiest way to install Heron on Kubernetes
new: true
---

> If you'd prefer to install Heron on Kubernetes *without* using the [Helm](https://helm.sh) package manager, see the [Heron on Kubernetes by hand](../kubernetes) document.

[Helm](https://helm.sh) is an open source package manager for [Kubernetes](https://kubernetes.io) that enables you to quickly and easily install even the most complex software systems on Kubernetes. Heron has a Helm [chart](https://docs.helm.sh/developing_charts/#charts) that you can use to install Heron on Kubernetes using just a few commands. The chart can be used to install Heron on the following platforms:

* [Minikube](#minikube) (the default)
* [Google Kubernetes Engine](#google-kubernetes-engine)
* [Amazon Web Services](#amazon-web-services)
* [Bare metal](#bare-metal)

## Requirements

In order to install Heron on Kubernetes using Helm, you'll need to have:

* An existing Kubernetes cluster on one of the available [platforms](#specifying-a-platform)

## Installing the Helm client

In order to get started, you need to install Helm on your machine. Installation instructions for [macOS](#helm-for-macos) and [Linux](#helm-for-linux) are below.

### Helm for macOS

You can install Helm on macOS using [Homebrew](https://brew.sh):

```bash
$ brew install kubernetes-helm
```

### Helm for Linux

You can install Helm on Linux using a simple installation script:

```bash
$ curl https://raw.githubusercontent.com/kubernetes/helm/master/scripts/get > install-helm.sh
$ chmod 700 install-helm.sh
$ ./install-helm.sh
```

## Installing Helm in your Kubernetes cluster

To run Helm on Kubernetes, you need to first make sure that [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl) is using the right configuration context for your cluster. To check which context is being used:

```bash
$ kubectl config current-context
```

If the context is correct, then you can get Helm running using just one command:

```bash
$ helm init
```

If the output of that command includes `Happy Helming!` then Helm is ready to go.

## Installing Heron on Kubernetes

Once you've installed the Helm client on your machine and gotten Helm running in your Kubernetes cluster, you need to make your client aware of the `heron-charts` Helm repository, which houses the chart for Heron:

```bash
$ helm repo add heron-charts https://storage.googleapis.com/heron-charts
"heron-charts" has been added to your repositories
```

Now you can install the Heron package:

```bash
$ helm install heron-charts/heron
```

This will install Heron and provide the installation with a random name like `jazzy-anaconda`. To provide the installation with a name, such as `heron-kubernetes`:

```bash
$ helm install heron-charts/heron \
  --name heron-kubernetes
```

### Specifying a platform

The default platform for running Heron on Kubernetes is [Minikube](#minikube). To specify a different platform, you can use the `--set platform=PLATFORM` flag. Here's an example:

```bash
$ helm install heron-charts/heron \
  --set platform=gke
```

The available platforms are:

Platform | Tag
:--------|:---
[Minikube](#minikube) | `minikube`
[Google Kubernetes Engine](#google-kubernetes-engine) | `gke`
[Amazon Web Services](#amazone-web-services) | `aws`
[Bare metal](#bare-metal) | `baremetal`

#### Minikube

To run Heron on Minikube, you need to first [install Minikube](https://kubernetes.io/docs/tasks/tools/install-minikube/). Once Minikube is installed, you can start it by running `minikube start`. Please note, however, that Heron currently requires the following resources:

* 7 GB of memory
* 5 CPUs
* 20 GB of disk space

To start up Minikube with the minimum necessary resources:

```bash
$ minikube start \
  --memory=7168 \
  --cpus=5 \
  --disk-size=20g
```

Once Minikube is running, you can then install Heron in one of two ways:

```bash
# Use the Minikube default
$ helm install heron-charts/heron

# Explicitly select Minikube
$ helm install heron-charts/heron \
  --set platform=minikube
```

#### Google Kubernetes Engine

The resources required to run Heron on [Google Kubernetes Engine](https://cloud.google.com/kubernetes-engine/) vary based on your use case. To run a basic Heron cluster intended for experimentation, you'll need:

* 3 nodes
* n1-standard-2 machines
* 2 SSDs per machine

To create a cluster with those resources using the [gcloud](https://cloud.google.com/sdk/gcloud/) tool:

```bash
$ gcloud container clusters create heron-gke-dev-cluster \
  --num-nodes=3 \
  --machine-type=n1-standard-2 \
  --local-ssd-count=2
```

For a production-ready cluster you'll want a larger cluster with:

* *at least* 8 nodes
* n1-standard-4 or n1-standard-8 machines (preferably the latter)
* 2 SSDs per machine

To create such a cluster:

```bash
$ gcloud container clusters create heron-gke-prod-cluster \
  --num-nodes=8 \
  --machine-type=n1-standard-8 \
  --local-ssd-count=2
```

Once the cluster has been successfully created, you'll need to install that cluster's credentials locally so that they can be used by [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl/). You can do this in just one command:

```bash
$ gcloud container clusters get-credentials heron-gke-dev-cluster # or heron-gke-prod-cluster
```

Once, the cluster is running (that could take a few minutes), you can initialize Helm on the cluster and then install Heron:

```bash
$ helm init
$ helm install heron-charts/heron \
  --set platform=gke
```

#### Amazon Web Services

To run Heron on Kubernetes on Amazon Web Services (AWS), you'll need to 

```bash
$ helm install heron-charts/heron \
  --set platform=aws
```

#### Bare metal

To run Heron on a bare metal Kubernetes cluster:

```bash
$ helm install heron-charts/heron \
  --set platform=baremetal
```

## Running topologies on Heron on Kubernetes

Get the URL for the [Heron API server](../../../heron-api-server)

```bash
$ heron config kubernetes set service_url \
  http://localhost:8001/api/v1/proxy/namespaces/default/services/heron-kubernetes-heron-apiserver:9000/api/v1/version
```

Now, you can submit an example topology:

```bash
$ heron submit kubernetes \
  ~/.heron/examples/heron-streamlet-examples.jar \
  com.twitter.heron.examples.streamlet.WindowedWordCountTopology \
  WindowedWordCount
```