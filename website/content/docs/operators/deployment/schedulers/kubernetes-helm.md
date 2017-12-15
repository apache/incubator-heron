---
title: Heron on Kubernetes with Helm
description: The fastest and easiest way to install Heron on Kubernetes
new: true
---

> If you'd prefer to install Heron on Kubernetes *without* using the [Helm](https://helm.sh) package manager, see the [Heron on Kubernetes by hand](../kubernetes) document.

[Helm](https://helm.sh) is an open source package manager for [Kubernetes](https://kubernetes.io) that enables you to quickly and easily install even the most complex software systems on Kubernetes. Heron has a Helm [chart](https://docs.helm.sh/developing_charts/#charts) that you can use to install Heron on Kubernetes using just a few commands.

> You can use the Helm chart for Heron to run Heron on *any* Kubernetes cluster, including local clusters running on [Minikube](https://github.com/kubernetes/minikube).

## Requirements

In order to install Heron on Kubernetes using Helm, you need to have an existing Kubernetes cluster

## Installing the Helm client

In order to get started, you need to install Helm on your 

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

```bash
$ helm init
```

## Installing Heron on Kubernetes

Once you've installed the Helm client on your machine, you need to make your client aware of the [Streamlio](https://streaml.io) repository, which houses the Helm chart for Heron:

```bash
$ helm repo add streamlio https://storage.googleapis.com/streamlio/charts
```

To install the Heron package:

```bash
$ helm install streamlio/heron
```

This will install Heron and provide the installation with a random name like `jazzy-anaconda`. To provide the installation with a name, such as `heron-kubernetes`:

```bash
$ helm install streamlio/heron \
  --name heron-kubernetes
```

### Specifying a platform

The default platform is [Minikube](#minikube). To specify a different platform, you can use the `--set platform=PLATFORM` flag. Here's an example:

```bash
$ helm install streamlio/heron \
  --set platform=gke
```

The available platforms are:

Platform | Tag
:--------|:---
[Minikube](#minikube) | `minikube`
[Google Kubernetes Engine](#google-kubernetes-engine) | `gke`
[Amazon Web Services](#amazone-web-services) | `aws`
[Bare metal](#bare-metal) | `baremetal`

### Minikube

[Install Minikube](https://kubernetes.io/docs/tasks/tools/install-minikube/)

```bash
$ minikube start \
  --memory=7168 \
  --cpus=5 \
  --disk-size=20g
```

### Google Kubernetes Engine

```bash
$ helm install streamlio/heron \
  --set platform=gke
```

### Amazon Web Services

```bash
$ helm install streamlio/heron \
  --set platform=aws
```

### Bare metal

```bash
$ helm install streamlio/heron \
  --set platform=baremetal
```