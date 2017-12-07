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

```bash
$ 
```