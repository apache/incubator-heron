---
id: schedulers-k8s-with-helm
title: Kubernetes with Helm
sidebar_label:  Kubernetes with Helm
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

> If you'd prefer to install Heron on Kubernetes *without* using the [Helm](https://helm.sh) package manager, see the [Heron on Kubernetes by hand](schedulers-k8s-by-hand) document.

[Helm](https://helm.sh) is an open source package manager for [Kubernetes](https://kubernetes.io) that enables you to quickly and easily install even the most complex software systems on Kubernetes. Heron has a Helm [chart](https://docs.helm.sh/developing_charts/#charts) that you can use to install Heron on Kubernetes using just a few commands. The chart can be used to install Heron on the following platforms:

* [Minikube](#minikube) (the default)
* [Google Kubernetes Engine](#google-kubernetes-engine)
* [Amazon Web Services](#amazon-web-services)
* [Bare metal](#bare-metal)

## Requirements

In order to install Heron on Kubernetes using Helm, you'll need to have an existing Kubernetes cluster on one of the supported [platforms](#specifying-a-platform) (which includes [bare metal](#bare-metal) installations).

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
$ curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/master/scripts/get-helm-3
$ chmod 700 get_helm.sh
$ ./get_helm.sh
```

## Installing Heron on Kubernetes

To use Helm with Kubernetes, you need to first make sure that [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl) is using the right configuration context for your cluster. To check which context is being used:

```bash
$ kubectl config current-context
```

Once you've installed the Helm client on your machine and gotten Helm pointing to your Kubernetes cluster, you need to make your client aware of the `heron-charts` Helm repository, which houses the chart for Heron:

```bash
$ helm repo add heron-charts https://storage.googleapis.com/heron-charts
"heron-charts" has been added to your repositories
```

Create a namespace to install into:

```bash
$ kubectl create namespace heron
```

Now you can install the Heron package:

```bash
$ helm install heron-charts/heron -g -n heron
```

This will install Heron and provide the installation in the `heron` namespace (`-n`) with a random name (`-g`) like `jazzy-anaconda`. To provide the installation with a name, such as `heron-kube`:

```bash
$ helm install heron-kube heron-charts/heron \
  --namespace heron
```

### Specifying a platform

The default platform for running Heron on Kubernetes is [Minikube](#minikube). To specify a different platform, you can use the `--set platform=PLATFORM` flag. Here's an example:

```bash
$ helm install heron-kube heron-charts/heron \
  --namespace heron \
  --set platform=gke
```

The available platforms are:

Platform | Tag
:--------|:---
[Minikube](#minikube) | `minikube`
[Google Kubernetes Engine](#google-kubernetes-engine) | `gke`
[Amazon Web Services](#amazon-web-services) | `aws`
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

Create a namespace to install into:

```bash
$ kubectl create namespace heron
```

```bash
# Use the Minikube default
$ helm install heron-kube heron-charts/heron \
  --namespace heron

# Explicitly select Minikube
$ helm install heron-kube heron-charts/heron \
  --namespace heron \
  --set platform=minikube
```

#### Google Kubernetes Engine

The resources required to run Heron on [Google Kubernetes Engine](https://cloud.google.com/kubernetes-engine/) vary based on your use case. To run a basic Heron cluster intended for development and experimentation, you'll need at least:

* 3 nodes
* [n1-standard-4](https://cloud.google.com/compute/docs/machine-types#standard_machine_types) machines

To create a cluster with those resources using the [gcloud](https://cloud.google.com/sdk/gcloud/) tool:

```bash
$ gcloud container clusters create heron-gke-dev-cluster \
  --num-nodes=3 \
  --machine-type=n1-standard-2
```

For a production-ready cluster you'll want a larger cluster with:

* *at least* 8 nodes
* [n1-standard-4 or n1-standard-8](https://cloud.google.com/compute/docs/machine-types#standard_machine_types) machines (preferably the latter)

To create such a cluster:

```bash
$ gcloud container clusters create heron-gke-prod-cluster \
  --num-nodes=8 \
  --machine-type=n1-standard-8
```

Once the cluster has been successfully created, you'll need to install that cluster's credentials locally so that they can be used by [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl/). You can do this in just one command:

```bash
$ gcloud container clusters get-credentials heron-gke-dev-cluster # or heron-gke-prod-cluster
```

Create a namespace to install into:

```bash
$ kubectl create namespace heron
```

Now you can install Heron:

```bash
$ helm install heron-kube heron-charts/heron \
  --namespace heron \
  --set platform=gke
```

##### Resource configurations

Helm enables you to supply sets of variables via YAML files. There are currently a handful of different resource configurations that can be applied to your Heron on GKE cluster upon installation:

Configuration | Description
:-------------|:-----------
[`small.yaml`](https://github.com/apache/incubator-heron/blob/master/deploy/kubernetes/gke/small.yaml) | Smaller Heron cluster intended for basic testing, development, and experimentation
[`medium.yaml`](https://github.com/apache/incubator-heron/blob/master/deploy/kubernetes/gke/medium.yaml) | Closer geared for production usage

To apply the `small` configuration, for example:

```bash
$ helm install heron-kube heron-charts/heron \
  --namespace heron \
  --set platform=gke \
  --values https://raw.githubusercontent.com/apache/incubator-heron/master/deploy/kubernetes/gcp/small.yaml
```

#### Amazon Web Services

To run Heron on Kubernetes on Amazon Web Services (AWS), you'll need to 

```bash
$ helm install heron-kube heron-charts/heron \
  --namespace heron \
  --set platform=aws
```

##### Using S3 uploader

You can make Heron to use S3 to distribute the user topologies. First you need to set up a S3 bucket and configure an IAM user with enough permissions over it. Get access keys for the user. Then you can deploy Heron like this:

```bash
$ helm install heron-kube heron-charts/heron \
  --namespace heron \
  --set platform=aws \
  --set uploader.class=s3 \
  --set uploader.s3Bucket=heron \
  --set uploader.s3PathPrefix=topologies \
  --set uploader.s3AccessKey=XXXXXXXXXXXXXXXXXXXX \
  --set uploader.s3SecretKey=XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX \
  --set uploader.s3Region=us-west-1
```

#### Bare metal

To run Heron on a bare metal Kubernetes cluster:

```bash
$ helm install heron-kube heron-charts/heron \
  --namespace heron \
  --set platform=baremetal
```

### Managing topologies

> When setting the `heron` CLI configuration, make sure that the cluster name matches the name of the Helm installation. This can be either the name auto-generated by Helm or the name you supplied via the `--name` flag upon installation (in some of the examples above, the `heron-kubernetes` name was used). Make sure to adjust the name accordingly if necessary.

Once all of the components have been successfully started up, you need to open up a proxy port to your Kubernetes cluster using the [`kubectl proxy`](https://kubernetes.io/docs/tasks/access-kubernetes-api/http-proxy-access-api/) command:

```bash
$ kubectl proxy -p 8001
```
> Note: All of the following Kubernetes specific urls are valid with the Kubernetes 1.10.0 release.
 
Now, verify that the Heron API server running on Minikube is available using curl:

```bash
$ curl http://localhost:8001/api/v1/namespaces/default/services/heron-kubernetes-apiserver:9000/proxy/api/v1/version
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

## Running topologies on Heron on Kubernetes

Once you have a Heron cluster up and running on Kubernetes via Helm, you can use the [`heron` CLI tool](user-manuals-heron-cli) like normal if you set the proper URL for the [Heron API server](deployment-api-server). When running Heron on Kubernetes, that URL is:

```bash
$ http://localhost:8001/api/v1/namespaces/default/services/heron-kubernetes-apiserver:9000/proxy
```

To set that URL:

```bash
$ heron config heron-kubernetes set service_url \
  http://localhost:8001/api/v1/namespaces/default/services/heron-kubernetes-apiserver:9000/proxy
```

To test your cluster, you can submit an example topology:

```bash
$ heron submit heron-kubernetes \
  ~/.heron/examples/heron-streamlet-examples.jar \
  org.apache.heron.examples.streamlet.WindowedWordCountTopology \
  WindowedWordCount
```
