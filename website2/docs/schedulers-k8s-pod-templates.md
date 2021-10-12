---
id: schedulers-k8s-pod-templates
title: Kubernetes Pod Templates
sidebar_label:  Kubernetes Pod Templates
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

> This document demonstrates how you can utilize custom [Pod Templates](https://kubernetes.io/docs/concepts/workloads/pods/#pod-templates) embedded in [Configuration Maps](https://kubernetes.io/docs/concepts/configuration/configmap/) for your computation nodes - i.e., Spouts and Bolts. You may specify different Pod Templates for different topologies.

<br/>

When you deploy a topology to Heron on Kubernetes, you may specify a Pod Template to be used on the computation nodes. This can be achieved by providing a valid Pod Template, and embedding the Pod Template within a Configuration Map. By default, Heron will use a minimally configured Pod Template which is adequate to deploy a topology.

Pod Templates will allow you to configure most aspects of the Pods where the computations occur, with some exceptions. There are some aspects of Pods for which Heron will have the final say, and which will not be user-customizable. Please view the tables at the end of this document to identify what is set by Heron.

<br>

> System Administrators: You may wish to disable the ability to load custom Pod Templates. To achieve this, you must pass the `-D heron.kubernetes.pod.template.configmap.disabled=true` to the Heron API Server on the command line during boot. This command has been added to the Kubernetes configuration files to deploy the Heron API Server and can be uncommented. Please take care to ensure that the indentation is correct.

<br/>

## Preparation

To deploy a custom Pod Template to Kubernetes with your topology, you must provide a valid Pod Template embedded in a valid Configuration Map. We will be using the following variables throughout this document, some of which are reserved variable names:

* `POD-TEMPLATE-NAME`: This is the name of the Pod Template's YAML definition file. This is ***not*** a reserved variable and is a place-holder name.
* `CONFIG-MAP-NAME`: This is the name which will be used by the Configuration Map in which the Pod Template will be embedded by `kubectl`. This is ***not*** a reserved variable and is a place-holder name.
* `heron.kubernetes.pod.template.configmap.name`: This variable name used as the key passed to Heron for the `--config-property` on the CLI. This ***is*** a reserved variable name.

***NOTE***: Please do ***not*** use the `.` (period character) in the name of the `CONFIG-MAP-NAME`. This character will be used as a delimiter when submitting your topologies.

It is highly advised that you validate your Pod Templates before placing them in a `ConfigMap` to isolate any validity issues using a tool such as [Kubeval](https://kubeval.instrumenta.dev/).

### Pod Templates

An example of a Pod Template is provided below, and is derived from the configuration for the Heron Tracker Pod:

```yaml
apiVersion: v1
kind: PodTemplate
metadata:
  name: heron-tracker
  namespace: default
template:
  metadata:
    labels:
      app: heron-tracker
  spec:
    containers:
      - name: heron-tracker
        image: apache/heron:latest
        ports:
          - containerPort: 8888
            name: api-port
        resources:
          requests:
            cpu: "100m"
            memory: "200M"
          limits:
            cpu: "400m"
            memory: "512M"
```

You would need to save this file as `POD-TEMPLATE-NAME`. Once you have a valid Pod Template you may proceed to generate a `ConfigMap`.

### Configuration Maps

To generate a `ConfigMap` you will need to run the following command:

```bash
kubectl create configmap CONFIG-MAP-NAME --from-file path/to/POD-TEMPLATE-NAME
```

You may then want to verify the contents of the `ConfigMap` by running the following command:

```bash
kubectl get configmaps CONFIG-MAP-NAME -o yaml
```

The `ConfigMap` should appear similar to the one below for our example:

```yaml
apiVersion: v1
data:
  POD-TEMPLATE-NAME: |
    apiVersion: v1
    kind: PodTemplate
    metadata:
      name: heron-tracker
      namespace: default
    template:
      metadata:
        labels:
          app: heron-tracker
      spec:
        containers:
          - name: heron-tracker
            image: apache/heron:latest
            ports:
              - containerPort: 8888
                name: api-port
            resources:
              requests:
                cpu: "100m"
                memory: "200M"
              limits:
                cpu: "400m"
                memory: "512M"
kind: ConfigMap
metadata:
  creationTimestamp: "2021-09-27T21:55:30Z"
  name: CONFIG-MAP-NAME
  namespace: default
  resourceVersion: "1313"
  uid: ba001653-03d9-4ac8-804c-d2c55c974281
```

## Submitting

To use the `ConfigMap` for a topology you would submit with the additional flag `--confg-property`. The `--config-property key=value` takes a key value pair:

* Key: `heron.kubernetes.pod.template.configmap.name`
* Value: `CONFIG-MAP-NAME.POD-TEMPLATE-NAME`

Please note that you must concatenate `CONFIG-MAP-NAME` and `POD-TEMPLATE-NAME` with a **`.`** (period chracter).

For example:

```bash
heron submit kubernetes \
  --service-url=http://localhost:8001/api/v1/namespaces/default/services/heron-apiserver:9000/proxy \
  ~/.heron/examples/heron-api-examples.jar \
  org.apache.heron.examples.api.AckingTopology acking \
  --config-property heron.kubernetes.pod.template.configmap.name=CONFIG-MAP-NAME.POD-TEMPLATE-NAME
```

## Heron Configured Items in Pod Templates

Heron will locate the container named `executor` in the Pod Template and customize it as outlined below. All other containers within the Pod Template will remain unchanged.

### Executor Container

All metadata for the `executor` container will be overwritten by Heron. In some other cases, values from the Pod Template for the `executor` will be overwritten by Heron as outline below.

| Name | Description | Policy |
|---|---|---|
| `image` | The `executor` container's image. | Overwritten by Heron using values form the config.
| `env` | Environment variables are made available within the container. The `HOST` and `POD_NAME` keys are required by Heron and are thus reserved. | Merged with Heron's values taking precedence. Deduplication is based on `name`.
| `ports` | Port numbers opened within the container. Some of these port number are required by Heron and are thus reserved. The reserved ports are defined in Heron's constants as [`6001`-`6010`]. | Merged with Heron's values taking precedence. Deduplication is based on the `containerPort` value.
| `limits` | Heron will attempt to load values for `cpu` and `memory` from its configs. If these values are not provided in the containers specs, Heron will place values from its configs. | User input takes precedence over Heron's values. This allows for per job custom resource limits.
| `volumeMounts` | These are the mount points within the `executor` container for the `volumes` available in the Pod. | Merged with Heron's values taking precedence. Deduplication is based on the `name` value.
| Annotation: `prometheus.io/scrape` | Flag to indicate whether Prometheus logs can be scraped and is set to `true`. | Value is overridden by Heron. |
| Annotation `prometheus.io/port` | Port address for Prometheus log scraping and is set to `8080`. | Values are overridden by Heron.
| Annotation: Pod | Pod's revision/version hash. | Automatically set.
| Annotation: Service | Labels services can use to attach to the Pod. | Automatically set.
| Label: `app` | Name of the application lauching the Pod and is set to `Heron`. | Values are overridden by Heron.
| Label: `topology`| The name of topology which was provided when submitting. | User defined and supplied on the CLI.

### Pod

The following items will be set in the Pod Template's `spec` by Heron.

| Name | Description | Policy |
|---|---|---|
`terminationGracePeriodSeconds` | Grace period to wait before shutting down the Pod after a `SIGTERM` signal and is set to `0` seconds. | Values are overridden by Heron.
| `tolerations` | Attempts to colocate Pods with `tolerations` and `taints` onto nodes hosting Pods with matching `tolerations` and `taints`. <br>  Keys:<br>`node.kubernetes.io/not-ready` <br> `node.alpha.kubernetes.io/notReady` <br> `node.alpha.kubernetes.io/unreachable`. <br> Values (common):<br> `operator: "Exists"`<br> `effect: NoExecute`<br> `tolerationSeconds: 10L` | Values are overridden by Heron.
| `containers` | Configurations for containers to be launched within the Pod. | All `containers`, excluding the `executor`, are loaded as-is.
| `volumes` | Volumes to be made available to the entire Pod. | Merged with Heron's values taking precedence. Deduplication is based on the `name` value.
| `secretVolumes` | Secrets to be mounted as volumes within the Pod. | Loaded from the Heron configs if present.
