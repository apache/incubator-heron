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

When you deploy a topology to Heron on Kubernetes, you may specify a Pod Template to be used on the computation nodes. This can be achieved by providing a *unique* Pod Template name, and embedding the Pod Template within a Configuration Map. By default, Heron will use a default Pod Template which includes minimal configurations.

Pod Templates will allow you to configure all aspects of the Pods where the computations occur. For instance, it might be desirable to set tighter security parameters in case of sensitive topologies or configure the metadata for the Pods to facilitate easier monitoring in `kubectl`. The use cases are boundless.

## Preparation

To deploy a custom Pod Template to the Kubernetes with your topology, you must provide a valid Pod Template embedded in a valid Configuration Map. The name of the Pod Template *must be unique*, if not the first match for the specified Pod Template will be utilized.

It is highly advised that you validate your Pod Templates before placing them in a `ConfigMap` to isolate any validity issues.

We will be using `POD-TEMPLATE-NAME` to refer to the name of key item in the `ConfigMap` and `CONFIGMAP-NAME` to refer to the name of the `ConfigMap`.

### Pod Templates

An example of a Pod Template is provided below, and is derived from the configuration for the Heron Tracker Pod:

```yaml
apiVersion: apps/v1
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

You would need to save this file as `POD-TEMPLATE-NAME.yaml`. Once you have a valid Pod Template you may proceed to generate a `ConfigMap`.

### Configuration Maps

TO generate a `ConfigMap` you will need to run the following command:

```bash
kubectl create configmap CONFIG-MAP-NAME --from-file=path/to/POD-TEMPLATE-NAME.yaml
```

If you then run the following command:

```bash
kubectl get configmaps CONFIG-MAP-NAME -o yaml
```

The `ConfigMap` should appear similar to the one below for our example:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  creationTimestamp: 2021-09-24T18:52:05Z
  name: CONFIG-MAP-NAME
  namespace: default
  resourceVersion: "516"
  uid: b4952dc3-d670-11e5-8cd0-68f728db1985
data:
  POD-TEMPLATE-NAME: |
    apiVersion: apps/v1
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
  SOME-OTHER-KEY: some_other_data_item
```

## Submitting

To use the `ConfigMap` for a topology you would submit with the additional flag `--confg-property`. The `--config-property` take a key value pair:

* Key: `heron.kubernetes.pod.template.configmap.name`
* Value: `POD-TEMPLATE-NAME`

For example:

```bash
heron submit kubernetes \
  --service-url=http://localhost:8001/api/v1/namespaces/default/services/heron-apiserver:9000/proxy \
  ~/.heron/examples/heron-api-examples.jar \
  org.apache.heron.examples.api.AckingTopology acking \
  --config-property heron.kubernetes.pod.template.configmap.name=POD-TEMPLATE-NAME
```
