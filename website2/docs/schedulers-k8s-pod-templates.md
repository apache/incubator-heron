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

> This document demonstrates how you can utilize custom [Pod Templates](https://kubernetes.io/docs/concepts/workloads/pods/#pod-templates) embedded in [Configuration Maps](https://kubernetes.io/docs/concepts/configuration/configmap/) for your computation nodes - i.e., Spouts and Bolts.

When you deploy a topology to Heron on Kubernetes, you may specify a Pod Template to be used on the computation nodes. This can be achieved by providing a *unique* Pod Template name, and embedding the Pod Template within a Configuration Map. By default, Heron will use a default Pod Template which includes minimal configurations.

Pod Templates will allow you to configure all aspects of the Pods where the computations occur. For instance, it might be desirable to set tighter security parameters in case of sensitive workloads or configure the metadata for the Pods to facilitate easier monitoring in `kubectl`. The use cases are boundless.

## Usage

To deploy a custom Pod Template to the Kubernetes with your workload, you must provide a valid Pod Template embedded in a valid Configuration Map. The name of the Pod Template *must be unique*, if not the first match for the specified Pod Template will be utilized.

It is highly advised that you validate your Pod Templates before placing them in a `ConfigMap` to isolate any validity issues. An example of a Pod Template is provided below, and is derived from the configuration for the Heron Tracker Pod:

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
Once you have a valid Pod Template you may proceed to place it in a `ConfigMap`. Please take care to specify a unique 


## How Heron on Kubernetes Works
