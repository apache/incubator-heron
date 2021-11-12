---
id: schedulers-k8s-persistent-volume-claims
title: Kubernetes Persistent Volume Claims via CLI
sidebar_label: Kubernetes Persistent Volume Claims (CLI)
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

> This document demonstrates how you can utilize both static and dynamically backed [Persistent Volume Claims](https://kubernetes.io/docs/concepts/storage/dynamic-provisioning/) in the `Executor` containers. You will need to enable Dynamic Provisioning in your Kubernetes cluster to proceed to use the dynamic provisioning functionality.

<br/>

It is possible to leverage Persistent Volumes with custom Pod Templates. The CLI commands allow you to configure a Persistent Volume Claim (dynamically or statically backed) when you submit your topology. They also permit you to configure a Persistent Volume without a custom Pod Template. The CLI commands override any configurations you may have present in the Pod Template, but Heron's configurations will take precedence over all others.

**Note:** Heron ***will*** remove any dynamically backed Persistent Volume Claims it creates when a topology is terminated. Please be aware that Heron uses the following `Labels` to locate the claims it has created:
```yaml
metadata:
  labels:
    topology: <topology-name>
    onDemand: true
```

<br>

> ***System Administrators:***
>
> * You may wish to disable the ability to configure dynamic Persistent Volume Claims specified on the CLI. To achieve this, you must pass the define option `-D heron.kubernetes.persistent.volume.claims.cli.disabled=true` to the Heron API Server on the command line during boot. This command has been added to the Kubernetes configuration files to deploy the Heron API Server and is set to `false` by default.
> * If you have a custom `Role`/`ClusterRole` for the Heron API Server you will need to ensure the `ServiceAccount` attached to the API server has the correct permissions to access the `Persistent Volume Claim`s:
>
>```yaml
>rules:
>- apiGroups: 
>  - ""
>  resources: 
>  - persistentvolumeclaims
>  verbs: 
>  - create
>  - delete
>  - get
>  - list
>```

<br>

## Usage

To configure a Persistent Volume Claim you must use the `--config-property` option with the `heron.kubernetes.volumes.persistentVolumeClaim.` command prefix. Heron will not validate your Persistent Volume Claim configurations, so please validate them to ensure they are well-formed. All names must comply with the [*lowercase RFC-1123*](https://kubernetes.io/docs/concepts/overview/working-with-objects/names/) standard.

The command pattern is as follows:
`heron.kubernetes.volumes.persistentVolumeClaim.[VOLUME NAME].[OPTION]=[VALUE]`

The currently supported CLI `options` are:

* `storageClass`
* `sizeLimit`
* `accessModes`
* `volumeMode`
* `path`
* `subPath`

***Note:*** The `accessModes` must be a comma separated list of values *without* any white space.

***Note:*** If a `storageClassName` is specified and there are no matching Persistent Volumes then [dynamic provisioning](https://kubernetes.io/docs/concepts/storage/dynamic-provisioning/) must be enabled. Kubernetes will attempt to locate a Persistent Volume that matches the `storageClassName` before it attempts to use dynamic provisioning. If a `storageClassName` is not specified there must be [Persistent Volumes](https://kubernetes.io/docs/tasks/configure-pod-container/configure-persistent-volume-storage/) provisioned manually with the `storageClassName` of `standard`.

<br>

### Example

An example series of commands and the `YAML` entries they make in their respective configurations are as follows.

***Dynamic:***

```bash
--config-property heron.kubernetes.volumes.persistentVolumeClaim.volumenameofchoice.storageClassName=storage-class-name-of-choice
--config-property heron.kubernetes.volumes.persistentVolumeClaim.volumenameofchoice.accessModes=comma,separated,list
--config-property heron.kubernetes.volumes.persistentVolumeClaim.volumenameofchoice.sizeLimit=555Gi
--config-property heron.kubernetes.volumes.persistentVolumeClaim.volumenameofchoice.volumeMode=volume-mode-of-choice
--config-property heron.kubernetes.volumes.persistentVolumeClaim.volumenameofchoice.path=/path/to/mount
--config-property heron.kubernetes.volumes.persistentVolumeClaim.volumenameofchoice.subPath=/sub/path/to/mount
```

Generated `Persistent Volume Claim`:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  labels:
    app: heron
    onDemand: "true"
    topology: [Topology-Name]
  name: volumenameofchoice-[Topology Name]-[Ordinal]
spec:
  accessModes:
  - comma
  - separated
  - list
  resources:
    requests:
      storage: 555Gi
  storageClassName: storage-class-name-of-choice
  volumeMode: volume-mode-of-choice
```

Pod Spec entries for `Volume`:

```yaml
volumes:
  - name: volumenameofchoice
    persistentVolumeClaim:
      claimName: volumenameofchoice-[Topology Name]-[Ordinal]
```

`Executor` container entries for `Volume Mounts`:

```yaml
volumeMounts:
  - mountPath: /path/to/mount
    subPath: /sub/path/to/mount
    name: volumenameofchoice
```

<br>

***Static:***

```bash
--config-property heron.kubernetes.volumes.persistentVolumeClaim.volumenameofchoice.accessModes=comma,separated,list
--config-property heron.kubernetes.volumes.persistentVolumeClaim.volumenameofchoice.sizeLimit=555Gi
--config-property heron.kubernetes.volumes.persistentVolumeClaim.volumenameofchoice.volumeMode=volume-mode-of-choice
--config-property heron.kubernetes.volumes.persistentVolumeClaim.volumenameofchoice.path=/path/to/mount
--config-property heron.kubernetes.volumes.persistentVolumeClaim.volumenameofchoice.subPath=/sub/path/to/mount
```

Generated `Persistent Volume Claim`:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  labels:
    app: heron
    onDemand: "true"
    topology: [Topology-Name]
  name: volumenameofchoice-[Topology Name]-[Ordinal]
spec:
  accessModes:
  - comma
  - separated
  - list
  resources:
    requests:
      storage: 555Gi
  storageClassName: standard
  volumeMode: volume-mode-of-choice
```

Pod Spec entries for `Volume`:

```yaml
volumes:
  - name: volumenameofchoice
    persistentVolumeClaim:
      claimName: volumenameofchoice-[Topology Name]-[Ordinal]
```

`Executor` container entries for `Volume Mounts`:

```yaml
volumeMounts:
  - mountPath: /path/to/mount
    subPath: /sub/path/to/mount
    name: volumenameofchoice
```

<br>

## Submitting

An example of sumbitting a topology using the *dynamic* example CLI commands above:

```bash
heron submit kubernetes \
  --service-url=http://localhost:8001/api/v1/namespaces/default/services/heron-apiserver:9000/proxy \
  ~/.heron/examples/heron-api-examples.jar \
  org.apache.heron.examples.api.AckingTopology acking \
--config-property heron.kubernetes.volumes.persistentVolumeClaim.volumenameofchoice.storageClassName=storage-class-name-of-choice \
--config-property heron.kubernetes.volumes.persistentVolumeClaim.volumenameofchoice.accessModes=comma,separated,list \
--config-property heron.kubernetes.volumes.persistentVolumeClaim.volumenameofchoice.sizeLimit=555Gi \
--config-property heron.kubernetes.volumes.persistentVolumeClaim.volumenameofchoice.volumeMode=volume-mode-of-choice \
--config-property heron.kubernetes.volumes.persistentVolumeClaim.volumenameofchoice.path=/path/to/mount \
--config-property heron.kubernetes.volumes.persistentVolumeClaim.volumenameofchoice.subPath=/sub/path/to/mount
```

## Required and Optional Configuration Items

The following table outlines CLI options which are either ***required*** ( &#x2611; ) or ***optional*** ( &#x2612; ) depending on if you are using dynamic or statically backed `Volumes`.

| Option | Dynamic | Static |
|---|---|---|
| `VOLUME NAME` | &#x2611; | &#x2611;
| `path` | &#x2611; | &#x2611;
| `subPath` | &#x2612; | &#x2612;
| `storageClassName` | &#x2611; | &#x2612;
| `accessModes` | &#x2611; | &#x2611;
| `sizeLimit` | &#x2612; | &#x2612;
| `volumeMode` | &#x2612; | &#x2612;

## Configuration Items Created and Entries Made

The configuration items and entries in the tables below will made in their respective areas.

One `Persistent Volume Claim` (if dynamically backed), a `Volume`, and a `VolumeMount` will be created for each `volume name` which you specify.

| Name | Description | Policy |
|---|---|---|
| `VOLUME NAME` | The `name` of the `Volume`. | Entries made in the `Persistent Volume Claim`'s spec, the Pod Spec's `Volumes`, and the `executor` containers `volumeMounts`.
| `path` | The `mountPath` of the `Volume`. | Entries made in the `executor` containers `volumeMounts`.
| `subPath` | The `subPath` of the `Volume`. | Entries made in the `executor` containers `volumeMounts`.
| `storageClassName` | The identifier name used to reference the dynamic `StorageClass`. | Entries made in the `Persistent Volume Claim` and Pod Spec's `Volume`.
| `accessModes` | A comma separated list of access modes. | Entries made in the `Persistent Volume Claim`.
| `sizeLimit` | A resource request for storage space. | Entries made in the `Persistent Volume Claim`.
| `volumeMode` | Either `FileSystem` (default) or `Block` (raw block). [Read more](https://kubernetes.io/docs/concepts/storage/_print/#volume-mode). | Entries made in the `Persistent Volume Claim`.
| Labels | Two labels for `topology` and `onDemand` provisioning are added. | These labels are only added to dynamically backed `Persistent Volume Claim`'s created by Heron to support the removal of any claims created when a topology is terminated.
