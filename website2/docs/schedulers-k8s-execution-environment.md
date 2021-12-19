---
id: schedulers-k8s-execution-environment
title: Kubernetes Execution Environment Customization
hide_title: true
sidebar_label: Kubernetes Environment Customization
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

# Customizing the Heron Execution Environment in Kubernetes

This document demonstrates how you can customize various aspects of the Heron execution environment when using the Kubernetes Scheduler.

<br>

---

<br>

## Customizing a Topology's Execution Environment Using Pod Templates

<br>

> This section demonstrates how you can utilize custom [Pod Templates](https://kubernetes.io/docs/concepts/workloads/pods/#pod-templates) embedded in [Configuration Maps](https://kubernetes.io/docs/concepts/configuration/configmap/) for your Topology's `Executor`s and `Manager` (hereinafter referred to as `Heron containers`). You may specify different Pod Templates for different topologies.

<br/>

When you deploy a topology to Heron on Kubernetes, you may specify individual Pod Templates to be used in your topology's `Executor`s and `Manager`. This can be achieved by providing valid Pod Templates, and embedding the Pod Templates in Configuration Maps. By default, Heron will use a minimally configured Pod Template which is adequate to deploy a topology.

Pod Templates will allow you to configure most aspects of your topology's execution environment, with some exceptions. There are some aspects of Pods for which Heron will have the final say, and which will not be user-customizable. Please view the [tables](#heron-configured-items-in-pod-templates) at the end of this section to identify what is set by Heron.

<br>

> ***System Administrators:***
>
> * You may wish to disable the ability to load custom Pod Templates. To achieve this, you must pass the define option `-D heron.kubernetes.pod.template.disabled=true` to the Heron API Server on the command line when launching. This command has been added to the Kubernetes configuration files to deploy the Heron API Server and is set to `false` by default.
> * If you have a custom `Role` for the Heron API Server you will need to ensure the `ServiceAccount` attached to the API server, via a `RoleBinding`, has the correct permissions to access the `ConfigMaps`:
>
>```yaml
>rules:
>- apiGroups: 
>  - ""
>  resources: 
>  - configmaps
>  verbs: 
>  - get
>  - list
>```

<br>

### Preparation

To deploy a custom Pod Template to Kubernetes with your topology, you must provide a valid Pod Template embedded in a valid Configuration Map. We will be using the following variables throughout this document, some of which are reserved variable names:

* `POD-TEMPLATE-NAME`: This is the name of the Pod Template's YAML definition file. This is ***not*** a reserved variable and is a place-holder name.
* `CONFIG-MAP-NAME`: This is the name that will be used by the Configuration Map in which the Pod Template will be embedded by `kubectl`. This is ***not*** a reserved variable and is a place-holder name.
* `heron.kubernetes.[executor | manager].pod.template`: This variable name is used as the key passed to Heron for the `--config-property` on the CLI. This ***is*** a reserved variable name.

***NOTE***: Please do ***not*** use the `.` (period character) in the name of the `CONFIG-MAP-NAME`. This character will be used as a delimiter when submitting your topologies.

It is highly advised that you validate your Pod Templates before placing them in a `ConfigMap` to isolate any validity issues using a tool such as [Kubeval](https://kubeval.instrumenta.dev/) or the built-in `dry-run` functionality in Kubernetes. Whilst these tools are handy, they will not catch all potential errors in Kubernetes configurations.

***NOTE***: When submitting a Pod Template to customize an `Executor` or `Manager`, Heron will look for containers named `executor` and `manager` respectively. These containers will be modified to support the functioning of Heron, please read further below.

#### Pod Templates

An example of the Pod Template format is provided below, and is derived from the configuration for the Heron Tracker Pod:

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

#### Configuration Maps

> You must place the `ConfigMap` in the same namespace as the Heron API Server using the `--namespace` option in the commands below if the API Server is not in the `default` namespace.

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

### Submitting

To use the `ConfigMap` for a topology you would will need to submit with the additional flag `--confg-property`. The `--config-property key=value` takes a key-value pair:

* Key: `heron.kubernetes.[executor | manager].pod.template`
* Value: `CONFIG-MAP-NAME.POD-TEMPLATE-NAME`

Please note that you must concatenate `CONFIG-MAP-NAME` and `POD-TEMPLATE-NAME` with a **`.`** (period character).

For example:

```bash
heron submit kubernetes \
  --service-url=http://localhost:8001/api/v1/namespaces/default/services/heron-apiserver:9000/proxy \
  ~/.heron/examples/heron-api-examples.jar \
  org.apache.heron.examples.api.AckingTopology acking \
  --config-property heron.kubernetes.executor.pod.template=CONFIG-MAP-NAME.POD-TEMPLATE-NAME \
  --config-property heron.kubernetes.manager.pod.template=CONFIG-MAP-NAME.POD-TEMPLATE-NAME
```

### Heron Configured Items in Pod Templates

Heron will locate the containers named `executor` and/or `manager` in the Pod Template and customize them as outlined below. All other containers within the Pod Templates will remain unchanged.

#### Executor and Manager Containers

All metadata for the `Heron containers` will be overwritten by Heron. In some other cases, values from the Pod Template for the `executor` and `manager` will be overwritten by Heron as outlined below.

| Name | Description | Policy |
|---|---|---|
| `image` | The `Heron container`'s image. | Overwritten by Heron using values from the config.
| `env` | Environment variables are made available within the container. The `HOST` and `POD_NAME` keys are required by Heron and are thus reserved. | Merged with Heron's values taking precedence. Deduplication is based on `name`.
| `ports` | Port numbers opened within the container. Some of these port numbers are required by Heron and are thus reserved. The reserved ports are defined in Heron's constants as [`6001`-`6010`]. | Merged with Heron's values taking precedence. Deduplication is based on the `containerPort` value.
| `limits` <br> `requests` | Heron will attempt to load values for `cpu` and `memory` from configs. | Heron's values take precedence over those in the Pod Templates.
| `volumeMounts` | These are the mount points within the `Heron container` for the `volumes` available in the Pod. | Merged with Heron's values taking precedence. Deduplication is based on the `name` value.
| Annotation: `prometheus.io/scrape` | Flag to indicate whether Prometheus logs can be scraped and is set to `true`. | Value is overridden by Heron. |
| Annotation `prometheus.io/port` | Port address for Prometheus log scraping and is set to `8080`. | Values are overridden by Heron.
| Annotation: Pod | Pod's revision/version hash. | Automatically set.
| Annotation: Service | Labels services can use to attach to the Pod. | Automatically set.
| Label: `app` | Name of the application launching the Pod and is set to `Heron`. | Values are overridden by Heron.
| Label: `topology`| The name of topology which was provided when submitting. | User-defined and supplied on the CLI.

#### Pod

The following items will be set in the Pod Template's `spec` by Heron.

| Name | Description | Policy |
|---|---|---|
`terminationGracePeriodSeconds` | Grace period to wait before shutting down the Pod after a `SIGTERM` signal and is set to `0` seconds. | Values are overridden by Heron.
| `tolerations` | Attempts to schedule Pods with `taints` onto nodes hosting Pods with matching `taints`. The entries below are included by default. <br>  Keys:<br>`node.kubernetes.io/not-ready` <br> `node.kubernetes.io/unreachable` <br> Values (common):<br> `operator: Exists`<br> `effect: NoExecute`<br> `tolerationSeconds: 10L` | Merged with Heron's values taking precedence. Deduplication is based on the `key` value.
| `containers` | Configurations for containers to be launched within the Pod. | All containers, excluding the `Heron container`s, are loaded as-is.
| `volumes` | Volumes to be made available to the entire Pod. | Merged with Heron's values taking precedence. Deduplication is based on the `name` value.
| `secretVolumes` | Secrets to be mounted as volumes within the Pod. | Loaded from the Heron configs if present.

<br>

---
<br>

## Adding Persistent Volumes via the Command-line Interface

<br>

> This section demonstrates how you can utilize both static and dynamically backed [Persistent Volume Claims](https://kubernetes.io/docs/concepts/storage/dynamic-provisioning/) in the `Executor` and `Manager` containers (hereinafter referred to as `Heron containers`). You will need to enable Dynamic Provisioning in your Kubernetes cluster to proceed to use the dynamic provisioning functionality.

<br/>

It is possible to leverage Persistent Volumes with custom Pod Templates but the Volumes you add will be shared between all  `Executor` Pods in the topology when customizing the `Executor`s.

The CLI commands allow you to configure a Persistent Volume Claim (dynamically or statically backed) which will be unique and isolated to each Pod and mounted in a single `Heron container` when you submit your topology with a claim name of `OnDemand`. Using any claim name other than on `OnDemand` will permit you to configure a shared Persistent Volume without a custom Pod Template which will be shared between all `Executor` Pods when customizing them. The CLI commands override any configurations you may have present in the Pod Template, but Heron's configurations will take precedence over all others.

Some use cases include process checkpointing, caching of results for later use in the process, intermediate results which could prove useful in analysis (ETL/ELT to a data lake or warehouse), as a source of data enrichment, etc.

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
> * You may wish to disable the ability to configure Persistent Volume Claims specified via the CLI. To achieve this, you must pass the define option `-D heron.kubernetes.volume.from.cli.disabled=true`to the Heron API Server on the command line when launching. This command has been added to the Kubernetes configuration files to deploy the Heron API Server and is set to `false` by default.
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
>  - deletecollection
>```

<br>

### Usage

To configure a Persistent Volume Claim you must use the `--config-property` option with the `heron.kubernetes.[executor | manager].volumes.persistentVolumeClaim.` command prefix. Heron will not validate your Persistent Volume Claim configurations, so please validate them to ensure they are well-formed. All names must comply with the [*lowercase RFC-1123*](https://kubernetes.io/docs/concepts/overview/working-with-objects/names/) standard.

The command pattern is as follows:
`heron.kubernetes.[executor | manager].volumes.persistentVolumeClaim.[VOLUME NAME].[OPTION]=[VALUE]`

The currently supported CLI `options` are:

* `claimName`
* `storageClass`
* `sizeLimit`
* `accessModes`
* `volumeMode`
* `path`
* `subPath`
* `readOnly`

***Note:*** A `claimName` of `OnDemand` will create unique Volumes for each `Heron container` as well as deploy a Persistent Volume Claim for each Volume. Any other claim name will result in a shared Volume being created between all Pods in the topology.

***Note:*** The `accessModes` must be a comma-separated list of values *without* any white space. Valid values can be found in the [Kubernetes documentation](https://kubernetes.io/docs/concepts/storage/persistent-volumes/#access-modes).

***Note:*** If a `storageClassName` is specified and there are no matching Persistent Volumes then [dynamic provisioning](https://kubernetes.io/docs/concepts/storage/dynamic-provisioning/) must be enabled. Kubernetes will attempt to locate a Persistent Volume that matches the `storageClassName` before it attempts to use dynamic provisioning. If a `storageClassName` is not specified there must be [Persistent Volumes](https://kubernetes.io/docs/tasks/configure-pod-container/configure-persistent-volume-storage/) provisioned manually. For more on statically and dynamically provisioned volumes please read [this](https://kubernetes.io/docs/concepts/storage/persistent-volumes/#lifecycle-of-a-volume-and-claim).

<br>

#### Example

A series of example commands to add `Persistent Volumes` to `Executor`s, and the `YAML` entries they make in their respective configurations, are as follows.

***Dynamic:***

```bash
--config-property heron.kubernetes.executor.volumes.persistentVolumeClaim.volumenameofchoice.claimName=OnDemand
--config-property heron.kubernetes.executor.volumes.persistentVolumeClaim.volumenameofchoice.storageClassName=storage-class-name-of-choice
--config-property heron.kubernetes.executor.volumes.persistentVolumeClaim.volumenameofchoice.accessModes=comma,separated,list
--config-property heron.kubernetes.executor.volumes.persistentVolumeClaim.volumenameofchoice.sizeLimit=555Gi
--config-property heron.kubernetes.executor.volumes.persistentVolumeClaim.volumenameofchoice.volumeMode=volume-mode-of-choice
--config-property heron.kubernetes.executor.volumes.persistentVolumeClaim.volumenameofchoice.path=/path/to/mount
--config-property heron.kubernetes.executor.volumes.persistentVolumeClaim.volumenameofchoice.subPath=/sub/path/to/mount
```

Generated `Persistent Volume Claim`:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  labels:
    app: heron
    onDemand: "true"
    topology: <topology-name>
  name: volumenameofchoice-<topology-name>-[Ordinal]
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
      claimName: volumenameofchoice-<topology-name>-[Ordinal]
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
--config-property heron.kubernetes.executor.volumes.persistentVolumeClaim.volumenameofchoice.claimName=OnDemand
--config-property heron.kubernetes.executor.volumes.persistentVolumeClaim.volumenameofchoice.accessModes=comma,separated,list
--config-property heron.kubernetes.executor.volumes.persistentVolumeClaim.volumenameofchoice.sizeLimit=555Gi
--config-property heron.kubernetes.executor.volumes.persistentVolumeClaim.volumenameofchoice.volumeMode=volume-mode-of-choice
--config-property heron.kubernetes.executor.volumes.persistentVolumeClaim.volumenameofchoice.path=/path/to/mount
--config-property heron.kubernetes.executor.volumes.persistentVolumeClaim.volumenameofchoice.subPath=/sub/path/to/mount
```

Generated `Persistent Volume Claim`:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  labels:
    app: heron
    onDemand: "true"
    topology: <topology-name>
  name: volumenameofchoice-<topology-name>-[Ordinal]
spec:
  accessModes:
  - comma
  - separated
  - list
  resources:
    requests:
      storage: 555Gi
  storageClassName: ""
  volumeMode: volume-mode-of-choice
```

Pod Spec entries for `Volume`:

```yaml
volumes:
  - name: volumenameofchoice
    persistentVolumeClaim:
      claimName: volumenameofchoice-<topology-name>-[Ordinal]
```

`Executor` container entries for `Volume Mounts`:

```yaml
volumeMounts:
  - mountPath: /path/to/mount
    subPath: /sub/path/to/mount
    name: volumenameofchoice
```

<br>

### Submitting

A series of example commands to sumbit a topology using the *dynamic* example CLI commands above:

```bash
heron submit kubernetes \
  --service-url=http://localhost:8001/api/v1/namespaces/default/services/heron-apiserver:9000/proxy \
  ~/.heron/examples/heron-api-examples.jar \
  org.apache.heron.examples.api.AckingTopology acking \
--config-property heron.kubernetes.executor.volumes.persistentVolumeClaim.volumenameofchoice.claimName=OnDemand \
--config-property heron.kubernetes.executor.volumes.persistentVolumeClaim.volumenameofchoice.storageClassName=storage-class-name-of-choice \
--config-property heron.kubernetes.executor.volumes.persistentVolumeClaim.volumenameofchoice.accessModes=comma,separated,list \
--config-property heron.kubernetes.executor.volumes.persistentVolumeClaim.volumenameofchoice.sizeLimit=555Gi \
--config-property heron.kubernetes.executor.volumes.persistentVolumeClaim.volumenameofchoice.volumeMode=volume-mode-of-choice \
--config-property heron.kubernetes.executor.volumes.persistentVolumeClaim.volumenameofchoice.path=/path/to/mount \
--config-property heron.kubernetes.executor.volumes.persistentVolumeClaim.volumenameofchoice.subPath=/sub/path/to/mount
```

### Required and Optional Configuration Items

The following table outlines CLI options which are either ***required*** ( &#x2705; ), ***optional*** ( &#x2754; ), or ***not available*** ( &#x274c; ) depending on if you are using dynamically/statically backed or shared `Volume`s.

| Option | Dynamic | Static | Shared
|---|---|---|---|
| `VOLUME NAME` | &#x2705; | &#x2705; | &#x2705;
| `claimName` | `OnDemand` | `OnDemand` | A valid name
| `path` | &#x2705; | &#x2705; | &#x2705;
| `subPath` | &#x2754; | &#x2754; | &#x2754;
| `storageClassName` | &#x2705; | &#x274c; | &#x274c;
| `accessModes` | &#x2705; | &#x2705; | &#x274c;
| `sizeLimit` | &#x2754; | &#x2754; | &#x274c;
| `volumeMode` | &#x2754; | &#x2754; | &#x274c;
| `readOnly` | &#x2754; | &#x2754; | &#x2754;

<br>

***Note:*** The `VOLUME NAME` will be extracted from the CLI command and a `claimName` is a always required.

<br>

### Configuration Items Created and Entries Made

The configuration items and entries in the tables below will made in their respective areas.

A `Volume` and a `Volume Mount` will be created for each `volume name` which you specify. Additionally, one `Persistent Volume Claim` will be created for each `Volume` specified as dynamic using the `OnDemand` claim name.

| Name | Description | Policy |
|---|---|---|
| `VOLUME NAME` | The `name` of the `Volume`. | Entries made in the `Persistent Volume Claim`'s spec, the Pod Spec's `Volumes`, and the `Heron container`'s `volumeMounts`.
| `claimName` | A Claim name for the Persistent Volume. | If `OnDemand` is provided as the parameter then a unique Volume and Persistent Volume Claim will be created. Any other name will result in a shared Volume between all Pods in the topology with only a Volume and Volume Mount being added.
| `path` | The `mountPath` of the `Volume`. | Entries made in the `Heron container`'s `volumeMounts`.
| `subPath` | The `subPath` of the `Volume`. | Entries made in the `Heron container`'s `volumeMounts`.
| `storageClassName` | The identifier name used to reference the dynamic `StorageClass`. | Entries made in the `Persistent Volume Claim` and Pod Spec's `Volume`.
| `accessModes` | A comma-separated list of [access modes](https://kubernetes.io/docs/concepts/storage/persistent-volumes/#access-modes). | Entries made in the `Persistent Volume Claim`.
| `sizeLimit` | A resource request for storage space [units](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#meaning-of-memory). | Entries made in the `Persistent Volume Claim`.
| `volumeMode` | Either `FileSystem` (default) or `Block` (raw block). [Read more](https://kubernetes.io/docs/concepts/storage/_print/#volume-mode). | Entries made in the `Persistent Volume Claim`.
| Labels | Two labels for `topology` and `onDemand` provisioning are added. | These labels are only added to dynamically backed `Persistent Volume Claim`s created by Heron to support the removal of any claims created when a topology is terminated.

<br>

---

<br>

## Adding Empty Directory, Host Path, and Nework File System Volumes via the Command-line Interface

<br>

> This section demonstrates how you can specify configurations for `Empty Dir`, `Host Path`, and `NFS` volumes via the Command Line Interface during the submit process.

<br/>

It is possible to allocate and configure Volumes with Pod Templates but the CLI commands extend this to being able to specify Volumes at submission time.

<br>

> ***System Administrators:***
>
> * You may wish to disable the ability to configure Volume configurations specified via the CLI. To achieve this, you must pass the define option `-D heron.kubernetes.volume.from.cli.disabled=true`to the Heron API Server on the command line when launching. This command has been added to the Kubernetes configuration files to deploy the Heron API Server and is set to `false` by default.
> * &#x26a0; ***WARNING*** &#x26a0; `Host Path` volumes have inherent [security concerns](https://kubernetes.io/docs/concepts/storage/volumes/#hostpath). `Host Path`s can breach the containment provided by containerization and should be exclusively used with volume mounts set to `read-only`, with usage limited to testing and development environments.

<br>

### Usage

To configure a Volume on the CLI you must use the `--config-property` option in combination with the following prefixes:

 * [Empty Directory](https://kubernetes.io/docs/concepts/storage/volumes/#emptydir): `heron.kubernetes.[executor | manager].volumes.emptyDir.`
 * [Host Path](https://kubernetes.io/docs/concepts/storage/volumes/#hostpath): `heron.kubernetes.[executor | manager].volumes.hostPath.`
 * [Network File System](https://kubernetes.io/docs/concepts/storage/volumes/#nfs): `heron.kubernetes.[executor | manager].volumes.nfs.`

 Heron will not validate your Volume configurations, so please validate them to ensure they are well-formed. All Volume names must comply with the [*lowercase RFC-1123*](https://kubernetes.io/docs/concepts/overview/working-with-objects/names/) standard.

The command patterns are as follows:

 * Empty Directory: `heron.kubernetes.[executor | manager].volumes.emptyDir.[VOLUME NAME].[OPTION]=[VALUE]`
 * Host Path: `heron.kubernetes.[executor | manager].volumes.hostPath.[VOLUME NAME].[OPTION]=[VALUE]`
 * Network File System: `heron.kubernetes.[executor | manager].volumes.nfs.[VOLUME NAME].[OPTION]=[VALUE]`

The currently supported CLI `options` are:

* `medium`
* `type`
* `server`
* `sizeLimit`
* `pathOnHost`
* `pathOnNFS`
* `path`
* `subPath`
* `readOnly`

<br>

#### Example

A series of example commands to add Volumes to a `Manager`, and the `YAML` entries they make in their respective configurations, are as follows.

***Empty Directory:***

```bash
--config-property heron.kubernetes.manager.volumes.emptyDir.manager-empty-dir.medium="Memory"
--config-property heron.kubernetes.manager.volumes.emptyDir.manager-empty-dir.sizeLimit="50Mi"
--config-property heron.kubernetes.manager.volumes.emptyDir.manager-empty-dir.path="empty/dir/path"
--config-property heron.kubernetes.manager.volumes.emptyDir.manager-empty-dir.subPath="empty/dir/sub/path"
--config-property heron.kubernetes.manager.volumes.emptyDir.manager-empty-dir.readOnly="true"
```

Generated `Volume` entry:

```yaml
volumes:
- emptyDir:
    medium: Memory
    sizeLimit: 50Mi
  name: manager-empty-dir
```

Generated `Volume Mount` entry:

```yaml
volumeMounts:
- mountPath: empty/dir/path
  name: manager-empty-dir
  readOnly: true
  subPath: empty/dir/sub/path
```

<br>

***Host Path:***

```bash
--config-property heron.kubernetes.manager.volumes.hostPath.manager-host-path.type="File"
--config-property heron.kubernetes.manager.volumes.hostPath.manager-host-path.pathOnHost="/dev/null"
--config-property heron.kubernetes.manager.volumes.hostPath.manager-host-path.path="host/path/path"
--config-property heron.kubernetes.manager.volumes.hostPath.manager-host-path.subPath="host/path/sub/path"
--config-property heron.kubernetes.manager.volumes.hostPath.manager-host-path.readOnly="true"
```

Generated `Volume` entry:

```yaml
volumes:
- hostPath:
    path: /dev/null
    type: File
  name: manager-host-path
```

Generated `Volume Mount` entry:

```yaml
volumeMounts:
- mountPath: host/path/path
  name: manager-host-path
  readOnly: true
  subPath: host/path/sub/path
```

<br>

***NFS:***

```bash
--config-property heron.kubernetes.manager.volumes.nfs.manager-nfs.server="nfs-server.address"
--config-property heron.kubernetes.manager.volumes.nfs.manager-nfs.readOnly="true"
--config-property heron.kubernetes.manager.volumes.nfs.manager-nfs.pathOnNFS="/dev/null"
--config-property heron.kubernetes.manager.volumes.nfs.manager-nfs.path="nfs/path"
--config-property heron.kubernetes.manager.volumes.nfs.manager-nfs.subPath="nfs/sub/path"
--config-property heron.kubernetes.manager.volumes.nfs.manager-nfs.readOnly="true"
```

Generated `Volume` entry:

```yaml
volumes:
- name: manager-nfs
  nfs:
    path: /dev/null
    readOnly: true
    server: nfs-server.address
```

Generated `Volume Mount` entry:

```yaml
volumeMounts:
- mountPath: nfs/path
  name: manager-nfs
  readOnly: true
  subPath: nfs/sub/path
```

<br>

### Submitting

A series of example commands to sumbit a topology using the example CLI commands above:

```bash
heron submit kubernetes \
  --service-url=http://localhost:8001/api/v1/namespaces/default/services/heron-apiserver:9000/proxy \
  ~/.heron/examples/heron-api-examples.jar \
  org.apache.heron.examples.api.AckingTopology acking \
\
--config-property heron.kubernetes.manager.volumes.emptyDir.manager-empty-dir.medium="Memory" \
--config-property heron.kubernetes.manager.volumes.emptyDir.manager-empty-dir.sizeLimit="50Mi" \
--config-property heron.kubernetes.manager.volumes.emptyDir.manager-empty-dir.path="empty/dir/path" \
--config-property heron.kubernetes.manager.volumes.emptyDir.manager-empty-dir.subPath="empty/dir/sub/path" \
--config-property heron.kubernetes.manager.volumes.emptyDir.manager-empty-dir.readOnly="true" \
\
--config-property heron.kubernetes.manager.volumes.hostPath.manager-host-path.type="File" \
--config-property heron.kubernetes.manager.volumes.hostPath.manager-host-path.pathOnHost="/dev/null" \
--config-property heron.kubernetes.manager.volumes.hostPath.manager-host-path.path="host/path/path" \
--config-property heron.kubernetes.manager.volumes.hostPath.manager-host-path.subPath="host/path/sub/path" \
--config-property heron.kubernetes.manager.volumes.hostPath.manager-host-path.readOnly="true" \
\
--config-property heron.kubernetes.manager.volumes.nfs.manager-nfs.server="nfs-server.address" \
--config-property heron.kubernetes.manager.volumes.nfs.manager-nfs.readOnly="true" \
--config-property heron.kubernetes.manager.volumes.nfs.manager-nfs.pathOnNFS="/dev/null" \
--config-property heron.kubernetes.manager.volumes.nfs.manager-nfs.path="nfs/path" \
--config-property heron.kubernetes.manager.volumes.nfs.manager-nfs.subPath="nfs/sub/path" \
--config-property heron.kubernetes.manager.volumes.nfs.manager-nfs.readOnly="true"
```

### Required and Optional Configuration Items

The following table outlines CLI options which are either ***required*** ( &#x2705; ), ***optional*** ( &#x2754; ), or ***not available*** ( &#x274c; ) depending on the type of `Volume`.

| Option | emptyDir | hostPath | NFS
|---|---|---|---|
| `VOLUME NAME` | &#x2705; | &#x2705; | &#x2705;
| `path` | &#x2705; | &#x2705; | &#x2705;
| `subPath` | &#x2754; | &#x2754; | &#x2754;
| `readOnly` | &#x2754; | &#x2754; | &#x2754;
| `medium` | &#x2754; | &#x274c; | &#x274c;
| `sizeLimit` | &#x2754; | &#x274c; | &#x274c;
| `pathOnHost` | &#x274c; | &#x2705; | &#x274c;
| `type` | &#x274c; | &#x2754; | &#x274c;
| `pathOnNFS` | &#x274c; | &#x274c; | &#x2705;
| `server` | &#x274c; | &#x274c; | &#x2705;

<br>

***Note:*** The `VOLUME NAME` will be extracted from the CLI command.

<br>

### Configuration Items Created and Entries Made

The configuration items and entries in the tables below will made in their respective areas.

A `Volume` and a `Volume Mount` will be created for each `volume name` which you specify.

| Name | Description | Policy |
|---|---|---|
| `VOLUME NAME` | The `name` of the `Volume`. | Entries are made in the Pod Spec's `Volumes`, and the `Heron container`'s `volumeMounts`.
| `path` | The `mountPath` of the `Volume`. | Entries are made in the `Heron container`'s `volumeMounts`.
| `subPath` | The `subPath` of the `Volume`. | Entries are made in the `Heron container`'s `volumeMounts`.
| `readOnly` | A boolean value which defaults to `false` and indicates whether the medium has read-write permissions. | Entries are made in the `Heron container`s `volumeMount`. When used with an `NFS` volume an entry is also made in the associated `Volume`.
| `medium` | The type of storage medium that will back the `Empty Dir` and defaults to "", please read more [here](https://kubernetes.io/docs/concepts/storage/volumes#emptydir). | An entry is made in the `Empty Dir`'s `Volume`.
| `sizeLimit` | Total [amount](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#meaning-of-memory) of local storage required for this `Empty Dir` Volume. | An entry is made `Empty Dir`'s `Volume`.
| `pathOnHost` | The directory path to be mounted the host. | A `path` entry is made `Host Path`'s `Volume`.
| `type` | The type of the `Host Path` volume and defaults to "", please read more [here](https://kubernetes.io/docs/concepts/storage/volumes#hostpath). | An entry is made `Host Path`'s `Volume`.
| `pathOnNFS` | The directory path to be mounted the NFS server. | A `path` entry is made `NFS`'s `Volume`.
| `server` | The hostname or IP address of the NFS server. | An entry is made `NFS`'s `Volume`.

<br>

---

<br>

## Setting Limits and Requests via the Command Line Interface

> This section demonstrates how you can configure a topology's `Executor` and/or `Manager` (hereinafter referred to as `Heron containers`) resource `Requests` and `Limits` through CLI commands.

<br/>

You may configure an individual topology's `Heron container`'s resource `Requests` and `Limits` during submission through CLI commands. The default behaviour is to acquire values for resources from Configurations and for them to be common between the `Executor`s and the `Manager` for a topology.

<br>

### Usage

The command pattern is as follows:
`heron.kubernetes.[executor | manager].[limits | requests].[OPTION]=[VALUE]`

The currently supported CLI `options` and their associated `values` are:

* `cpu`: A natural number indicating the number of [CPU units](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#meaning-of-cpu).
* `memory`: A natural number indicating the amount of [memory units](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#meaning-of-memory).

<br>

#### Example

An example submission command is as follows.

***Limits and Requests:***

```bash
~/bin/heron submit kubernetes ~/.heron/examples/heron-api-examples.jar \
org.apache.heron.examples.api.AckingTopology acking \
--config-property heron.kubernetes.manager.limits.cpu=2 \
--config-property heron.kubernetes.manager.limits.memory=3 \
--config-property heron.kubernetes.manager.requests.cpu=1 \
--config-property heron.kubernetes.manager.requests.memory=2
```
