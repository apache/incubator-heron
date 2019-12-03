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
# Heron Kubernetes

### Requirements:
* Kubernetes cluster with 3 or more nodes
* [kubectl](https://kubernetes.io/docs/tasks/kubectl/install/)
* [heron client](https://apache.github.io/incubator-heron/docs/getting-started/)



### Start Components:

1. Start zookeeper:

```shell
$ kubectl create -f https://raw.githubusercontent.com/apache/incubator-heron/master/deploy/kubernetes/general/zookeeper.yaml

$ kubectl get pods
NAME                                  READY     STATUS    RESTARTS   AGE
zk-0                                  1/1       Running   0          1m

# wait until zk-0 STATUS is Running before proceeding to the next steps

```

2. Start bookkeeper:
```shell
$ kubectl create -f https://raw.githubusercontent.com/apache/incubator-heron/master/deploy/kubernetes/general/bookkeeper.yaml
```

This deploys bookkeeper in a `DaemonSet` and requires the ability of exposing `hostPort` for pods communication.
In some environments like K8S on DC/OS, `hostPort` is not well supported. You can consider deploying bookkeeper in
a `StatefulSet` with `Persistent Volumes` as below. Please see [Persistent Volumes](https://kubernetes.io/docs/concepts/storage/persistent-volumes/) for more details.

```shell
$ kubectl create -f https://raw.githubusercontent.com/apache/incubator-heron/master/deploy/kubernetes/general/bookkeeper.statefulset.yaml
```

3. Start heron tools:
```shell
$ kubectl create -f https://raw.githubusercontent.com/apache/incubator-heron/master/deploy/kubernetes/general/tools.yaml
```

4. Start heron API server:
```shell
$ kubectl create -f https://raw.githubusercontent.com/apache/incubator-heron/master/deploy/kubernetes/general/apiserver.yaml
```

### Deploy via heron API server
We will start a proxy to the cluster and then construct a [proxy url](https://kubernetes.io/docs/tasks/administer-cluster/access-cluster-services/#manually-constructing-apiserver-proxy-urls) to access the API server.


1. Start kubectl proxy:
```shell
$ kubectl proxy -p 8001
```

2. Verify we can access the API server:

```shell
$ curl http://localhost:8001/api/v1/namespaces/default/services/heron-apiserver:9000/proxy/api/v1/version
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

3. Set service_url:
```shell
$ heron config kubernetes \
set service_url http://localhost:8001/api/v1/namespaces/default/services/heron-apiserver:9000/proxy \
org.apache.heron.examples.api.AckingTopology acking
```

4. Submit an example topology:
```shell
$ heron submit kubernetes ~/.heron/examples/heron-api-examples.jar \
org.apache.heron.examples.api.AckingTopology acking
```

5. View heron ui:
```
http://localhost:8001/api/v1/namespaces/default/services/heron-ui:8889/proxy
```
