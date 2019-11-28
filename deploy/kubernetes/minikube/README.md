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
# Heron on Kubernetes via Minikube


### Requirements:
* [minikube](https://kubernetes.io/docs/getting-started-guides/minikube/#installation)
* [kubectl](https://kubernetes.io/docs/tasks/kubectl/install/)
* [heron client](https://apache.github.io/incubator-heron/docs/getting-started/)


### Start minkube
It is recommended to start minikube with at least 7 GB of memory ```--memory=7168```, 
5 cpus ```--cpus=5``` and 20 GB of storage ```--disk-size=20g```
```shell
$ minikube start --memory=7168 --cpus=5 --disk-size=20g
```

### Start Components:

1. Start zookeeper:

```shell
$ kubectl create -f https://raw.githubusercontent.com/apache/incubator-heron/master/deploy/kubernetes/minikube/zookeeper.yaml

$ kubectl get pods
NAME                                  READY     STATUS    RESTARTS   AGE
zk-0                                  1/1       Running   0          1m

# wait until zk-0 STATUS is Running before proceeding to the next steps

```

2. Start bookkeeper:
```shell
$ kubectl create -f https://raw.githubusercontent.com/apache/incubator-heron/master/deploy/kubernetes/minikube/bookkeeper.yaml
```

3. Start heron tools:
```shell
$ kubectl create -f https://raw.githubusercontent.com/apache/incubator-heron/master/deploy/kubernetes/minikube/tools.yaml
```

4. Start heron API server:
```shell
$ kubectl create -f https://raw.githubusercontent.com/apache/incubator-heron/master/deploy/kubernetes/minikube/apiserver.yaml
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
