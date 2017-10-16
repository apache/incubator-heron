# Heron Kubernetes

### Requirements:
* Kubernetes cluster with 3 or more nodes
* [kubectl](https://kubernetes.io/docs/tasks/kubectl/install/)
* [heron client](https://twitter.github.io/heron/docs/getting-started/)



### Start Components:

1. Start zookeeper:

```shell
$ kubectl create -f https://raw.githubusercontent.com/twitter/heron/master/deploy/kubernetes/general/zookeeper.yaml

$ kubectl get pods
NAME                                  READY     STATUS    RESTARTS   AGE
zk-0                                  1/1       Running   0          1m

# wait until zk-0 STATUS is Running before proceeding to the next steps

```

2. Start bookkeeper:
```shell
$ kubectl create -f https://raw.githubusercontent.com/twitter/heron/master/deploy/kubernetes/general/bookkeeper.yaml
```

3. Start heron tools:
```shell
$ kubectl create -f https://raw.githubusercontent.com/twitter/heron/master/deploy/kubernetes/general/tools.yaml
```

4. Start heron apiserver:
```shell
$ kubectl create -f https://raw.githubusercontent.com/twitter/heron/master/deploy/kubernetes/general/apiserver.yaml
```

### Deploy via heron apiserver
We will start a proxy to the cluster and then construct a [proxy url](https://kubernetes.io/docs/tasks/administer-cluster/access-cluster-services/#manually-constructing-apiserver-proxy-urls) to access the apiserver.


1. Start kubectl proxy:
```shell
$ kubectl proxy -p 8001
```

2. Verify we can access the apiserver:

```shell
$ curl http://localhost:8001/api/v1/proxy/namespaces/default/services/heron-apiserver:9000/api/v1/version
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

3. Submit an example topology:
```shell
$ heron submit kubernetes \
--service-url=http://localhost:8001/api/v1/proxy/namespaces/default/services/heron-apiserver:9000 \
~/.heron/examples/heron-api-examples.jar \
com.twitter.heron.examples.api.AckingTopology acking
```

4. View heron ui:
```
http://localhost:8001/api/v1/proxy/namespaces/default/services/heron-ui:8889
```
