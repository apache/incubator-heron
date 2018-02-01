# Heron Sandbox on Docker

### Requirements:
* [Docker](https://docs.docker.com/install/)

### Start Docker
It is recommended to that docker gets 4 or more cores and 2 GB or more memory 
```shell
$ minikube start --memory=7168 --cpus=5 --disk-size=20g
```

### Download sandbox script

```shell
$ curl -O https://raw.githubusercontent.com/twitter/heron/master/deploy/docker/sandbox.sh
```

### Getting help about heron sandbox:

```shell
$ ./sandbox.sh help
```

### Starting heron sandbox

```shell
$ ./sandbox.sh start
```

### Viewing heron ui
```
http://localhost:8889
```

### Starting heron shell and play around with example topologies:
```shell
$ ./sandbox.sh shell
```

### Shutting down heron sandbox
```shell
$ ./sandbox.sh stop
```
