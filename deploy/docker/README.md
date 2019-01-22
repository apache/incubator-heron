# Heron Sandbox on Docker

### Requirements:
* [Docker](https://docs.docker.com/install/)

It is recommended that docker gets 4 or more cores and 2 GB or more memory 

### Download heron sandbox script

```shell
$ curl -O https://raw.githubusercontent.com/apache/incubator-heron/master/deploy/docker/sandbox.sh
$ chmod +x sandbox.sh
```

### Getting help about heron sandbox

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
Starting heron sandbox shell 
root@16092325a696:/heron# heron submit sandbox /heron/examples/heron-api-examples.jar org.apache.heron.examples.api.ExclamationTopology exclamation
[2018-02-01 20:24:20 +0000] [INFO]: Using cluster definition in /heron/heron-tools/conf/sandbox
[2018-02-01 20:24:20 +0000] [INFO]: Launching topology: 'exclamation'
[2018-02-01 20:24:21 +0000] [INFO]: Successfully launched topology 'exclamation' 
root@16092325a696:/heron# heron deactivate sandbox exclamation
[2018-02-01 20:24:46 +0000] [INFO]: Using cluster definition in /heron/heron-tools/conf/sandbox
[2018-02-01 20:24:47 +0000] [INFO] org.apache.heron.spi.utils.TMasterUtils: Topology command DEACTIVATE completed successfully.
[2018-02-01 20:24:47 +0000] [INFO]: Successfully deactivate topology: exclamation
root@16092325a696:/heron# heron activate sandbox exclamation
[2018-02-01 20:24:55 +0000] [INFO]: Using cluster definition in /heron/heron-tools/conf/sandbox
[2018-02-01 20:24:56 +0000] [INFO] org.apache.heron.spi.utils.TMasterUtils: Topology command ACTIVATE completed successfully.
[2018-02-01 20:24:56 +0000] [INFO]: Successfully activate topology: exclamation
root@16092325a696:/heron# heron kill sandbox exclamation
[2018-02-01 20:25:08 +0000] [INFO]: Using cluster definition in /heron/heron-tools/conf/sandbox
[2018-02-01 20:25:09 +0000] [INFO]: Successfully kill topology: exclamation
root@16092325a696:/heron# exit
exit
Terminating heron sandbox shell
```

### Shutting down heron sandbox
```shell
$ ./sandbox.sh stop
```
