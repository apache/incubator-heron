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
[2018-02-01 20:24:47 +0000] [INFO] org.apache.heron.spi.utils.TManagerUtils: Topology command DEACTIVATE completed successfully.
[2018-02-01 20:24:47 +0000] [INFO]: Successfully deactivate topology: exclamation
root@16092325a696:/heron# heron activate sandbox exclamation
[2018-02-01 20:24:55 +0000] [INFO]: Using cluster definition in /heron/heron-tools/conf/sandbox
[2018-02-01 20:24:56 +0000] [INFO] org.apache.heron.spi.utils.TManagerUtils: Topology command ACTIVATE completed successfully.
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
