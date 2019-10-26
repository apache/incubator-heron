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

This directory contains implementations of Heron spouts that can be used in user topologies.

The spout implementations are maintained by Heron community.

## Spout Development

- https://apache.github.io/incubator-heron/docs/developers/python/spouts/
- https://apache.github.io/incubator-heron/docs/developers/java/spouts/


## Requirements

### Directories and Files

Spout implementation files should be organized in this directory structure:

`external/spouts/{client_name}/{language}/{spout_name}`

Client name: kafka, pulsar, etc
Language: java, python, scala, etc
Spout name: kafka_spout, stateful_kafka_spout, etc

Files in each spout should be organized into these subdirectories:

- `src/main/`: source code
- `src/test/`: unit tests
- `doc/` : documentations


### Documentation

Each spout should have a design doc as well as related information in the doc/ directory.

### License

All source code files should include a short Apache license header at the top.
