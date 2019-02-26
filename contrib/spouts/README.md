
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
