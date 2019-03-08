
This directory contains implementations of Heron bolts that can be used in user topologies.

The bolt implementations are maintained by Heron community.

## Bolt Development

- https://apache.github.io/incubator-heron/docs/developers/python/bolts/
- https://apache.github.io/incubator-heron/docs/developers/java/bolts/


## Requirements

### Directories and Files

Bolt implementation files should be organized in this directory structure:

`external/bolts/{bolt_name}/{language}`

Language: java, python, scala, etc
bolt name: reduce_bolt, filter_bolt, etc

Files in each bolt should be organized into these subdirectories:

- `src/main/`: source code
- `src/test/`: unit tests
- `doc/` : documentations


### Documentation

Each bolt should have a design doc as well as related information in the doc/ directory.

### License

All source code files should include a short Apache license header at the top.
