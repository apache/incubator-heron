# The Heron Tracker

The **Heron Tracker** is a long-running service that gathers a wide variety of
information about Heron topologies and then exposes that information through a
JSON REST API.

The Tracker can run within your Heron cluster (e.g. on
[Mesos](../operators/deployment/mesos.html) or
[Aurora](../operators/deployment/aurora.html)) or outside of it, provided that
the machine on which it runs has access to your Heron cluster.

## REST API

See [The Heron Tracker REST API](heron-tracker-rest-api.html) for exhaustive
API documentation.

## Starting the Tracker

You can start the Heron Tracker by running the `heron-tracker` executable, which
is generated as part of a [Heron release](release.html).

```bash
$ cd /path/to/heron/binaries
$ ./heron-tracker
```

By default, the Tracker runs on port 8888. You can specify a different port
using the `--port` flag:

```bash
$ ./heron-tracker --port=1234
```
