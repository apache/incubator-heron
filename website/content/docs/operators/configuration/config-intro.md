---
title: Intro to Heron Configuration
---

Heron can be configured at two levels:

1. **The system level** &mdash; System-level configurations apply to the whole
Heron cluster rather than to any specific topology.
2. **The topology level** &mdash; Topology configurations apply only to a
specific topology and can be modified at any stage of the topology's
[lifecycle](../../../concepts/topologies#topology-lifecycle).

All system-level configs and topology-level defaults are declared in a
[YAML](http://www.yaml.org/) config file in `heron/config/heron_internals.yaml`
in the Heron codebase. You can leave that file as is when [compiling
Heron](../../../developers/compiling) or modify the values to suit your use
case.

## The System Level

There are a small handful of system-level configs for Heron. These are detailed
in [System-level Configuration](../system).

## The Topology Level

There is a wide variety of topology-level configurations that you can establish
as defaults in your Heron cluster. These configurations tend to apply to
specific components in a topology and are detailed in the docs below:

* [Heron Instance](../instance)
* [Heron Metrics Manager](../metrics-manager)
* [Heron Topology Master](../tmaster)
* [Heron Stream Manager](../stmgr)

### Overriding Topology-level Defaults

The parameters set in `heron/config/heron_internals.yaml` are defaults that
will be automatically applied to all topologies in your cluster. You can
override these values on a per-topology basis using **scheduler overrides**.
These overrides are the second argument in all topology management commands and
have the following syntax:

    param1:value1 param2:value2 param3:value3 ...

Here's an example:

```bash
$ heron-cli submit "topology.debug:false heron.local.working.directory:/path/to/dir" \
    /path/to/topology/my-topology.jar \
    biz.acme.topologies.MyTopology \
    my-topology
```

More on Heron's CLI tool can be found in [Managing Heron
Topologies](../../../../heron-cli).
