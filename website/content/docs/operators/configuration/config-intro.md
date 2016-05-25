---
title: Intro to Heron Configuration
---

Heron can be configured at two levels:

1. **The system level** --- System-level configurations apply to the whole
Heron cluster rather than to any specific component.
2. **The component level** --- Component configurations apply only to a
specific component. Currently it is static at any stage of the topology's
[lifecycle](../../../concepts/topologies#topology-lifecycle), once the topology
is deployed.

All system-level configs and component-level defaults are declared in a
[YAML](http://www.yaml.org/) config file in `heron/config/src/yaml/conf/{cluster}/heron_internals.yaml`
in the Heron codebase. You can leave that file as is when [compiling
Heron](../../../developers/compiling) or modify the values to suit your use
case.

## The System Level

There are a small handful of system-level configs for Heron. These are detailed
in [System-level Configuration](../system).

## The Component Level

There is a wide variety of component-level configurations that you can establish
as defaults in your Heron cluster. These configurations tend to apply to
specific components in a topology and are detailed in the docs below:

* [Heron Instance](../instance)
* [Heron Metrics Manager](../metrics-manager)
* [Heron Topology Master](../tmaster)
* [Heron Stream Manager](../stmgr)

### Overriding Heron Configuration

Heron configuration is considered as internal for 
a cluster, and it is often discouraged to modify the configuration to suit one topology.
Currently, there are no interfaces supported to override heron configuration
for a topology via heron client or other heron tools.

More on Heron's CLI tool can be found in [Managing Heron
Topologies](../../../../heron-cli).
