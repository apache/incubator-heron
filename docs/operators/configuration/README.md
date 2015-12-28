# How Heron Configuration Works

Heron is configured on a per-topology basis. You can apply a whole configuration
at any stage of a topology's lifecycle using a [YAML](http://yaml.org) file.
This means that you can apply one configuration when you
[submit](../heron-cli.html#submitting-a-topology) a topology, a different one
when you [activate](../heron-cli.html#activating-a-topology) it, etc.

You can specify a location for your YAML configuration file using the 

### Default vs. User-defined Configuration

You can find lists of configurable parameters on a topology-wide or
per-component basis in the following docs:

* [Topology-level Configuration](topology-config.html)
* [Heron Instance](instance.html)
* [Heron Metrics Manager](metrics-manager.html)
* [Heron Topology Master](topology-master.html)
* [Heron Stream Manager](stream-manager.html)

You can override any
