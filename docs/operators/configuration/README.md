# How Heron Configuration Works

Heron is configured on a per-topology basis. You can apply a whole configuration
at any stage of a topology's lifecycle using a [YAML](http://yaml.org) file.
This means that you can apply one configuration when you
[submit](../heron-cli.html#submitting-a-topology) a topology, a different one
when you [activate](../heron-cli.html#activating-a-topology) it, etc.

You can specify a location for your YAML configuration file using the 


### Default vs. User-defined Configuration

You can find lists of configurable parameters on a per-component basis in the
following docs:

* [Heron Instance](instance.html)
* [Heron Metrics Manager](metrics-manager.html)
* [Heron Topology Master](topology-master.html)
* [Heron Stream Manager](stream-manager.html)

You can override any

### System-level Configuration

Parameter | Meaning | Default
:-------- |:------- |:-------
`heron.java.home.path` | The `JAVA_HOME` path in the host's environment | **Must be specified**

### Logging Configuration

Parameter | Meaning | Default
:-------- |:------- |:-------
`heron.logging.directory` | The relative path to the logging directory with respect to the running sandbox directory | `log-files`
`heron.logging.maximum.size.mb` | The maximum log file size (in megabytes) | `100`
`heron.logging.maximum.files` | The maximum number of log files | `5`
`heron.logging.prune.interval.sec` | The time interval, in seconds, at which Heron prunes log files (if log files exceed the maximum size) | `300`
`heron.logging.flush.interval.sec` | The time interval, in seconds, at which Heron flushes log files from memory | `10`
`heron.logging.err.threshold` | The threshold level to log error | `3`

Sandbox directory: depends on Aurora/Mesos/YARN; each creates a sandbox directory
SM, for example, write to same log folder; value to link them together
