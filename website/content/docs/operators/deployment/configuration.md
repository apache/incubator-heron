# Configuring a cluster

In order to setup a Heron cluster, you need to configure a few files. Each file configures 
a component of the Heron streaming framework.

* `scheduler.yaml` &mdash; This file specifies the classes you need for launcher, scheduler, and
for managing the topology at runtime. Any other specific parameters for the scheduler goes into
this file.

* `statemgr.yaml` &mdash; It contains the classes and the configuration for state manager.
The state manager maintains the running state of the topology as logical plan, physical plan,
scheduler state, and execution state.

* `uploader.yaml` &mdash; This file specifies the classes, and its configuration, for
uploading the topology jars to storage. Once the containers are scheduled, they will download
these jars from the storage for running. 

* `heron_internals.yaml` &mdash; This file contains a bunch of parameters that serves as
knobs for how heron behaves. This requires advanced knowledge of heron architecture and its
components. For starters, the best option is just to copy the file provided with sample
configuration. Once you are familiar with the system you can tune these parameters to achieve
high throughput or low latency topologies.

* `metrics_sinks.yaml` &mdash; This file specifies where the metrics needs to be routed. 
By default, the `file sink` and `tmaster sink` need to be present. In addition, we supply 
`scribe sink` and `graphite sink`.

* `packing.yaml` &mdash; This file specifies the classes for `packing algorithm`, we will default
to Round Robin, if this is not specified.

* `client.yaml` &mdash; This file controls the behavior of `heron` cli client. This is optional.
