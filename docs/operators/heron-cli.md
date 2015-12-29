# Managing Topologies

Heron has a CLI tool called `heron-cli` that you can use to manage every aspect
of the [lifecycle of
topologies](../concepts/topologies.html#topology-lifecycle).

## Obtaining the `heron-cli` Executable

In order to use `heron-cli`, you need to generate a full [Heron
release](release.html) and distribute the resulting `heron-cli` binary to all
machines that will be used to manage topologies.

## CLI Flags

There are two flags that are available for all topology management commands
(`submit`, `activate`, `deactivate`, `restart`, and `kill`):

* `--config-loader` &mdash; Every Heron scheduler must implement a
  default configuration loader that's responsible for providing the topology's
  configuration from a file or other source. This flag enables you to specify a
  non-default loader.
* `--config-file` &mdash; If you specify a configuration loader using the
  `--config-loader` flag and that loader draws its configuration from a file
  (rather than another source), you can use the `--config-file` flag to provide
  a path for that file.

These flags are especially useful if you're developing a [custom
scheduler](../contributors/custom-scheduler.html).

Here's an example topology management command that uses both of these flags:

```bash
$ heron-cli activate "topology.debug:true" \
    /path/to/topology/my-topology.jar \
    biz.acme.topologies.MyTopology \
    my-topology /
    --config-loader=biz.acme.config.MyConfigLoader \
    --config-file=/path/to/config/scheduler.conf
```

## Submitting a Topology

In order to run a topology in a Heron cluster, you need to submit it using the
`submit` command. Topologies can be submitted in either an activated or
deactivated state (more on [activation](#activating-a-topology) and
[deactivation](#deactivating-a-topology) below).

Here's the basic syntax:

```bash
$ heron-cli submit <scheduler overrides> <filepath> <class name> [topology args]
```

Arguments of the `submit` command:

* Scheduler overrides &mdash; Default parameters that you'd like to override.
  The syntax is `"param1:value1 param2:value2 param3:value3"`.

  **Example**: `"heron.local.working.directory:/path/to/dir
  topology.debug:true"`

* Filepath &mdash; The path of the file in which you've packaged the
  topology's code. For Java topologies this will be a `.jar` file; for
  topologies in other languages (not yet supported), use a `.tar` file.

  **Example**: `/path/to/topology/my-topology.jar`

* Class name &mdash; The name of the class containing the `main` function
  for the topology.

  **Example**: `com.example.topologies.MyTopology`

* Topology args (**optional**) &mdash; Arguments specific to the topology.
  You will need to supply additional args only if the `main` function for your
  topology requires them.

### Example Topology Submission Command

Below is an example command that would run a topology with a main class named
`com.example.topologies.MyTopology` packaged in `my-topology.jar`, along with
some scheduler overrides:

```bash
$ heron-cli submit "heron.local.working.directory:/path/to/dir topology.debug:true" \
    /path/to/topology/my-topology.jar \
    com.example.topologies.MyTopology \
    my-topology
```

### Other Topology Submission Options

Flag | Meaning
:--- | :------
`--deactivated` | If set, the topology is deployed in a deactivated state.

## Activating a Topology

Once a topology has been successfully submitted to your cluster, you can
activate it using the `activate` command. Here's the basic syntax:

```bash
$ heron-cli activate <activator-overrides> <topology>
```

Arguments of the `activate` command:

* Activator overrides &mdash; Default activator parameters that you'd like to
  override. The syntax is `"param1:value1 param2:value2 param3:value3"`.

  **Example**: `heron.local.working.directory:/path/to/dir topology.debug:true`

* Topology name  &mdash; The name of the already-submitted topology that you'd
  like to activate.

### Example Topology Activation Command

```bash
$ heron-cli activate "heron.local.working.directory:/path/to/dir topology.debug:true"  \
    my-topology
```

## Deactivating a Topology

You can deactivate a running topology at any time using the `deactivate`
command. Here's the basic syntax:

```bash
$ heron-cli deactivate <deactivator-overrides> <topology>
```

Arguments of the `deactivate` command:

* Deactivator overrides &mdash; Deactivation parameters that you'd like to
  override. The syntax is `"param1:value1 param2:value2 param3:value3"`.

  **Example**: `heron.local.working.directory:/path/to/dir topology.debug:true`

* Topology name &mdash; The name of the topology that you'd like to deactivate.

## Restarting a Topology

You can restart a deactivated topology using the `restart` command (assuming
that the topology has not yet been killed, i.e. removed from the cluster).

```bash
$ heron-cli restart <restarter-overrides> <topology> [shard]
```

Arguments of the `restart` command:

* Restarter overrides &mdash; Restart parameters that you'd like to override.
  The syntax is `"param1:value1 param2:value2 param3:value3"`.

  **Example**: `heron.local.working.directory:/path/to/dir topology.debug:true`

* Topology name &mdash; The name of the topology that you'd like to restart.
* Shard ID (**optional**) &mdash; This enables you to specify the shard ID to be
  restarted if you want to restart only a specific shard of the topology.

### Example Topology Restart Command

```bash
$ heron-cli restart "topology.debug:true" \
    my-topology
```

## Killing a Topology

If you've submitted a topology to your Heron cluster and would like to remove
knowledge of the topology entirely, you can remove it using the `kill` command.
Here's the basic syntax:

```bash
$ heron-cli kill <killer-overrides> <topology>
```

Arguments of the `kill` command:

* Killer overrides &dash; Default scheduler parameters that you'd like to
  override. The syntax is `"param1:value1 param2:value2 param3:value3"`.
* Topology name &mdash; The name of the topology that you'd like to kill.

### Example Topology Kill Command

```bash
$ heron-cli kill "topology.debug:true" \
    my-topology
```

## Other Commands

### Version

Run the `version` command at any time to see which version of `heron-cli` you're
using:

```bash
$ heron-cli version
```

### Classpath

At any time you can display the classpath used by the Heron CLI client when
running commands.

```bash
$ heron-cli classpath
```
