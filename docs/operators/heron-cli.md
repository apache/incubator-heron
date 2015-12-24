# Managing Topologies

Heron has a CLI tool called `heron-cli` that you can use to manage every aspect
of the [lifecycle of
topologies](../concepts/topologies.html#topology-lifecycle).

## Obtaining the `heron-cli` Executable

In order to use `heron-cli`, you need to generate a full [Heron
release](release.html) and distribute the `heron-cli` binary to all machines
that will be used to manage topologies.

## Submitting a Topology

In order to run a topology in a Heron cluster, you need to submit it using the
`submit` command. Topologies can be submitted in either an activated or
deactivated state (more on [activation](#activating-a-topology) and
[deactivation](#deactivating-a-topology) below).

Here's the basic syntax:

```bash
$ heron-cli submit <scheduler-overrides> <filepath> <classname> [topology args]
```

Arguments of the `submit` command:

* `scheduler-overrides` &mdash; Default parameters that you'd like to override.
  The syntax is `"param1:value1 param2:value2 param3:value3"`.

  **Example**: `"heron.local.working.directory:/path/to/dir
  topology.debug:true"`

* `filepath` &mdash; The path of the file in which you've packaged the
  topology's code. For Java topologies this will be a `.jar` file; for
  topologies in other languages (not yet supported), use a `.tar` file.

  **Example**: `/path/to/topology/my-topology.jar`

* `classname` &mdash; The class name of the class containing the `main` function for
  the topology.

  **Example**: `com.example.topologies.MyTopology`

* `topology args` (**optional**) &mdash; Arguments specific to the topology.
  You will need to supply args only if the `main` function for your topology
  requires them.

### Example Topology Submission Command

Below is an example command that would run a topology with a main class named
`com.example.topologies.MyTopology` packaged in `my-topology.jar`, along with
some scheduler overrides:

```bash
$ heron-cli submit "heron.local.working.directory:/path/to/dir topology.debug:true" \
    /path/to/topology/my-topology.jar \
    com.example.topologies.MyTopology \
    my-topology # Assuming that this topology requires a name argument
```

### Other Topology Submission Options

Flag | Meaning
:--- | :------
`--submitter-config-loader` | The class name of the non-default config loader (assuming that the loader is on the classpath).
`--scheduler-config` | Scheduler config file, if a non-default configuration is being used.
`--deactivated` | If set, the topology is deployed in a deactivated state.

## Activating a Topology

Once a topology has been successfully submitted to your cluster, you can
activate it using the `activate` command. Here's the basic syntax:

```bash
$ heron-cli activate <activator-overrides> <topology>
```

Arguments of the `activate` command:

* `activator-overrides` &mdash; Default activator parameters that you'd like to
  override. The syntax is `"param1:value1 param2:value2 param3:value3"`.

  **Example**: `heron.local.working.directory:/path/to/dir topology.debug:true`

* `topology` &mdash; The name of the already-submitted topology that you'd like to
  activate.

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

* `deactivator-overrides` &mdash; Deactivation parameters that you'd like to
  override. The syntax is `"param1:value1 param2:value2 param3:value3"`.

  **Example**: `heron.local.working.directory:/path/to/dir topology.debug:true`

* `topology` &mdash; The name of the topology that you'd like to deactivate.

## Restarting a Topology

You can restart a deactivated topology using the `restart` command (assuming
that the topology has not yet been killed, i.e. removed from the cluster).

```bash
$ heron-cli restart <restarter-overrides> <topology> [shard]
```

Arguments of the `restart` command:

* `restarter-overrides` &mdash; Restart parameters that you'd like to override. The
  syntax is `"param1:value1 param2:value2 param3:value3"`.

  **Example**: `heron.local.working.directory:/path/to/dir topology.debug:true`

* `topology` -- The name of the topology that you'd like to restart.
* `shard` (**optional**) &mdash; This enables you to specify the shard ID to be
  restarted if you want to restart only a specific shard of the topology.

### Example Topology Restart Command

```bash
$ heron-cli restart "topology.debug:true" \
    /path/to/topology/my-topology.jar \
    com.example.topologies.MyTopology \
    my-topology # Assuming that this topology requires a name argument
```

## Killing a Topology

If you've submitted a topology to your Heron cluster and would like to remove
knowledge of the topology entirely, you can remove it using the `kill` command.
Here's the basic syntax:

```bash
$ heron-cli kill <killer-overrides> <topology>
```

Arguments of the `kill` command:

* `killer-overrides` &dash; Default scheduler parameters that you'd like to
  override. The syntax is `"param1:value1 param2:value2 param3:value3"`.
* `topology` &mdash; The name of the topology that you'd like to kill.

### Example Topology Kill Command

```bash
$ heron-cli kill "topology.debug:true" \
    /path/to/topology/my-topology.jar \
    com.example.topologies.MyTopology \
    my-topology # Assuming that this topology requires a name argument
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
