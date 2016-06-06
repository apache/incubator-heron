---
title: Simulator Mode
---

Simulator mode is specifically designed for topology developers to better debug or optimize their topologies.

Simulator mode simulates a heron cluster in a single JVM process. This is useful for developing and testing topologies.
Running topologies under simulator mode is similar to running topologies on a cluster.

With a whole topology running in a single process, you could enjoy all free benefits it brings.
For example, one can run program in IDE and set breakpoints to examine the states of a topology, or profile your program to optimize it.

# Develop topology with simulator mode

To use simulator mode, simply use the ``SimulatorMode`` class, which is
in ``storm-compatibility-unshaded_deploy.jar``  (currently under ``bazel-bin/heron/storm/src/java``).

For example:

```java
import com.twitter.heron.simulator.Simulator;
Simulator simulator = new Simulator();
```

You can then submit topologies using the ``submitTopology`` method on the ``Simulator`` object. Just like the corresponding method on ``StormSubmitter``, ``submitTopology`` takes a name, a topology configuration, and a topology object.

For example:

```java
simulator.submitTopology("test", conf, builder.createTopology());
```

Other interfaces for simulator mode are:

```java
simulator.killTopology("test");
simulator.activate("test");
simulator.deactivate("test");
simulator.shutdown();
```

To kill a topology, you could also terminate the process.

The simulator mode will run in separate threads other than main thread. All those interfaces are thread-safe. This means that you could invoke these interfaces in other threads and monitor the corresponding behaviors interactively.

# Debug topology using IntelliJ

Bolts and Spouts run as separate threads in simulator. To add breakpoints inside a bolt/spout, the Suspend Policy of the breakpoint needs to be set as Thread. To change the Suspend Policy, Right click on the breakpoint as shown in the following image:

![Set Breakpoint](/img/intellij-set-breakpoint.jpg)

If output and logs are not convenient to be checked in intellij console, they can be saved to local file by choose `Run -> Edit Configurations....` and set as shown in the following image:

![Save Console](/img/intellij-save-console.jpg)

