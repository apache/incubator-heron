---
title: Local mode
---

The page explains how to run a topology in local mode.

Local mode is designed for topology developers to better debug or optimize their topologies.

Local mode simulates a heron cluster in a single JVM process and is useful for developing and testing topologies. Running topologies in local mode is similar to running topologies on a cluster.
With the whole topology running in a single process, you could enjoy all free benefits it brings. For example, one can run program in IDE and set breakpoints to examine the states of a topology, or profile your program to optimize topology.

To use local mode, simply use the ``LocalMode`` class, which is in ``storm-compatibility-unshaded_deploy.jar``  (currently under ``bazel-bin/heron/storm/src/java``).

For example:


```java
import com.twitter.heron.localmode.LocalMode;
LocalMode localMode = new LocalMode();
```

You can then submit topologies using the ``submitTopology`` method on the ``LocalMode`` object. Just like the corresponding method on ``StormSubmitter``, ``submitTopology`` takes a name, a topology configuration, and a topology object.

For example:

```java
localMode.submitTopology("test", conf, builder.createTopology());
```

Other interfaces for local mode are:

```java
localMode.killTopology("test");
localMode.activate("test");
localMode.deactivate("test");
localMode.shutdown();
```

To kill a topology, you could also simply terminate this process.

An interesting point is that, the local mode would run in separate threads other than main thread. All those interfaces are thread-safe. This means that you could invoke these interfaces in other threads and monitor the corresponding behaviors interactively.

You may also want to use visual panels to communicate related information, tips or things users need to be aware of.
