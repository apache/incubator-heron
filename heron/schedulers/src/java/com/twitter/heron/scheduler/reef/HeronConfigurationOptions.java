package com.twitter.heron.scheduler.reef;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

public class HeronConfigurationOptions {
  @NamedParameter(doc = "Heron-REEF cluster configuration param", default_value = "reef")
  public class Cluster implements Name<String> {
  }

  @NamedParameter(doc = "Heron-REEF environment configuration parameter", default_value = "default")
  public class Environ implements Name<String> {
  }

  @NamedParameter(doc = "Heron-REEF Role configuration parameter", default_value = "heron")
  public class Role implements Name<String> {
  }

  @NamedParameter(doc = "Topology Jar name")
  public class TopologyJar implements Name<String> {
  }

  @NamedParameter(doc = "Name of topology package file")
  public class TopologyPackageName implements Name<String> {
  }

  @NamedParameter(doc = "Name of heron core package file")
  public class HeronCorePackageName implements Name<String> {
  }

  @NamedParameter(doc = "Topology packing plan representation")
  public class PackedPlan implements Name<String> {
  }

  @NamedParameter(doc = "Heron topology Name")
  public class TopologyName implements Name<String> {
  }

  @NamedParameter(doc = "Heron-REEF http port configuration", default_value = "0")
  public class HttpPort implements Name<Integer> {
  }

  @NamedParameter(doc = "Identifies the heron executor, either running TM or a worker", default_value = "0")
  public class HeronExecutorId implements Name<String> {
  }
}
