package com.twitter.heron.spi.packing;

import java.util.HashMap;

import com.twitter.heron.spi.common.PackingPlan;
import com.twitter.heron.spi.scheduler.context.LaunchContext;
import com.twitter.heron.spi.packing.IPackingAlgorithm;

public class NullPackingAlgorithm implements IPackingAlgorithm {
  public PackingPlan pack(LaunchContext context) {
    return new PackingPlan(
      "",
      new HashMap<String, PackingPlan.ContainerPlan>(),
      new PackingPlan.Resource(0.0, 0L, 0L));
  }
}
