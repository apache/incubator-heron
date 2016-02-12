package com.twitter.heron.packing.nulled;

import java.util.HashMap;

import com.twitter.heron.spi.common.PackingPlan;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.packing.IPackingAlgorithm;

public class NullPackingAlgorithm implements IPackingAlgorithm {

  public void initialize(Context context) {
  }

  public PackingPlan pack() {
    return new PackingPlan(
      "",
      new HashMap<String, PackingPlan.ContainerPlan>(),
      new PackingPlan.Resource(0.0, 0L, 0L));
  }

  public void cleanup() {
  }
}
