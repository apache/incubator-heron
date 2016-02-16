package com.twitter.heron.packing;

import java.util.HashMap;

import com.twitter.heron.spi.common.PackingPlan;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.packing.IPacking;

public class NullPacking implements IPacking {

  public void initialize(Context context) {
  }

  public PackingPlan pack() {
    return new PackingPlan(
      "",
      new HashMap<>(),
      new PackingPlan.Resource(0.0, 0L, 0L));
  }

  @Override
  public void close() {
  }
}
