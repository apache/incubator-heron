package com.twitter.heron.instance;

import com.twitter.heron.common.utils.misc.PhysicalPlanHelper;

public class InstanceControlMsg {
  public static class Builder {
    private PhysicalPlanHelper newPhysicalPlanHelper;

    private Builder() {

    }

    public Builder setNewPhysicalPlanHelper(PhysicalPlanHelper newPhysicalPlanHelper) {
      this.newPhysicalPlanHelper = newPhysicalPlanHelper;
      return this;
    }

    public InstanceControlMsg build() {
      return new InstanceControlMsg(this);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  private InstanceControlMsg(Builder builder) {
    this.newPhysicalPlanHelper = builder.newPhysicalPlanHelper;
  }

  private PhysicalPlanHelper newPhysicalPlanHelper;

  public PhysicalPlanHelper getNewPhysicalPlanHelper() {
    return newPhysicalPlanHelper;
  }

  public boolean isNewPhysicalPlanHelper() {
    return newPhysicalPlanHelper != null;
  }
}
