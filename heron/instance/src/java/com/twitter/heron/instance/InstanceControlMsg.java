// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.twitter.heron.instance;

import com.twitter.heron.common.utils.misc.PhysicalPlanHelper;
import com.twitter.heron.proto.ckptmgr.CheckpointManager;

public final class InstanceControlMsg {
  private PhysicalPlanHelper newPhysicalPlanHelper;
  private CheckpointManager.RestoreInstanceStateRequest restoreInstanceStateRequest;
  private CheckpointManager.StartInstanceStatefulProcessing startInstanceStatefulProcessing;

  private InstanceControlMsg(Builder builder) {
    this.newPhysicalPlanHelper = builder.newPhysicalPlanHelper;
    this.restoreInstanceStateRequest = builder.restoreInstanceStateRequest;
    this.startInstanceStatefulProcessing = builder.startInstanceStatefulProcessing;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public PhysicalPlanHelper getNewPhysicalPlanHelper() {
    return newPhysicalPlanHelper;
  }

  public boolean isNewPhysicalPlanHelper() {
    return newPhysicalPlanHelper != null;
  }

  public CheckpointManager.RestoreInstanceStateRequest getRestoreInstanceStateRequest() {
    return this.restoreInstanceStateRequest;
  }

  public boolean isRestoreInstanceStateRequest() {
    return this.restoreInstanceStateRequest != null;
  }

  public CheckpointManager.StartInstanceStatefulProcessing getStartInstanceStatefulProcessing() {
    return this.startInstanceStatefulProcessing;
  }

  public boolean isStartInstanceStatefulProcessing() {
    return this.startInstanceStatefulProcessing != null;
  }

  public static final class Builder {
    private PhysicalPlanHelper newPhysicalPlanHelper;
    private CheckpointManager.RestoreInstanceStateRequest restoreInstanceStateRequest;
    private CheckpointManager.StartInstanceStatefulProcessing startInstanceStatefulProcessing;

    private Builder() {

    }

    public Builder setNewPhysicalPlanHelper(PhysicalPlanHelper physicalPlanHelper) {
      this.newPhysicalPlanHelper = physicalPlanHelper;
      return this;
    }

    public Builder setRestoreInstanceStateRequest(
        CheckpointManager.RestoreInstanceStateRequest request) {
      this.restoreInstanceStateRequest = request;
      return this;
    }

    public Builder setStartInstanceStatefulProcessing(
        CheckpointManager.StartInstanceStatefulProcessing request) {
      this.startInstanceStatefulProcessing = request;
      return this;
    }

    public InstanceControlMsg build() {
      return new InstanceControlMsg(this);
    }
  }
}
