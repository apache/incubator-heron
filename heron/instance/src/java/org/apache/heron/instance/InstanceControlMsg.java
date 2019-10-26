/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.heron.instance;

import org.apache.heron.common.utils.misc.PhysicalPlanHelper;
import org.apache.heron.proto.ckptmgr.CheckpointManager;

public final class InstanceControlMsg {
  private PhysicalPlanHelper newPhysicalPlanHelper;
  private CheckpointManager.RestoreInstanceStateRequest restoreInstanceStateRequest;
  private CheckpointManager.StartInstanceStatefulProcessing startInstanceStatefulProcessing;
  private CheckpointManager.StatefulConsistentCheckpointSaved statefulConsistentCheckpointSaved;

  private InstanceControlMsg(Builder builder) {
    this.newPhysicalPlanHelper = builder.newPhysicalPlanHelper;
    this.restoreInstanceStateRequest = builder.restoreInstanceStateRequest;
    this.startInstanceStatefulProcessing = builder.startInstanceStatefulProcessing;
    this.statefulConsistentCheckpointSaved = builder.statefulConsistentCheckpointSaved;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public PhysicalPlanHelper getNewPhysicalPlanHelper() {
    return newPhysicalPlanHelper;
  }

  public CheckpointManager.StatefulConsistentCheckpointSaved getStatefulCheckpointSavedMessage() {
    return this.statefulConsistentCheckpointSaved;
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
    private CheckpointManager.StatefulConsistentCheckpointSaved statefulConsistentCheckpointSaved;

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

    public Builder setStatefulCheckpointSaved(
        CheckpointManager.StatefulConsistentCheckpointSaved message) {
      this.statefulConsistentCheckpointSaved = message;
      return this;
    }

    public InstanceControlMsg build() {
      return new InstanceControlMsg(this);
    }
  }
}
