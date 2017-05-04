// Copyright 2017 Twitter. All rights reserved.
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

package com.twitter.heron.spi.statefulstorage;

import com.twitter.heron.proto.ckptmgr.CheckpointManager;

public class Checkpoint {
  private final String topologyName;
  private final String checkpointId;
  private final String componentName;
  private final String instanceId;
  private final String taskId;

  private CheckpointManager.SaveInstanceStateRequest saveBytes;
  private int nBytes;

  public Checkpoint(String topologyName, CheckpointManager.SaveInstanceStateRequest saveRequest) {
    this.topologyName = topologyName;
    this.checkpointId = saveRequest.getCheckpoint().getCheckpointId();
    this.componentName = saveRequest.getInstance().getInfo().getComponentName();
    this.instanceId = saveRequest.getInstance().getInstanceId();
    this.taskId = "" + saveRequest.getInstance().getInfo().getTaskId();
    this.saveBytes = saveRequest;
    this.nBytes = saveRequest.getSerializedSize();
  }

  public Checkpoint(String topologyName, CheckpointManager.GetInstanceStateRequest getRequest) {
    this.topologyName = topologyName;
    this.checkpointId = getRequest.getCheckpointId();
    this.componentName = getRequest.getInstance().getInfo().getComponentName();
    this.instanceId = getRequest.getInstance().getInstanceId();
    this.taskId = "" + getRequest.getInstance().getInfo().getTaskId();
    saveBytes = null;
    nBytes = 0;
  }

  public String getTopologyName() {
    return topologyName;
  }

  public String getCheckpointId() {
    return checkpointId;
  }

  public String getComponent() {
    return componentName;
  }

  public String getInstance() {
    return instanceId;
  }

  public String getTaskId() {
    return taskId;
  }

  public CheckpointManager.SaveInstanceStateRequest checkpoint() {
    return saveBytes;
  }

  public void setCheckpoint(CheckpointManager.SaveInstanceStateRequest checkpoint) {
    assert checkpoint != null;
    saveBytes = checkpoint;
    nBytes = checkpoint.getSerializedSize();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(topologyName).append(" ").append(checkpointId)
        .append(" ").append(componentName).append(" ").append(instanceId);
    return sb.toString();
  }
}
