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

package org.apache.heron.spi.statefulstorage;

import org.apache.heron.proto.ckptmgr.CheckpointManager;

/**
 * The checkpoint data from an instance. It contains the instance information and the checkpoint
 * data for the instance.
 * TODO(nwang): Currently each instance has only one partition.
 */
public class Checkpoint {
  private CheckpointManager.InstanceStateCheckpoint checkpoint;
  private int nBytes;

  public Checkpoint(CheckpointManager.InstanceStateCheckpoint checkpoint) {
    this.checkpoint = checkpoint;
    this.nBytes = checkpoint.getSerializedSize();
  }

  public CheckpointManager.InstanceStateCheckpoint getCheckpoint() {
    return this.checkpoint;
  }

  @Override
  public String toString() {
    return String.format("Checkpoint(%d bytes)", nBytes);
  }
}
