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

package org.apache.heron.statefulstorage;

import com.google.protobuf.ByteString;

import org.apache.heron.proto.ckptmgr.CheckpointManager;
import org.apache.heron.proto.system.PhysicalPlans;

public final class StatefulStorageTestContext {

  private StatefulStorageTestContext() {
  }

  public static final String TOPOLOGY_NAME = "topology_name";
  public static final String CHECKPOINT_ID = "checkpoint_id";
  public static final String ROOT_PATH_KEY = "root.path";
  public static final String ROOT_PATH = "localFSTest";
  public static final String STMGR_ID = "stmgr_id";
  public static final String INSTANCE_ID = "instance_id";
  public static final String COMPONENT_NAME = "component_name";
  public static final int TASK_ID = 1;
  public static final int COMPONENT_INDEX = 1;
  public static final byte[] BYTES = "LocalFS test bytes".getBytes();

  public static PhysicalPlans.Instance getInstance() {
    PhysicalPlans.InstanceInfo info = PhysicalPlans.InstanceInfo.newBuilder()
        .setTaskId(StatefulStorageTestContext.TASK_ID)
        .setComponentIndex(StatefulStorageTestContext.COMPONENT_INDEX)
        .setComponentName(StatefulStorageTestContext.COMPONENT_NAME)
        .build();

    return PhysicalPlans.Instance.newBuilder()
        .setInstanceId(StatefulStorageTestContext.INSTANCE_ID)
        .setStmgrId(StatefulStorageTestContext.STMGR_ID)
        .setInfo(info)
        .build();
  }

  public static CheckpointManager.InstanceStateCheckpoint getInstanceStateCheckpoint() {
    return CheckpointManager.InstanceStateCheckpoint.newBuilder()
        .setCheckpointId(StatefulStorageTestContext.CHECKPOINT_ID)
        .setState(ByteString.copyFrom(StatefulStorageTestContext.BYTES))
        .build();
  }
}
