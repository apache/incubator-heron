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

package org.apache.heron.api.topology;

import java.io.Serializable;

public interface I2PhaseCommitComponent<K extends Serializable, V extends Serializable>
    extends IStatefulComponent<K, V> {

  /**
   * This is a hook for the component to perform some actions after a checkpoint is persisted
   * successfully for all components in the topology.
   *
   * @param checkpointId the ID of the checkpoint
   */
  void postSave(String checkpointId);

  /**
   * This is a hook for the component to perform some actions (eg. state clean-up) before the
   * framework attempts to delete the component and restore it to a previously-saved checkpoint.
   *
   * @param checkpointId the ID of the checkpoint that the component is being restored to
   */
  void preRestore(String checkpointId);

}
