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

/**
 * Defines a stateful component that is aware of Heron topology's "two-phase commit".
 *
 * Note tasks saving a distributed checkpoint would be the "prepare" phase of the two-phase commit
 * algorithm. When a distributed checkpoint is done, we can say that all tasks agree that they
 * will not roll back to the time before that distributed checkpoint, and the "prepare" phase is
 * complete.
 *
 * When the "prepare" phase is complete, Heron will invoke the "postSave" hook to signal the
 * beginning of the "commit" phase. If there is a failure occurred during the "prepare" phase,
 * Heron will invoke the hook "preRestore" to signal that two-phase commit is aborted, and the
 * topology will be rolled back to the previous checkpoint.
 *
 * Note that the commit phase will finish after the postSave hook exits successfully. Then, the
 * prepare phase of the following checkpoint will begin.
 *
 * In addition, for two-phase stateful components specifically, Heron will not execute (for bolts)
 * or produce (for spouts) tuples between preSave and postSave. This will guarantee that the prepare
 * phase of the next checkpoint will not overlap with the commit phase of the current checkpoint
 * (eg. we block execution of tuples from the next checkpoint unless commit phase is done).
 *
 * See the end-to-end effectively-once designed doc (linked in the PR of this commit) for more
 * details.
 */
public interface ITwoPhaseStatefulComponent<K extends Serializable, V extends Serializable>
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
