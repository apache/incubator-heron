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

import java.util.Map;

import org.apache.heron.proto.system.PhysicalPlans;

public interface IStatefulStorage {
  /**
   * Initialize the Stateful Storage
   *
   * @param conf An unmodifiableMap containing basic configuration
   * Attempts to modify the returned map,
   * whether direct or via its collection views, result in an UnsupportedOperationException.
   */
  void init(Map<String, Object> conf) throws StatefulStorageException;

  /**
   * Closes the Stateful Storage
   */
  void close();

  // Store the checkpoint
  void store(Checkpoint checkpoint) throws StatefulStorageException;

  // Retrieve the checkpoint
  Checkpoint restore(String topologyName, String checkpointId,
                     PhysicalPlans.Instance instanceInfo) throws StatefulStorageException;

  // TODO(mfu): We should refactor all interfaces in IStatefulStorage,
  // TODO(mfu): instead providing Class Checkpoint, we should provide an Context class,
  // TODO(mfu): It should:
  // TODO(mfu): 1. Provide meta data access, like topologyName
  // TODO(mfu): 2. Provide utils method to parse the protobuf object, like getTaskId()
  // TODO(mfu): 3. Common methods, like getCheckpointDir()
  // Dispose the checkpoint
  void dispose(String topologyName, String oldestCheckpointId, boolean deleteAll)
                  throws StatefulStorageException;
}
