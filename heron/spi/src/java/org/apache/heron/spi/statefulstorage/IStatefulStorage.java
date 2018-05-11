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

/**
 * The interface of all storage classes for checkpoints.
 * For each checkpoint, two types of data are stored:
 * - Component Meta Data (one per component).
 * - Instance Checkpoint Data (one per instance or patition)
 * Each Stateful Storage implementation needs to handle them accordingly.
 */
public interface IStatefulStorage {
  /**
   * Initialize the Stateful Storage
   * @param topologyName The name of the topology.
   * @param conf An unmodifiableMap containing basic configuration
   */
  void init(String topologyName, final Map<String, Object> conf)
      throws StatefulStorageException;

  /**
   * Closes the Stateful Storage
   */
  void close();

  /**
   * Store instance checkpoint.
   * @param info The information (reference key) for the checkpoint partition.
   * @param checkpoint The checkpoint data.
   */
  void storeCheckpoint(final CheckpointInfo info, final Checkpoint checkpoint)
      throws StatefulStorageException;

  /**
   * Retrieve instance checkpoint.
   * @param info The information (reference key) for the checkpoint partition.
   * @return The checkpoint data from the specified blob id.
   */
  Checkpoint restoreCheckpoint(final CheckpointInfo info)
        throws StatefulStorageException;

  /**
   * Store medata data for component. Ideally in distributed storages this function should only
   * be called once for each component. In local storages, the function should be called by
   * every instance and the data should be stored with the checkpoint data for each partition.
   * @param info The information (reference key) for the checkpoint partition.
   * @param metadata The checkpoint metadata from a component.
   */
  void storeComponentMetaData(final CheckpointInfo info, final CheckpointMetadata metadata)
      throws StatefulStorageException;

  /**
   * Retrieve component metadata.
   * @param info The information (reference key) for the checkpoint partition.
   * @return The checkpoint metadata for the component.
   */
  CheckpointMetadata restoreComponentMetadata(final CheckpointInfo info)
      throws StatefulStorageException;

  /**
   * Dispose checkpoints.
   * @param oldestCheckpointPreserved The oldest checkpoint id to be preserved. All checkpoints
   *        before this id should be deleted.
   * @param deleteAll Ignore the checkpoint Id and delete all checkpoints.
   */
  void dispose(String oldestCheckpointPreserved, boolean deleteAll)
      throws StatefulStorageException;
}
