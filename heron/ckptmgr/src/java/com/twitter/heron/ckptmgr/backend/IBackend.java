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

package com.twitter.heron.ckptmgr.backend;

import java.util.Map;

import com.twitter.heron.proto.ckptmgr.CheckpointManager;

public interface IBackend {
  /**
   * Initialize the Checkpoints Backend
   *
   * @param conf An unmodifiableMap containing basic configuration
   * Attempts to modify the returned map,
   * whether direct or via its collection views, result in an UnsupportedOperationException.
   */
  void init(Map<String, Object> conf);

  /**
   * Closes the Checkpoints Backend
   */
  void close();

  // Store the checkpoint
  boolean store(Checkpoint checkpoint);

  // Retrieve the checkpoint
  boolean restore(Checkpoint checkpoint);

  // TODO(mfu): We should refactor all interfaces in IBackend,
  // TODO(mfu): instead providing Class Checkpoint, we should provide an Context class,
  // TODO(mfu): It should:
  // TODO(mfu): 1. Provide meta data access, like topologyName
  // TODO(mfu): 2. Provide utils method to parse the protobuf object, like getTaskId()
  // TODO(mfu): 3. Common methods, like getCheckpointDir()
  // Dispose the checkpoint
  boolean dispose(CheckpointManager.CleanStatefulCheckpointRequest request,
                  String topologyName);
}
