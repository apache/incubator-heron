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

package com.twitter.heron.statefulstorage.localfs;

import java.io.File;
import java.util.Map;
import java.util.logging.Logger;

import com.google.protobuf.InvalidProtocolBufferException;

import com.twitter.heron.common.basics.FileUtils;
import com.twitter.heron.proto.ckptmgr.CheckpointManager;
import com.twitter.heron.spi.statefulstorage.Checkpoint;
import com.twitter.heron.spi.statefulstorage.IStatefulStorage;
import com.twitter.heron.spi.statefulstorage.StatefulStorageException;

public class LocalFileSystemStorage implements IStatefulStorage {
  private static final Logger LOG = Logger.getLogger(LocalFileSystemStorage.class.getName());

  private static final String ROOT_PATH_KEY = "root.path";

  private String checkpointRootPath;

  @Override
  public void init(Map<String, Object> conf) throws StatefulStorageException {
    checkpointRootPath = (String) conf.get(ROOT_PATH_KEY);
    LOG.info("Initialing... Checkpoint root path: " + checkpointRootPath);
  }

  @Override
  public void close() {
    // Nothing to do here
  }

  @Override
  public void store(Checkpoint checkpoint) throws StatefulStorageException {
    String path = getCheckpointPath(checkpoint);

    // We would try to create but we would not enforce this operation successful,
    // since it is possible already created by others
    FileUtils.createDirectory(getCheckpointDir(checkpoint));

    // Do a check after the attempt
    if (!FileUtils.isDirectoryExists(getCheckpointDir(checkpoint))) {
      throw new StatefulStorageException("Failed to create dir: " + getCheckpointDir(checkpoint));
    }

    byte[] contents = checkpoint.checkpoint().toByteArray();

    // In fact, no need atomic write, since our mechanism requires only best effort
    if (!FileUtils.writeToFile(path, contents, true)) {
      throw new StatefulStorageException("Failed to persist checkpoint to: " + path);
    }
  }

  @Override
  public void restore(Checkpoint checkpoint) throws StatefulStorageException {
    String path = getCheckpointPath(checkpoint);

    byte[] res = FileUtils.readFromFile(path);
    if (res.length != 0) {
      // Try to parse the protobuf
      CheckpointManager.SaveInstanceStateRequest state;
      try {
        state =
            CheckpointManager.SaveInstanceStateRequest.parseFrom(res);
      } catch (InvalidProtocolBufferException e) {
        throw new StatefulStorageException("Failed to parse the data", e);
      }
      checkpoint.setCheckpoint(state);
    }
  }

  @Override
  public void dispose(String topologyName, String oldestCheckpointPreserved,
                      boolean deleteAll) throws StatefulStorageException {
    String topologyCheckpointRoot = getTopologyCheckpointRoot(topologyName);

    if (deleteAll) {
      // Clean all checkpoint states
      FileUtils.deleteDir(topologyCheckpointRoot);
      if (FileUtils.isDirectoryExists(topologyCheckpointRoot)) {
        throw new StatefulStorageException("Failed to delete " + topologyCheckpointRoot);
      }
    } else {
      String[] names = new File(topologyCheckpointRoot).list();
      for (String name : names) {
        if (name.compareTo(oldestCheckpointPreserved) < 0) {
          FileUtils.deleteDir(new File(topologyCheckpointRoot, name));
        }
      }

      // Do a double check. Now all checkpoints with smaller checkpoint id should be cleaned
      names = new File(topologyCheckpointRoot).list();
      for (String name : names) {
        if (name.compareTo(oldestCheckpointPreserved) < 0) {
          throw new StatefulStorageException("Failed to delete " + name);
        }
      }
    }
  }

  protected String getTopologyCheckpointRoot(String topologyName) {
    return new StringBuilder()
        .append(checkpointRootPath).append("/")
        .append(topologyName)
        .toString();
  }

  protected String getCheckpointDir(Checkpoint checkpoint) {
    return new StringBuilder()
        .append(getTopologyCheckpointRoot(checkpoint.getTopologyName())).append("/")
        .append(checkpoint.getCheckpointId()).append("/")
        .append(checkpoint.getComponent())
        .toString();
  }

  protected String getCheckpointPath(Checkpoint checkpoint) {
    return new StringBuilder()
        .append(getCheckpointDir(checkpoint)).append("/")
        .append(checkpoint.getTaskId())
        .toString();
  }
}
