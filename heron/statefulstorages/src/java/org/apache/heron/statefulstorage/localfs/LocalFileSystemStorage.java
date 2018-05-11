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

package org.apache.heron.statefulstorage.localfs;

import java.io.File;
import java.util.Map;
import java.util.logging.Logger;

import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.heron.common.basics.FileUtils;
import org.apache.heron.proto.ckptmgr.CheckpointManager;
import org.apache.heron.spi.statefulstorage.Checkpoint;
import org.apache.heron.spi.statefulstorage.CheckpointInfo;
import org.apache.heron.spi.statefulstorage.CheckpointMetadata;
import org.apache.heron.spi.statefulstorage.IStatefulStorage;
import org.apache.heron.spi.statefulstorage.StatefulStorageException;

public class LocalFileSystemStorage implements IStatefulStorage {
  private static final Logger LOG = Logger.getLogger(LocalFileSystemStorage.class.getName());

  private static final String ROOT_PATH_KEY = "heron.statefulstorage.localfs.root.path";

  private String checkpointRootPath;
  private String topologyName;

  @Override
  public void init(String topology, final Map<String, Object> conf)
      throws StatefulStorageException {
    this.topologyName = topology;
    checkpointRootPath = (String) conf.get(ROOT_PATH_KEY);
    if (checkpointRootPath != null) {
      checkpointRootPath = checkpointRootPath.replaceFirst("^~", System.getProperty("user.home"));
    }
    LOG.info("Initialing... Checkpoint root path: " + checkpointRootPath);
  }

  @Override
  public void close() {
    // Nothing to do here
  }

  @Override
  public void storeCheckpoint(CheckpointInfo info, Checkpoint checkpoint)
      throws StatefulStorageException {
    String path = getCheckpointPath(info.getCheckpointId(),
                                    info.getComponent(), info.getPartitionId());

    // We would try to create but we would not enforce this operation successful,
    // since it is possible already created by others
    String checkpointDir = getCheckpointDir(info.getCheckpointId(),
                                            info.getComponent());
    FileUtils.createDirectory(checkpointDir);

    // Do a check after the attempt
    if (!FileUtils.isDirectoryExists(checkpointDir)) {
      throw new StatefulStorageException("Failed to create dir: " + checkpointDir);
    }

    byte[] contents = checkpoint.getCheckpoint().toByteArray();

    // In fact, no need atomic write, since our mechanism requires only best effort
    if (!FileUtils.writeToFile(path, contents, true)) {
      throw new StatefulStorageException("Failed to persist checkpoint to: " + path);
    }
  }

  @Override
  public Checkpoint restoreCheckpoint(CheckpointInfo info)
      throws StatefulStorageException {
    String path = getCheckpointPath(info.getCheckpointId(), info.getComponent(),
                                    info.getPartitionId());

    byte[] res = FileUtils.readFromFile(path);
    if (res.length != 0) {
      // Try to parse the protobuf
      CheckpointManager.InstanceStateCheckpointPartition state;
      try {
        state =
            CheckpointManager.InstanceStateCheckpointPartition.parseFrom(res);
      } catch (InvalidProtocolBufferException e) {
        throw new StatefulStorageException("Failed to parse the data", e);
      }
      return new Checkpoint(state);
    } else {
      throw new StatefulStorageException("Failed to parse the data");
    }
  }

  @Override
  public void storeComponentMetaData(CheckpointInfo info, CheckpointMetadata metadata)
      throws StatefulStorageException {
    // TODO(nwang): To implement
  }

  @Override
  public CheckpointMetadata restoreComponentMetadata(CheckpointInfo info)
      throws StatefulStorageException {
    // TODO(nwang): To implement
    return null;
  }

  @Override
  public void dispose(String oldestCheckpointPreserved, boolean deleteAll)
      throws StatefulStorageException {
    String topologyCheckpointRoot = getTopologyCheckpointRoot();

    if (deleteAll) {
      // Clean all checkpoint states
      FileUtils.deleteDir(topologyCheckpointRoot);
      if (FileUtils.isDirectoryExists(topologyCheckpointRoot)) {
        throw new StatefulStorageException("Failed to delete " + topologyCheckpointRoot);
      }
    } else {
      String[] names = new File(topologyCheckpointRoot).list();
      if (names == null) {
        LOG.warning("There is no such checkpoint root path: " + topologyCheckpointRoot);
      } else {
        for (String name : names) {
          if (name.compareTo(oldestCheckpointPreserved) < 0) {
            FileUtils.deleteDir(new File(topologyCheckpointRoot, name), true);
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
  }

  private String getTopologyCheckpointRoot() {
    return String.format("%s/%s", checkpointRootPath, topologyName);
  }

  private String getCheckpointDir(String checkpointId, String componentName) {
    return String.format("%s/%s/%s", getTopologyCheckpointRoot(), checkpointId, componentName);
  }

  private String getCheckpointPath(String checkpointId, String componentName, int taskId) {
    return String.format("%s/%d", getCheckpointDir(checkpointId, componentName), taskId);
  }
}
