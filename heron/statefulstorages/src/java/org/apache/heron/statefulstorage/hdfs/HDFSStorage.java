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

package org.apache.heron.statefulstorage.hdfs;

import java.io.IOException;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.heron.common.basics.SysUtils;
import org.apache.heron.proto.ckptmgr.CheckpointManager;
import org.apache.heron.spi.statefulstorage.Checkpoint;
import org.apache.heron.spi.statefulstorage.CheckpointInfo;
import org.apache.heron.spi.statefulstorage.CheckpointMetadata;
import org.apache.heron.spi.statefulstorage.IStatefulStorage;
import org.apache.heron.spi.statefulstorage.StatefulStorageException;

/**
 * Note: The hadoop cluster config should be provided through the classpath
 */
public class HDFSStorage implements IStatefulStorage {
  private static final Logger LOG = Logger.getLogger(HDFSStorage.class.getName());

  private static final String ROOT_PATH_KEY = "heron.statefulstorage.hdfs.root.path";

  private String checkpointRootPath;
  private FileSystem fileSystem;
  private String topologyName;

  @Override
  public void init(String topology, final Map<String, Object> conf)
      throws StatefulStorageException {
    LOG.info("Initializing... Config: " + conf.toString());
    LOG.info("Class path: " + System.getProperty("java.class.path"));

    this.topologyName = topology;
    checkpointRootPath = (String) conf.get(ROOT_PATH_KEY);

    // Notice, we pass the config folder via classpath
    // So hadoop will automatically search config files from classpath
    Configuration hadoopConfig = new Configuration();

    try {
      fileSystem = FileSystem.get(hadoopConfig);
      LOG.info("Hadoop FileSystem URI: " + fileSystem.getUri()
          + " ; Home Dir: " + fileSystem.getHomeDirectory());
    } catch (IOException e) {
      throw new StatefulStorageException("Failed to get hadoop file system", e);
    }
  }

  @Override
  public void close() {
    SysUtils.closeIgnoringExceptions(fileSystem);
  }

  @Override
  public void storeCheckpoint(CheckpointInfo info, Checkpoint checkpoint)
      throws StatefulStorageException {
    Path path = new Path(getCheckpointPath(info.getCheckpointId(),
                                           info.getComponent(),
                                           info.getInstanceId()));

    // We need to ensure the existence of directories structure,
    // since it is not guaranteed that FileSystem.create(..) always creates parents' dirs.
    String checkpointDir = getCheckpointDir(info.getCheckpointId(),
                                            info.getComponent());
    createDir(checkpointDir);

    FSDataOutputStream out = null;
    try {
      out = fileSystem.create(path);
      checkpoint.getCheckpoint().writeTo(out);
    } catch (IOException e) {
      throw new StatefulStorageException("Failed to persist", e);
    } finally {
      SysUtils.closeIgnoringExceptions(out);
    }
  }

  @Override
  public Checkpoint restoreCheckpoint(CheckpointInfo info)
      throws StatefulStorageException {
    Path path = new Path(getCheckpointPath(info.getCheckpointId(),
                                           info.getComponent(),
                                           info.getInstanceId()));

    FSDataInputStream in = null;
    CheckpointManager.InstanceStateCheckpoint state = null;
    try {
      in = fileSystem.open(path);
      state =
          CheckpointManager.InstanceStateCheckpoint.parseFrom(in);
    } catch (IOException e) {
      throw new StatefulStorageException("Failed to read", e);
    } finally {
      SysUtils.closeIgnoringExceptions(in);
    }
    return new Checkpoint(state);
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
    Path topologyRootPath = new Path(topologyCheckpointRoot);

    if (deleteAll) {
      // Clean all checkpoint states
      try {
        fileSystem.delete(topologyRootPath, true);
        if (fileSystem.exists(topologyRootPath)) {
          throw new StatefulStorageException("Failed to delete " + topologyRootPath);
        }
      } catch (IOException e) {
        throw new StatefulStorageException("Error while deleting " + topologyRootPath, e);
      }
    } else {
      try {
        FileStatus[] statuses = fileSystem.listStatus(topologyRootPath);
        for (FileStatus status : statuses) {
          String name = status.getPath().getName();
          if (name.compareTo(oldestCheckpointPreserved) < 0) {
            fileSystem.delete(status.getPath(), true);
          }
        }

        // Do a double check. Now all checkpoints with smaller checkpoint id should be cleaned
        statuses = fileSystem.listStatus(topologyRootPath);
        for (FileStatus status : statuses) {
          String name = status.getPath().getName();
          if (name.compareTo(oldestCheckpointPreserved) < 0) {
            throw new StatefulStorageException("Error while deleting " + name);
          }
        }

      } catch (IOException e) {
        throw new StatefulStorageException("Failed to clean to: " + oldestCheckpointPreserved, e);
      }
    }
  }

  /**
   * Creates the directory if it does not exist.
   *
   * @param dir The path of dir to ensure existence
   */
  protected void createDir(String dir) throws StatefulStorageException {
    Path path = new Path(dir);

    try {
      fileSystem.mkdirs(path);
      if (!fileSystem.exists(path)) {
        throw new StatefulStorageException("Failed to create dir: " + dir);
      }
    } catch (IOException e) {
      throw new StatefulStorageException("Failed to create dir: " + dir, e);
    }
  }

  private String getTopologyCheckpointRoot() {
    return String.format("%s/%s", checkpointRootPath, topologyName);
  }

  private String getCheckpointDir(String checkpointId, String componentName) {
    return String.format("%s/%s/%s",
        getTopologyCheckpointRoot(), checkpointId, componentName);
  }

  private String getCheckpointPath(String checkpointId, String componentName, int taskId) {
    return String.format("%s/%d", getCheckpointDir(checkpointId, componentName), taskId);
  }
}
