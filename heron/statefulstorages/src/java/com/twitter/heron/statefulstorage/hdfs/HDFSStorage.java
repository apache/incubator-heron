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

package com.twitter.heron.statefulstorage.hdfs;

import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.twitter.heron.common.basics.SysUtils;
import com.twitter.heron.proto.ckptmgr.CheckpointManager;
import com.twitter.heron.spi.statefulstorage.Checkpoint;
import com.twitter.heron.spi.statefulstorage.IStatefulStorage;

/**
 * Note: The hadoop cluster config should be provided through the classpath
 */
public class HDFSStorage implements IStatefulStorage {
  private static final Logger LOG = Logger.getLogger(HDFSStorage.class.getName());

  private static final String ROOT_PATH_KEY = "root.path";

  private String checkpointRootPath;
  private FileSystem fileSystem;

  @Override
  public void init(Map<String, Object> conf) {
    LOG.info("Initialing... Config: " + conf.toString());
    LOG.info("Class path: " + System.getProperty("java.class.path"));

    checkpointRootPath = (String) conf.get(ROOT_PATH_KEY);

    // Notice, we pass the config folder via classpath
    // So hadoop will automatically search config files from classpath
    Configuration hadoopConfig = new Configuration();

    try {
      fileSystem = FileSystem.get(hadoopConfig);
      LOG.info("URI: " + fileSystem.getUri() + " ; Home Dir: " + fileSystem.getHomeDirectory());
    } catch (IOException e) {
      throw new RuntimeException("Failed to get hadoop file system", e);
    }
  }

  @Override
  public void close() {
    SysUtils.closeIgnoringExceptions(fileSystem);
  }

  @Override
  public boolean store(Checkpoint checkpoint) {
    Path path = new Path(getCheckpointPath(checkpoint));

    // We need to ensure the existence of directories structure,
    // since it is not guaranteed that FileSystem.create(..) always creates parents' dirs.
    if (!ensureDirExists(getCheckpointDir(checkpoint))) {
      LOG.warning("Failed to ensure dir exists: " + getCheckpointDir(checkpoint));
      return false;
    }

    FSDataOutputStream out = null;
    try {
      out = fileSystem.create(path);
      checkpoint.checkpoint().writeTo(out);
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to persist", e);
      return false;
    } finally {
      SysUtils.closeIgnoringExceptions(out);
    }

    return true;
  }

  @Override
  public boolean restore(Checkpoint checkpoint) {
    Path path = new Path(getCheckpointPath(checkpoint));

    FSDataInputStream in = null;
    CheckpointManager.SaveInstanceStateRequest state = null;
    try {
      in = fileSystem.open(path);
      state =
          CheckpointManager.SaveInstanceStateRequest.parseFrom(in);
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to read", e);
      return false;
    } finally {
      SysUtils.closeIgnoringExceptions(in);
    }

    checkpoint.setCheckpoint(state);

    return true;
  }

  @Override
  public boolean dispose(String topologyName, String oldestCheckpointPreserved,
                         boolean deleteAll) {
    String topologyCheckpointRoot = getTopologyCheckpointRoot(topologyName);
    Path topologyRootPath = new Path(topologyCheckpointRoot);


    if (deleteAll) {
      // Clean all checkpoint states
      try {
        fileSystem.delete(topologyRootPath, true);
        if (fileSystem.exists(topologyRootPath)) {
          return false;
        }
      } catch (IOException e) {
        LOG.log(Level.SEVERE, "Failed to delete: " + topologyCheckpointRoot, e);
        return false;
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
            return false;
          }
        }

      } catch (IOException e) {
        LOG.log(Level.SEVERE, "Failed to clean to: " + oldestCheckpointPreserved, e);
        return false;
      }
    }


    return true;
  }

  /**
   * Ensure the existence of a directory.
   * Will create the directory if it does not exist.
   *
   * @param dir The path of dir to ensure existence
   * @return true if the directory exists after this call.
   */
  protected boolean ensureDirExists(String dir) {
    Path path = new Path(dir);

    try {
      fileSystem.mkdirs(path);
      if (!fileSystem.exists(path)) {
        return false;
      }
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to mkdirs: " + dir, e);
      return false;
    }

    return true;
  }

  protected String getTopologyCheckpointRoot(String topologyName) {
    return String.format("%s/%s", checkpointRootPath, topologyName);
  }

  protected String getCheckpointDir(Checkpoint checkpoint) {
    return String.format("%s/%s/%s", getTopologyCheckpointRoot(checkpoint.getTopologyName()),
                         checkpoint.getCheckpointId(), checkpoint.getComponent());
  }

  protected String getCheckpointPath(Checkpoint checkpoint) {
    return String.format("%s/%d", getCheckpointDir(checkpoint), checkpoint.getTaskId());
  }
}
