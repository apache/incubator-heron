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

package com.twitter.heron.statemgr.localfs;

import java.nio.charset.Charset;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.FileUtils;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.proto.system.ExecutionEnvironment;
import com.twitter.heron.proto.system.PackingPlans;
import com.twitter.heron.proto.system.PhysicalPlans;
import com.twitter.heron.proto.tmaster.TopologyMaster;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.statemgr.Lock;
import com.twitter.heron.spi.statemgr.WatchCallback;
import com.twitter.heron.statemgr.FileSystemStateManager;

public class LocalFileSystemStateManager extends FileSystemStateManager {
  private static final Logger LOG = Logger.getLogger(LocalFileSystemStateManager.class.getName());

  /**
   * Local filesystem implementation of a lock that mimics the file system behavior of the
   * distributed lock.
   */
  private final class FileSystemLock implements Lock {
    private String path;

    private FileSystemLock(String path) {
      this.path = path;
    }

    @Override
    public boolean tryLock(long timeout, TimeUnit unit) throws InterruptedException {
      long giveUpAtMillis = System.currentTimeMillis() + unit.toMillis(timeout);
      byte[] fileContents = Thread.currentThread().getName().getBytes(Charset.defaultCharset());
      while (true) {
        try {
          if (setData(this.path, fileContents, false).get()) {
            return true;
          } else if (System.currentTimeMillis() >= giveUpAtMillis) {
            return false;
          } else {
            TimeUnit.SECONDS.sleep(2); // need to pole the filesystem for availability
          }
        } catch (ExecutionException e) {
          // this is thrown when the file exists, which means the lock can't be obtained
        }
      }
    }

    @Override
    public void unlock() {
      deleteNode(this.path, false);
    }
  }

  @Override
  public void initialize(Config ipconfig) {

    super.initialize(ipconfig);

    // By default, we would init the file tree if it is not there
    boolean isInitLocalFileTree = LocalFileSystemContext.initLocalFileTree(ipconfig);

    if (isInitLocalFileTree && !initTree()) {
      throw new IllegalArgumentException("Failed to initialize Local State manager. "
          + "Check rootAddress: " + rootAddress);
    }
  }

  protected boolean initTree() {
    for (StateLocation location : StateLocation.values()) {
      String dir = getStateDirectory(location);
      LOG.fine(String.format("%s directory: %s", location.getName(), dir));
      if (!FileUtils.isDirectoryExists(dir) && !FileUtils.createDirectory(dir)) {
        return false;
      }
    }

    return true;
  }

  // Make utils class protected for easy unit testing
  protected ListenableFuture<Boolean> setData(String path, byte[] data, boolean overwrite) {
    final SettableFuture<Boolean> future = SettableFuture.create();
    boolean ret = FileUtils.writeToFile(path, data, overwrite);
    future.set(ret);

    return future;
  }

  private ListenableFuture<Boolean> setData(StateLocation location,
                                            String topologyName,
                                            byte[] bytes,
                                            boolean overwrite) {
    return setData(getStatePath(location, topologyName), bytes, overwrite);
  }

  @Override
  protected ListenableFuture<Boolean> nodeExists(String path) {
    SettableFuture<Boolean> future = SettableFuture.create();
    boolean ret = FileUtils.isFileExists(path);
    future.set(ret);

    return future;
  }

  @Override
  protected ListenableFuture<Boolean> deleteNode(String path, boolean deleteChildrenIfNecessary) {
    final SettableFuture<Boolean> future = SettableFuture.create();
    boolean ret = true;
    if (FileUtils.isFileExists(path)) {
      if (!deleteChildrenIfNecessary && FileUtils.hasChildren(path)) {
        LOG.severe("delete called on a path with children but deleteChildrenIfNecessary is false: "
            + path);
        ret = false;
      } else {
        ret = FileUtils.deleteFile(path);
      }
    }
    future.set(ret);

    return future;
  }

  @Override
  @SuppressWarnings("unchecked") // we don't know what M is until runtime
  protected <M extends Message> ListenableFuture<M> getNodeData(WatchCallback watcher,
                                                                String path,
                                                                Message.Builder builder) {
    final SettableFuture<M> future = SettableFuture.create();
    byte[] data = new byte[]{};
    if (FileUtils.isFileExists(path)) {
      data = FileUtils.readFromFile(path);
    }
    if (data.length == 0) {
      future.set(null);
      return future;
    }

    try {
      builder.mergeFrom(data);
      future.set((M) builder.build());
    } catch (InvalidProtocolBufferException e) {
      future.setException(new RuntimeException("Could not parse " + Message.Builder.class, e));
    }

    return future;
  }

  @Override
  public ListenableFuture<Boolean> setExecutionState(
      ExecutionEnvironment.ExecutionState executionState, String topologyName) {
    return setData(
        StateLocation.EXECUTION_STATE, topologyName, executionState.toByteArray(), false);
  }

  @Override
  public ListenableFuture<Boolean> setTMasterLocation(
      TopologyMaster.TMasterLocation location, String topologyName) {
    // Note: Unlike Zk statemgr, we overwrite the location even if there is already one.
    // This is because when running in simulator we control when a tmaster dies and
    // comes up deterministically.
    return setData(StateLocation.TMASTER_LOCATION, topologyName, location.toByteArray(), true);
  }

  @Override
  public ListenableFuture<Boolean> setTopology(TopologyAPI.Topology topology, String topologyName) {
    return setData(StateLocation.TOPOLOGY, topologyName, topology.toByteArray(), false);
  }

  @Override
  public ListenableFuture<Boolean> setPhysicalPlan(
      PhysicalPlans.PhysicalPlan physicalPlan, String topologyName) {
    return setData(StateLocation.PHYSICAL_PLAN, topologyName, physicalPlan.toByteArray(), false);
  }

  @Override
  public ListenableFuture<Boolean> setPackingPlan(
      PackingPlans.PackingPlan packingPlan, String topologyName) {
    return setData(StateLocation.PACKING_PLAN, topologyName, packingPlan.toByteArray(), true);
  }

  @Override
  public ListenableFuture<Boolean> setSchedulerLocation(
      Scheduler.SchedulerLocation location, String topologyName) {
    // Note: Unlike Zk statemgr, we overwrite the location even if there is already one.
    // This is because when running in simulator we control when a scheduler dies and
    // comes up deterministically.
    return setData(StateLocation.SCHEDULER_LOCATION, topologyName, location.toByteArray(), true);
  }

  @Override
  protected Lock getLock(String path) {
    return new FileSystemLock(path);
  }

  @Override
  public void close() {
    // We would not clear anything here
    // Scheduler kill interface should take care of the cleaning
  }

  public static void main(String[] args) throws ExecutionException, InterruptedException,
      IllegalAccessException, ClassNotFoundException, InstantiationException {
    Config config = Config.newBuilder()
        .put(Keys.stateManagerRootPath(),
            System.getProperty("user.home") + "/.herondata/repository/state/local")
        .build();
    LocalFileSystemStateManager stateManager = new LocalFileSystemStateManager();
    stateManager.doMain(args, config);
  }
}
