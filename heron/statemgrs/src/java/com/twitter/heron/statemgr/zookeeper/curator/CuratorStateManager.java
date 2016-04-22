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

package com.twitter.heron.statemgr.zookeeper.curator;

import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.Message;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.proto.system.ExecutionEnvironment;
import com.twitter.heron.proto.system.PhysicalPlans;
import com.twitter.heron.proto.tmaster.TopologyMaster;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.statemgr.WatchCallback;
import com.twitter.heron.statemgr.FileSystemStateManager;
import com.twitter.heron.statemgr.zookeeper.ZkContext;
import com.twitter.heron.statemgr.zookeeper.ZkWatcherCallback;

// TODO(mfu): Add Proxy or tunnel support, rather than to return the value stored directly
public class CuratorStateManager extends FileSystemStateManager {
  private static final Logger LOG = Logger.getLogger(CuratorStateManager.class.getName());
  private CuratorFramework client;
  private String connectionString;

  @Override
  public void initialize(Config config) {
    super.initialize(config);

    connectionString = Context.stateManagerConnectionString(config);

    // these are reasonable arguments for the ExponentialBackoffRetry. The first
    // retry will wait 1 second - the second will wait up to 2 seconds - the
    // third will wait up to 4 seconds.
    ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(
        ZkContext.retryIntervalMs(config), ZkContext.retryCount(config));

    // using the CuratorFrameworkFactory.builder() gives fine grained control
    // over creation options. See the CuratorFrameworkFactory.Builder javadoc
    // details
    client = CuratorFrameworkFactory.builder()
        .connectString(connectionString)
        .retryPolicy(retryPolicy)
        .connectionTimeoutMs(ZkContext.connectionTimeoutMs(config))
        .sessionTimeoutMs(ZkContext.sessionTimeoutMs(config))
            // etc. etc.
        .build();

    LOG.info("Starting client to: " + connectionString);

    // Start it
    client.start();

    try {
      if (!client.blockUntilConnected(ZkContext.connectionTimeoutMs(config),
          TimeUnit.MILLISECONDS)) {
        throw new RuntimeException("Failed to initialize CuratorClient");
      }
    } catch (InterruptedException e) {
      throw new RuntimeException("Failed to initialize CuratorClient", e);
    }

    if (ZkContext.isInitializeTree(config)) {
      try {
        initTree();
      } catch (Exception e) {
        throw new RuntimeException("Failed to initialize tree", e);
      }
    }
  }

  protected void initTree() throws Exception {
    // Make necessary directories
    LOG.info("Topologies directory: " + getTopologyDir());
    LOG.info("Tmaster location directory: " + getTMasterLocationDir());
    LOG.info("Physical plan directory: " + getPhysicalPlanDir());
    LOG.info("Execution state directory: " + getExecutionStateDir());
    LOG.info("Scheduler location directory: " + getSchedulerLocationDir());

    client.createContainers(getTopologyDir());
    client.createContainers(getTMasterLocationDir());
    client.createContainers(getPhysicalPlanDir());
    client.createContainers(getExecutionStateDir());
    client.createContainers(getSchedulerLocationDir());

    LOG.info("Directory tree initialized.");
  }

  @Override
  public void close() {
    if (client != null) {
      LOG.info("Closing the CuratorClient to: " + connectionString);
      client.close();
    }
  }

  public String getConnectionString() {
    return connectionString;
  }

  // Make utils class protected for easy unit testing
  protected ListenableFuture<Boolean> existNode(String path) {
    final SettableFuture<Boolean> result = SettableFuture.create();

    try {
      LOG.info("Checking exists for path: " + path);
      result.set(client.checkExists().forPath(path) != null);
    } catch (Exception e) {
      result.setException(new RuntimeException("Could not check Exist", e));
    }

    return result;
  }

  protected ListenableFuture<Boolean> createNode(String path, byte[] data, boolean isEphemeral) {
    final SettableFuture<Boolean> result = SettableFuture.create();

    try {
      client.create().
          withMode(isEphemeral ? CreateMode.EPHEMERAL : CreateMode.PERSISTENT)
          .forPath(path, data);
      LOG.info("Created node for path: " + path);
      result.set(true);
    } catch (Exception e) {
      result.setException(new RuntimeException("Could not createNode:", e));
    }
    return result;
  }

  protected ListenableFuture<Boolean> deleteNode(String path) {
    final SettableFuture<Boolean> result = SettableFuture.create();

    try {
      client.delete().withVersion(-1).forPath(path);
      LOG.info("Deleted node for path: " + path);
      result.set(true);
    } catch (Exception e) {
      result.setException(new RuntimeException("Could not deleteNode", e));
    }

    return result;
  }

  protected <M extends Message> ListenableFuture<M> getNodeData(
      WatchCallback watcher, String path, final Message.Builder builder) {
    final SettableFuture<M> future = SettableFuture.create();

    Watcher wc = ZkWatcherCallback.makeZkWatcher(watcher);

    BackgroundCallback cb = new BackgroundCallback() {
      @Override
      public void processResult(CuratorFramework aClient, CuratorEvent event) throws Exception {
        byte[] data;
        if (event != null & (data = event.getData()) != null) {
          builder.mergeFrom(data);
          future.set((M) builder.build());
        } else {
          future.setException(new RuntimeException("Failed to fetch data from path: "
              + event.getPath()));
        }
      }
    };

    try {
      client.getData().usingWatcher(wc).inBackground(cb).forPath(path);
    } catch (Exception e) {
      future.setException(new RuntimeException("Could not getData", e));
    }

    return future;
  }

  @Override
  public ListenableFuture<Boolean> setTMasterLocation(
      TopologyMaster.TMasterLocation location, String topologyName) {
    return createNode(getTMasterLocationPath(topologyName), location.toByteArray(), true);
  }

  @Override
  public ListenableFuture<Boolean> setExecutionState(
      ExecutionEnvironment.ExecutionState executionState, String topologyName) {
    return createNode(getExecutionStatePath(topologyName), executionState.toByteArray(), false);
  }

  @Override
  public ListenableFuture<Boolean> setTopology(TopologyAPI.Topology topology, String topologyName) {
    return createNode(getTopologyPath(topologyName), topology.toByteArray(), false);
  }

  @Override
  public ListenableFuture<Boolean> setPhysicalPlan(
      PhysicalPlans.PhysicalPlan physicalPlan, String topologyName) {
    return createNode(getPhysicalPlanPath(topologyName), physicalPlan.toByteArray(), false);
  }

  @Override
  public ListenableFuture<Boolean> setSchedulerLocation(
      Scheduler.SchedulerLocation location, String topologyName) {
    return createNode(getSchedulerLocationPath(topologyName), location.toByteArray(), true);
  }

  @Override
  public ListenableFuture<Boolean> deleteTMasterLocation(String topologyName) {
    // It is a EPHEMERAL node and would be removed automatically
    final SettableFuture<Boolean> result = SettableFuture.create();
    result.set(true);
    return result;
  }

  @Override
  public ListenableFuture<Boolean> deleteExecutionState(String topologyName) {
    return deleteNode(getExecutionStatePath(topologyName));
  }

  @Override
  public ListenableFuture<Boolean> deleteTopology(String topologyName) {
    return deleteNode(getTopologyPath(topologyName));
  }

  @Override
  public ListenableFuture<Boolean> deletePhysicalPlan(String topologyName) {
    return deleteNode(getPhysicalPlanPath(topologyName));
  }

  @Override
  public ListenableFuture<Boolean> deleteSchedulerLocation(String topologyName) {
    // It is a EPHEMERAL node and would be removed automatically
    final SettableFuture<Boolean> result = SettableFuture.create();
    result.set(true);
    return result;
  }

  @Override
  public ListenableFuture<TopologyMaster.TMasterLocation> getTMasterLocation(
      WatchCallback watcher, String topologyName) {
    return getNodeData(watcher, getTMasterLocationPath(topologyName),
        TopologyMaster.TMasterLocation.newBuilder());
  }

  @Override
  public ListenableFuture<Scheduler.SchedulerLocation> getSchedulerLocation(
      WatchCallback watcher, String topologyName) {
    return getNodeData(watcher, getSchedulerLocationPath(topologyName),
        Scheduler.SchedulerLocation.newBuilder());
  }

  @Override
  public ListenableFuture<TopologyAPI.Topology> getTopology(
      WatchCallback watcher, String topologyName) {
    return getNodeData(watcher, getTopologyPath(topologyName),
        TopologyAPI.Topology.newBuilder());
  }

  @Override
  public ListenableFuture<ExecutionEnvironment.ExecutionState> getExecutionState(
      WatchCallback watcher, String topologyName) {
    return getNodeData(watcher, getExecutionStatePath(topologyName),
        ExecutionEnvironment.ExecutionState.newBuilder());
  }

  @Override
  public ListenableFuture<PhysicalPlans.PhysicalPlan> getPhysicalPlan(
      WatchCallback watcher, String topologyName) {
    return getNodeData(watcher, getPhysicalPlanPath(topologyName),
        PhysicalPlans.PhysicalPlan.newBuilder());
  }

  @Override
  public ListenableFuture<Boolean> isTopologyRunning(String topologyName) {
    return existNode(getTopologyPath(topologyName));
  }
}
