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

package org.apache.heron.statemgr.zookeeper.curator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.Message;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.DeleteBuilder;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.api.utils.Slf4jUtils;
import org.apache.heron.common.basics.Pair;
import org.apache.heron.proto.ckptmgr.CheckpointManager;
import org.apache.heron.proto.scheduler.Scheduler;
import org.apache.heron.proto.system.ExecutionEnvironment;
import org.apache.heron.proto.system.PackingPlans;
import org.apache.heron.proto.system.PhysicalPlans;
import org.apache.heron.proto.tmanager.TopologyManager;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Context;
import org.apache.heron.spi.common.Key;
import org.apache.heron.spi.statemgr.Lock;
import org.apache.heron.spi.statemgr.WatchCallback;
import org.apache.heron.spi.utils.NetworkUtils;
import org.apache.heron.statemgr.FileSystemStateManager;
import org.apache.heron.statemgr.zookeeper.ZkContext;
import org.apache.heron.statemgr.zookeeper.ZkUtils;
import org.apache.heron.statemgr.zookeeper.ZkWatcherCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

public class CuratorStateManager extends FileSystemStateManager {
  private static final Logger LOG = Logger.getLogger(CuratorStateManager.class.getName());
  private static final int TUNNEL_SETUP_RETRY = 0;  // 0 means no retry
  private static final int TUNNEL_SETUP_RETRY_SLEEP_SEC = 5;

  private CuratorFramework client;
  private String connectionString;
  private boolean isSchedulerService;
  private List<Process> tunnelProcesses;
  private Config config;

  @Override
  public void initialize(Config newConfig) {
    super.initialize(newConfig);

    this.config = newConfig;
    this.connectionString = Context.stateManagerConnectionString(newConfig);
    this.isSchedulerService = Context.schedulerService(newConfig);
    this.tunnelProcesses = new ArrayList<>();

    NetworkUtils.TunnelConfig tunnelConfig =
        NetworkUtils.TunnelConfig.build(config, NetworkUtils.HeronSystem.STATE_MANAGER);

    if (tunnelConfig.isTunnelNeeded()) {
      for (int setupCount = 0;; ++setupCount) {
        Pair<String, List<Process>> tunneledResults = setupZkTunnel(tunnelConfig);
        String newConnectionString = tunneledResults.first;

        // If tunnel can't be setup correctly. Retry or bail.
        if (!newConnectionString.isEmpty()) {
          // Success, use the new connection string
          connectionString = newConnectionString;
          tunnelProcesses.addAll(tunneledResults.second);
          break;
        } else {
          if (setupCount < TUNNEL_SETUP_RETRY) {
            try {
              TimeUnit.SECONDS.sleep(TUNNEL_SETUP_RETRY_SLEEP_SEC);
            } catch (InterruptedException ex) {
              Thread.currentThread().interrupt();
            }
          } else {
            throw new IllegalArgumentException("Failed to connect to tunnel host '"
                + tunnelConfig.getTunnelHost() + "'");
          }
        }
      }
    }

    // Start it
    client = getCuratorClient();
    LOG.info("Starting Curator client connecting to: " + connectionString);
    client.start();

    try {
      if (!client.blockUntilConnected(ZkContext.connectionTimeoutMs(newConfig),
          TimeUnit.MILLISECONDS)) {
        throw new RuntimeException("Failed to connect to " + connectionString);
      }
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted from blockUntilConnected(): " + connectionString, e);
    }

    if (ZkContext.isInitializeTree(newConfig)) {
      initTree();
    }
  }

  /**
   * Lock backed by {@code InterProcessSemaphoreMutex}. Guaranteed to atomically get a
   * distributed ephemeral lock backed by zookeeper. The lock should be explicitly released to
   * avoid unnecessary waiting by other threads waiting on it.
   */
  private final class DistributedLock implements Lock {
    private String path;
    private InterProcessSemaphoreMutex lock;

    private DistributedLock(CuratorFramework client, String path) {
      this.path = path;
      this.lock = new InterProcessSemaphoreMutex(client, path);
    }

    @Override
    public boolean tryLock(long timeout, TimeUnit unit) throws InterruptedException {
      try {
        return this.lock.acquire(timeout, unit);
      } catch (InterruptedException e) {
        throw e;
        // SUPPRESS CHECKSTYLE IllegalCatch
      } catch (Exception e) {
        throw new RuntimeException("Error while trying to acquire distributed lock at " + path, e);
      }
    }

    @Override
    public void unlock() {
      try {
        this.lock.release();
        // SUPPRESS CHECKSTYLE IllegalCatch
      } catch (Exception e) {
        throw new RuntimeException("Error while trying to release distributed lock at " + path, e);
      }
    }
  }

  protected CuratorFramework getCuratorClient() {
    // these are reasonable arguments for the ExponentialBackoffRetry. The first
    // retry will wait 1 second - the second will wait up to 2 seconds - the
    // third will wait up to 4 seconds.
    ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(
        ZkContext.retryIntervalMs(config), ZkContext.retryCount(config));

    // using the CuratorFrameworkFactory.builder() gives fine grained control
    // over creation options. See the CuratorFrameworkFactory.Builder javadoc
    // details
    return CuratorFrameworkFactory.builder()
        .connectString(connectionString)
        .retryPolicy(retryPolicy)
        .connectionTimeoutMs(ZkContext.connectionTimeoutMs(config))
        .sessionTimeoutMs(ZkContext.sessionTimeoutMs(config))
            // etc. etc.
        .build();
  }

  protected Pair<String, List<Process>> setupZkTunnel(NetworkUtils.TunnelConfig tunnelConfig) {
    return ZkUtils.setupZkTunnel(config, tunnelConfig);
  }

  protected void initTree() {
    // Make necessary directories
    for (StateLocation location : StateLocation.values()) {
      LOG.fine(String.format("%s directory: %s", location.getName(), getStateDirectory(location)));
    }

    try {
      for (StateLocation location : StateLocation.values()) {
        client.createContainers(getStateDirectory(location));
      }

      // Suppress it since createContainers() throws Exception
      // SUPPRESS CHECKSTYLE IllegalCatch
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize tree", e);
    }

    LOG.info("Directory tree initialized.");
  }

  @Override
  public void close() {
    if (client != null) {
      LOG.info("Closing the CuratorClient to: " + connectionString);
      client.close();
    }

    // Close the tunneling
    LOG.info("Closing the tunnel processes");
    if (tunnelProcesses != null) {
      for (Process process : tunnelProcesses) {
        process.destroy();
      }
    }
  }

  public String getConnectionString() {
    return connectionString;
  }

  // Make utils class protected for easy unit testing
  @Override
  protected ListenableFuture<Boolean> nodeExists(String path) {
    final SettableFuture<Boolean> result = SettableFuture.create();

    try {
      LOG.info("Checking existence of path: " + path);
      safeSetFuture(result, client.checkExists().forPath(path) != null);

      // Suppress it since forPath() throws Exception
      // SUPPRESS CHECKSTYLE IllegalCatch
    } catch (Exception e) {
      safeSetException(result, new RuntimeException("Could not check Exist", e));
    }

    return result;
  }

  protected ListenableFuture<Boolean> createNode(
      StateLocation location, String topologyName,
      byte[] data,
      boolean isEphemeral) {
    return createNode(getStatePath(location, topologyName), data, isEphemeral);
  }

  @VisibleForTesting
  protected ListenableFuture<Boolean> createNode(
      String path,
      byte[] data,
      boolean isEphemeral) {
    final SettableFuture<Boolean> result = SettableFuture.create();

    try {
      client.create().
          withMode(isEphemeral ? CreateMode.EPHEMERAL : CreateMode.PERSISTENT)
          .forPath(path, data);
      LOG.info("Created node for path: " + path);
      safeSetFuture(result, true);

      // Suppress it since forPath() throws Exception
      // SUPPRESS CHECKSTYLE IllegalCatch
    } catch (Exception e) {
      safeSetException(result, new RuntimeException("Could not createNode:", e));
    }
    return result;
  }

  @Override
  protected ListenableFuture<Boolean> deleteNode(String path, boolean deleteChildrenIfNecessary) {
    final SettableFuture<Boolean> result = SettableFuture.create();

    try {
      DeleteBuilder deleteBuilder = client.delete();
      if (deleteChildrenIfNecessary) {
        deleteBuilder = (DeleteBuilder) deleteBuilder.deletingChildrenIfNeeded();
      }
      deleteBuilder.withVersion(-1).forPath(path);
      LOG.info("Deleted node for path: " + path);
      safeSetFuture(result, true);

    } catch (KeeperException e) {
      if (KeeperException.Code.NONODE.equals(e.code())) {
        safeSetFuture(result, true);
      } else {
        safeSetException(result, new RuntimeException("Could not deleteNode", e));
      }

      // Suppress it since forPath() throws Exception
      // SUPPRESS CHECKSTYLE IllegalCatch
    } catch (Exception e) {
      safeSetException(result, new RuntimeException("Could not deleteNode", e));
    }

    return result;
  }

  @Override
  protected <M extends Message> ListenableFuture<M> getNodeData(
      WatchCallback watcher,
      String path,
      final Message.Builder builder) {
    final SettableFuture<M> future = SettableFuture.create();

    Watcher wc = ZkWatcherCallback.makeZkWatcher(watcher);

    BackgroundCallback cb = new BackgroundCallback() {
      @Override
      @SuppressWarnings("unchecked") // we don't know what M is until runtime
      public void processResult(CuratorFramework aClient, CuratorEvent event) throws Exception {
        byte[] data;
        if (event != null & (data = event.getData()) != null) {
          builder.mergeFrom(data);
          safeSetFuture(future, (M) builder.build());
        } else {
          safeSetException(future, new RuntimeException("Failed to fetch data from path: "
              + event.getPath()));
        }
      }
    };

    try {
      client.getData().usingWatcher(wc).inBackground(cb).forPath(path);

      // Suppress it since forPath() throws Exception
      // SUPPRESS CHECKSTYLE IllegalCatch
    } catch (Exception e) {
      safeSetException(future, new RuntimeException(
          "Could not getNodeData using watcher for path: " + path, e));
    }

    return future;
  }

  @Override
  protected Lock getLock(String path) {
    return new DistributedLock(this.client, path);
  }

  @Override
  public ListenableFuture<Boolean> setTManagerLocation(
      TopologyManager.TManagerLocation location,
      String topologyName) {
    return createNode(StateLocation.TMANAGER_LOCATION, topologyName, location.toByteArray(), true);
  }

  @Override
  public ListenableFuture<Boolean> setMetricsCacheLocation(
      TopologyManager.MetricsCacheLocation location,
      String topologyName) {
    client.getConnectionStateListenable().addListener(new ConnectionStateListener() {
      @Override
      public void stateChanged(CuratorFramework arg0, ConnectionState arg1) {
        if (arg1 != ConnectionState.CONNECTED) {
          // if not the first time successful connection, fail fast
          throw new RuntimeException("Unexpected state change to: " + arg1.name());
        }
      }
    });
    return createNode(
        StateLocation.METRICSCACHE_LOCATION, topologyName, location.toByteArray(), true);
  }

  @Override
  public ListenableFuture<Boolean> setExecutionState(
      ExecutionEnvironment.ExecutionState executionState,
      String topologyName) {
    return createNode(
        StateLocation.EXECUTION_STATE, topologyName, executionState.toByteArray(), false);
  }

  @Override
  public ListenableFuture<Boolean> setTopology(
      TopologyAPI.Topology topology,
      String topologyName) {
    return createNode(StateLocation.TOPOLOGY, topologyName, topology.toByteArray(), false);
  }

  @Override
  public ListenableFuture<Boolean> setPhysicalPlan(
      PhysicalPlans.PhysicalPlan physicalPlan,
      String topologyName) {
    return createNode(StateLocation.PHYSICAL_PLAN, topologyName, physicalPlan.toByteArray(), false);
  }

  @Override
  public ListenableFuture<Boolean> setPackingPlan(
      PackingPlans.PackingPlan packingPlan,
      String topologyName) {
    return createNode(StateLocation.PACKING_PLAN, topologyName, packingPlan.toByteArray(), false);
  }

  @Override
  public ListenableFuture<Boolean> setStatefulCheckpoints(
      CheckpointManager.StatefulConsistentCheckpoints checkpoint,
      String topologyName) {
    return createNode(StateLocation.STATEFUL_CHECKPOINT, topologyName,
        checkpoint.toByteArray(), false);
  }

  @Override
  public ListenableFuture<Boolean> setSchedulerLocation(
      Scheduler.SchedulerLocation location,
      String topologyName) {
    // if isService, set the node as ephemeral node; set as persistent node otherwise
    return createNode(StateLocation.SCHEDULER_LOCATION, topologyName,
        location.toByteArray(),
        isSchedulerService);
  }

  @Override
  public ListenableFuture<Boolean> deleteTManagerLocation(String topologyName) {
    // It is a EPHEMERAL node and would be removed automatically
    final SettableFuture<Boolean> result = SettableFuture.create();
    safeSetFuture(result, true);
    return result;
  }

  @Override
  public ListenableFuture<Boolean> deleteMetricsCacheLocation(String topologyName) {
    // It is a EPHEMERAL node and would be removed automatically
    final SettableFuture<Boolean> result = SettableFuture.create();
    safeSetFuture(result, true);
    return result;
  }

  @Override
  public ListenableFuture<Boolean> deleteSchedulerLocation(String topologyName) {
    // if scheduler is service, the znode is ephemeral and it's deleted automatically
    if (isSchedulerService) {
      final SettableFuture<Boolean> result = SettableFuture.create();
      safeSetFuture(result, true);
      return result;
    } else {
      return deleteNode(getStatePath(StateLocation.SCHEDULER_LOCATION, topologyName), false);
    }
  }

  public static void main(String[] args) throws ExecutionException, InterruptedException,
      IllegalAccessException, ClassNotFoundException, InstantiationException {
    if (args.length < 2) {
      throw new RuntimeException("Expects 2 arguments: <topology_name> <zookeeper_hostname>");
    }
    Slf4jUtils.installSLF4JBridge();
    String zookeeperHostname = args[1];
    Config config = Config.newBuilder()
        .put(Key.STATEMGR_ROOT_PATH, "/storm/heron/states")
        .put(Key.STATEMGR_CONNECTION_STRING, zookeeperHostname)
        .put(Key.SCHEDULER_IS_SERVICE, false)
        .build();
    CuratorStateManager stateManager = new CuratorStateManager();
    stateManager.doMain(args, config);
  }
}
