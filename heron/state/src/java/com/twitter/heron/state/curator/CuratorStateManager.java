package com.twitter.heron.state.curator;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

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
import com.twitter.heron.state.FileSystemStateManager;
import com.twitter.heron.state.WatchCallback;
import com.twitter.heron.state.zookeeper.ZkWatcherCallback;

public class CuratorStateManager extends FileSystemStateManager {
  private static final Logger LOG = Logger.getLogger(CuratorStateManager.class.getName());
  private CuratorFramework client;
  private String connectionString;

  public static final String ZK_CONNECTION_STRING = "zk.connection.string";
  @Override
  public void initialize(Map<Object, Object> conf) {
    super.initialize(conf);

    int sessionTimeoutMs = 30 * 1000;
    int connectionTimeoutMs = 30 * 1000;

    int retryCount = 10;
    int retryIntervalMs = 1000;

    connectionString = (String) conf.get(ZK_CONNECTION_STRING);

    // these are reasonable arguments for the ExponentialBackoffRetry. The first
    // retry will wait 1 second - the second will wait up to 2 seconds - the
    // third will wait up to 4 seconds.
    ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(retryIntervalMs, retryCount);

    // using the CuratorFrameworkFactory.builder() gives fine grained control
    // over creation options. See the CuratorFrameworkFactory.Builder javadoc
    // details
    client = CuratorFrameworkFactory.builder()
        .connectString(connectionString)
        .retryPolicy(retryPolicy)
        .connectionTimeoutMs(connectionTimeoutMs)
        .sessionTimeoutMs(sessionTimeoutMs)
            // etc. etc.
        .build();

    LOG.info("Starting client to: " + connectionString);

    // Start it
    client.start();

    try {
      if (!client.blockUntilConnected(connectionTimeoutMs, TimeUnit.MILLISECONDS)) {
        throw new RuntimeException("Failed to initialize CuratorClient");
      }
    } catch (InterruptedException e) {
      throw new RuntimeException("Failed to initialize CuratorClient", e);
    }
  }

  @Override
  public void close() {
    LOG.info("Closing the CuratorClient to: " + connectionString);
    client.close();
  }

  public String getConnectionString() {
    return connectionString;
  }

  private ListenableFuture<Boolean> existNode(String path) {
    final SettableFuture<Boolean> result = SettableFuture.create();

    try {
      LOG.info("Checking exists for path: " + path);
      result.set(client.checkExists().forPath(path) != null);
    } catch (Exception e) {
      result.setException(new RuntimeException("Could not check Exist", e));
    }

    return result;
  }

  private ListenableFuture<Boolean> createNode(String path, byte[] data, boolean isEphemeral) {
    final SettableFuture<Boolean> result = SettableFuture.create();

    try {
      client.create().creatingParentsIfNeeded().
          withMode(isEphemeral ? CreateMode.EPHEMERAL : CreateMode.PERSISTENT)
          .forPath(path, data);
      LOG.info("Created node for path: " + path);
      result.set(true);
    } catch (Exception e) {
      result.setException(new RuntimeException("Could not createNode:", e));
    }
    return result;
  }

  private ListenableFuture<Boolean> deleteNode(String path) {
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

  @Override
  public ListenableFuture<Boolean> setTMasterLocation(TopologyMaster.TMasterLocation location, String topologyName) {
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
  public ListenableFuture<Boolean> setPhysicalPlan(PhysicalPlans.PhysicalPlan physicalPlan, String topologyName) {
    return createNode(getPhysicalPlanPath(topologyName), physicalPlan.toByteArray(), false);
  }

  @Override
  public ListenableFuture<Boolean> setSchedulerLocation(Scheduler.SchedulerLocation location, String topologyName) {
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
  public ListenableFuture<TopologyMaster.TMasterLocation> getTMasterLocation(WatchCallback watcher, String topologyName) {
    final SettableFuture<TopologyMaster.TMasterLocation> tLocationFuture = SettableFuture.create();
    String path = getTMasterLocationPath(topologyName);
    Watcher wc = ZkWatcherCallback.makeZkWatcher(watcher);

    BackgroundCallback cb = new BackgroundCallback() {
      @Override
      public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
        byte[] data;
        if (event != null & (data = event.getData()) != null) {
          tLocationFuture.set(TopologyMaster.TMasterLocation.parseFrom(data));
        } else {
          tLocationFuture.setException(new RuntimeException("Failed to fetch data from path: " + event.getPath()));
        }
      }
    };

    try {
      client.getData().usingWatcher(wc).inBackground(cb).forPath(path);
    } catch (Exception e) {
      tLocationFuture.setException(new RuntimeException("Could not getData", e));
    }

    return tLocationFuture;
  }

  @Override
  public ListenableFuture<Scheduler.SchedulerLocation> getSchedulerLocation(WatchCallback watcher, String topologyName) {
    final SettableFuture<Scheduler.SchedulerLocation> schedulerLocationFuture = SettableFuture.create();
    String path = getSchedulerLocationPath(topologyName);
    Watcher wc = ZkWatcherCallback.makeZkWatcher(watcher);

    BackgroundCallback cb = new BackgroundCallback() {
      @Override
      public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
        byte[] data;
        if (event != null & (data = event.getData()) != null) {
          LOG.info("SEE SEE the data: " + new String(data));
          schedulerLocationFuture.set(Scheduler.SchedulerLocation.parseFrom(data));
        } else {
          schedulerLocationFuture.setException(new RuntimeException("Failed to fetch data from path: " + event.getPath()));
        }
      }
    };

    try {
      client.getData().usingWatcher(wc).inBackground(cb).forPath(path);
    } catch (Exception e) {
      schedulerLocationFuture.setException(new RuntimeException("Could not getData", e));
    }

    return schedulerLocationFuture;
  }

  @Override
  public ListenableFuture<TopologyAPI.Topology> getTopology(WatchCallback watcher, String topologyName) {
    final SettableFuture<TopologyAPI.Topology> topologyFuture = SettableFuture.create();
    String path = getTopologyPath(topologyName);
    Watcher wc = ZkWatcherCallback.makeZkWatcher(watcher);

    BackgroundCallback cb = new BackgroundCallback() {
      @Override
      public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
        byte[] data;
        if (event != null & (data = event.getData()) != null) {
          topologyFuture.set(TopologyAPI.Topology.parseFrom(data));
        } else {
          topologyFuture.setException(new RuntimeException("Failed to fetch data from path: " + event.getPath()));
        }
      }
    };

    try {
      client.getData().usingWatcher(wc).inBackground(cb).forPath(path);
    } catch (Exception e) {
      topologyFuture.setException(new RuntimeException("Could not getData", e));
    }

    return topologyFuture;
  }

  @Override
  public ListenableFuture<ExecutionEnvironment.ExecutionState> getExecutionState(WatchCallback watcher, String topologyName) {
    final SettableFuture<ExecutionEnvironment.ExecutionState> executionStateFuture = SettableFuture.create();
    String path = getExecutionStatePath(topologyName);
    Watcher wc = ZkWatcherCallback.makeZkWatcher(watcher);

    BackgroundCallback cb = new BackgroundCallback() {
      @Override
      public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
        byte[] data;
        if (event != null & (data = event.getData()) != null) {
          executionStateFuture.set(ExecutionEnvironment.ExecutionState.parseFrom(data));
        } else {
          executionStateFuture.setException(new RuntimeException("Failed to fetch data from path: " + event.getPath()));
        }
      }
    };

    try {
      client.getData().usingWatcher(wc).inBackground(cb).forPath(path);
    } catch (Exception e) {
      executionStateFuture.setException(new RuntimeException("Could not getData", e));
    }

    return executionStateFuture;
  }

  @Override
  public ListenableFuture<PhysicalPlans.PhysicalPlan> getPhysicalPlan(WatchCallback watcher, String topologyName) {
    final SettableFuture<PhysicalPlans.PhysicalPlan> physicalPlanFuture = SettableFuture.create();
    String path = getPhysicalPlanPath(topologyName);
    Watcher wc = ZkWatcherCallback.makeZkWatcher(watcher);

    BackgroundCallback cb = new BackgroundCallback() {
      @Override
      public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
        byte[] data;
        if (event != null & (data = event.getData()) != null) {
          physicalPlanFuture.set(PhysicalPlans.PhysicalPlan.parseFrom(data));
        } else {
          physicalPlanFuture.setException(new RuntimeException("Failed to fetch data from path: " + event.getPath()));
        }
      }
    };

    try {
      client.getData().usingWatcher(wc).inBackground(cb).forPath(path);
    } catch (Exception e) {
      physicalPlanFuture.setException(new RuntimeException("Could not getData", e));
    }

    return physicalPlanFuture;
  }

  @Override
  public ListenableFuture<Boolean> isTopologyRunning(String topologyName) {
    return existNode(getTopologyPath(topologyName));
  }
}
