package com.twitter.heron.instance.spout;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.config.SystemConfig;
import com.twitter.heron.common.basics.Communicator;
import com.twitter.heron.common.basics.SingletonRegistry;
import com.twitter.heron.common.basics.SlaveLooper;
import com.twitter.heron.common.utils.misc.PhysicalPlanHelper;
import com.twitter.heron.instance.InstanceControlMsg;
import com.twitter.heron.instance.Slave;
import com.twitter.heron.proto.system.HeronTuples;
import com.twitter.heron.proto.system.Metrics;
import com.twitter.heron.proto.system.PhysicalPlans;
import com.twitter.heron.resource.Constants;
import com.twitter.heron.resource.UnitTestHelper;

public class ActivateDeactivateTest {
  static final String spoutInstanceId = "spout-id";
  SlaveLooper slaveLooper;

  // Only one outStreamQueue, which is responsible for both control tuples and data tuples
  private Communicator<HeronTuples.HeronTupleSet> outStreamQueue;

  // This blocking queue is used to buffer tuples read from socket and ready to be used by instance
  // For spout, it will buffer Control tuple, while for bolt, it will buffer data tuple.
  private Communicator<HeronTuples.HeronTupleSet> inStreamQueue;

  private Communicator<InstanceControlMsg> inControlQueue;

  private ExecutorService threadsPool;

  private Communicator<Metrics.MetricPublisherPublishMessage> slaveMetricsOut;

  private Slave slave;

  // Singleton to be changed globally for testing
  PhysicalPlans.PhysicalPlan physicalPlan;

  @Before
  public void before() throws Exception {
    UnitTestHelper.addSystemConfigToSingleton();

    slaveLooper = new SlaveLooper();
    outStreamQueue = new Communicator<HeronTuples.HeronTupleSet>(slaveLooper, null);
    outStreamQueue.init(Constants.QUEUE_BUFFER_SIZE, Constants.QUEUE_BUFFER_SIZE, 0.5);
    inStreamQueue = new Communicator<HeronTuples.HeronTupleSet>(null, slaveLooper);
    inStreamQueue.init(Constants.QUEUE_BUFFER_SIZE, Constants.QUEUE_BUFFER_SIZE, 0.5);
    slaveMetricsOut = new Communicator<Metrics.MetricPublisherPublishMessage>(slaveLooper, null);
    slaveMetricsOut.init(Constants.QUEUE_BUFFER_SIZE, Constants.QUEUE_BUFFER_SIZE, 0.5);
    inControlQueue = new Communicator<InstanceControlMsg>(null, slaveLooper);

    slave = new Slave(slaveLooper, inStreamQueue, outStreamQueue, inControlQueue, slaveMetricsOut);
    threadsPool = Executors.newSingleThreadExecutor();

    threadsPool.execute(slave);
  }

  @After
  public void after() throws Exception {
    UnitTestHelper.clearSingletonRegistry();
    slaveLooper.exitLoop();

    threadsPool.shutdownNow();

    physicalPlan = null;
    slaveLooper = null;
    outStreamQueue = null;
    inStreamQueue = null;

    slave = null;
    threadsPool = null;
  }


  /**
   * We will test whether spout would pull activate/deactivate state change and
   * invoke activate()/deactivate()
   */

  @Test
  public void testActivateAndDeactivate() throws Exception {
    physicalPlan = UnitTestHelper.getPhysicalPlan(true, -1, TopologyAPI.TopologyState.RUNNING);

    PhysicalPlanHelper physicalPlanHelper = new PhysicalPlanHelper(physicalPlan, spoutInstanceId);
    InstanceControlMsg instanceControlMsg = InstanceControlMsg.newBuilder().
        setNewPhysicalPlanHelper(physicalPlanHelper).
        build();

    inControlQueue.offer(instanceControlMsg);

    AtomicInteger activateCount = new AtomicInteger(0);
    AtomicInteger deactivateCount = new AtomicInteger(0);
    SingletonRegistry.INSTANCE.registerSingleton(Constants.ACTIVATE_COUNT, activateCount);
    SingletonRegistry.INSTANCE.registerSingleton(Constants.DEACTIVATE_COUNT, deactivateCount);

    // We reset the heron.instance.state.check.interval.sec in SystemConfig for faster test
    SystemConfig systemConfig =
        (SystemConfig) SingletonRegistry.INSTANCE.getSingleton(
            SystemConfig.HERON_SYSTEM_CONFIG);
    systemConfig.put("heron.instance.state.check.interval.sec", 1);

    // Now the activateCount and deactivateCount should be 0
    // And we start the test
    physicalPlan = UnitTestHelper.getPhysicalPlan(true, -1, TopologyAPI.TopologyState.PAUSED);
    physicalPlanHelper = new PhysicalPlanHelper(physicalPlan, spoutInstanceId);
    instanceControlMsg = InstanceControlMsg.newBuilder().
        setNewPhysicalPlanHelper(physicalPlanHelper).
        build();

    inControlQueue.offer(instanceControlMsg);

    Thread.sleep(Constants.TEST_WAIT_TIME_MS);

    Assert.assertEquals(1, deactivateCount.get());

    physicalPlan = UnitTestHelper.getPhysicalPlan(true, -1, TopologyAPI.TopologyState.RUNNING);
    physicalPlanHelper = new PhysicalPlanHelper(physicalPlan, spoutInstanceId);
    instanceControlMsg = InstanceControlMsg.newBuilder().
        setNewPhysicalPlanHelper(physicalPlanHelper).
        build();

    inControlQueue.offer(instanceControlMsg);

    Thread.sleep(Constants.TEST_WAIT_TIME_MS);

    Assert.assertEquals(1, activateCount.get());
    Assert.assertEquals(1, deactivateCount.get());
  }
}
