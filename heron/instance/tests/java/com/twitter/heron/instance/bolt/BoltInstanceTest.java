package com.twitter.heron.instance.bolt;


import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.protobuf.ByteString;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.api.serializer.IPluggableSerializer;
import com.twitter.heron.api.serializer.KryoSerializer;
import com.twitter.heron.api.utils.Utils;
import com.twitter.heron.common.core.base.Communicator;
import com.twitter.heron.common.core.base.SingletonRegistry;
import com.twitter.heron.common.core.base.SlaveLooper;
import com.twitter.heron.common.core.base.WakeableLooper;
import com.twitter.heron.common.utils.misc.PhysicalPlanHelper;
import com.twitter.heron.instance.InstanceControlMsg;
import com.twitter.heron.instance.Slave;
import com.twitter.heron.proto.system.HeronTuples;
import com.twitter.heron.proto.system.Metrics;
import com.twitter.heron.proto.system.PhysicalPlans;
import com.twitter.heron.resource.Constants;
import com.twitter.heron.resource.UnitTestHelper;

/**
 * To test the Bolt's ReadTupleAndExecute() method, it will:
 * 1. We will instantiate a slave with TestBolt's instance.
 * 2. Construct a bunch of mock Tuples as protobuf Message to be consumed by the TestBolt
 * 3. Offer those protobuf Message into inStreamQueue.
 * 4. The TestBolt should consume the Tuples, and behave as described in its comments.
 * 5. Check the values in singleton registry: execute-count, ack-count, fail-count, received-string-list.
 * 6. We will also check some common values, for instance, the stream name.
 */
public class BoltInstanceTest {
  static final String boltInstanceId = "bolt-id";


  WakeableLooper testLooper;
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
  AtomicInteger ackCount;
  AtomicInteger failCount;
  AtomicInteger tupleExecutedCount;
  volatile StringBuilder receivedStrings;
  PhysicalPlans.PhysicalPlan physicalPlan;

  static IPluggableSerializer serializer;

  @BeforeClass
  public static void beforeClass() throws Exception {
    serializer = new KryoSerializer();
    serializer.initialize(null);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    serializer = null;
  }

  @Before
  public void before() throws Exception {
    UnitTestHelper.addSystemConfigToSingleton();

    ackCount = new AtomicInteger(0);
    failCount = new AtomicInteger(0);
    tupleExecutedCount = new AtomicInteger(0);
    receivedStrings = new StringBuilder();

    testLooper = new SlaveLooper();
    slaveLooper = new SlaveLooper();
    outStreamQueue = new Communicator<HeronTuples.HeronTupleSet>(slaveLooper, testLooper);
    outStreamQueue.init(Constants.QUEUE_BUFFER_SIZE, Constants.QUEUE_BUFFER_SIZE, 0.5);
    inStreamQueue = new Communicator<HeronTuples.HeronTupleSet>(testLooper, slaveLooper);
    inStreamQueue.init(Constants.QUEUE_BUFFER_SIZE, Constants.QUEUE_BUFFER_SIZE, 0.5);
    inControlQueue = new Communicator<InstanceControlMsg>(testLooper, slaveLooper);

    slaveMetricsOut = new Communicator<Metrics.MetricPublisherPublishMessage>(slaveLooper, testLooper);
    slaveMetricsOut.init(Constants.QUEUE_BUFFER_SIZE, Constants.QUEUE_BUFFER_SIZE, 0.5);

    slave = new Slave(slaveLooper, inStreamQueue, outStreamQueue, inControlQueue, slaveMetricsOut);
    threadsPool = Executors.newSingleThreadExecutor();

    threadsPool.execute(slave);
  }

  @After
  public void after() throws Exception {
    UnitTestHelper.clearSingletonRegistry();

    ackCount = new AtomicInteger(0);
    failCount = new AtomicInteger(0);
    tupleExecutedCount = new AtomicInteger(0);
    receivedStrings = null;

    testLooper.exitLoop();
    slaveLooper.exitLoop();
    threadsPool.shutdownNow();

    physicalPlan = null;
    testLooper = null;
    slaveLooper = null;
    outStreamQueue = null;
    inStreamQueue = null;

    slave = null;
    threadsPool = null;
  }

  @Test
  public void testReadTupleAndExecute() throws Exception {
    physicalPlan = UnitTestHelper.getPhysicalPlan(false, -1);

    PhysicalPlanHelper physicalPlanHelper = new PhysicalPlanHelper(physicalPlan, boltInstanceId);
    InstanceControlMsg instanceControlMsg = InstanceControlMsg.newBuilder().
        setNewPhysicalPlanHelper(physicalPlanHelper).
        build();

    inControlQueue.offer(instanceControlMsg);

    SingletonRegistry.INSTANCE.registerSingleton(Constants.ACK_COUNT, ackCount);
    SingletonRegistry.INSTANCE.registerSingleton(Constants.FAIL_COUNT, failCount);
    SingletonRegistry.INSTANCE.registerSingleton("execute-count", tupleExecutedCount);
    SingletonRegistry.INSTANCE.registerSingleton("received-string-list", receivedStrings);

    // Send tuples to bolt instance
    HeronTuples.HeronTupleSet.Builder heronTupleSet = HeronTuples.HeronTupleSet.newBuilder();
    HeronTuples.HeronDataTupleSet.Builder dataTupleSet = HeronTuples.HeronDataTupleSet.newBuilder();
    TopologyAPI.StreamId.Builder streamId = TopologyAPI.StreamId.newBuilder();
    streamId.setComponentName("test-spout");
    streamId.setId("default");
    dataTupleSet.setStream(streamId);

    // We will add 10 tuples to the set
    for (int i = 0; i < 10; i++) {
      HeronTuples.HeronDataTuple.Builder dataTuple = HeronTuples.HeronDataTuple.newBuilder();
      dataTuple.setKey(19901017 + i);

      HeronTuples.RootId.Builder rootId = HeronTuples.RootId.newBuilder();
      rootId.setKey(19901017 + i);
      rootId.setTaskid(0);
      dataTuple.addRoots(rootId);

      String s = "";
      if ((i & 1) == 0) {
        s = "A";
      } else {
        s = "B";
      }
      ByteString byteString = ByteString.copyFrom(serializer.serialize(s));
      dataTuple.addValues(byteString);

      dataTupleSet.addTuples(dataTuple);
    }

    heronTupleSet.setData(dataTupleSet);
    inStreamQueue.offer(heronTupleSet.build());

    for (int i = 0; i < Constants.RETRY_TIMES; i++) {
      if (tupleExecutedCount.intValue() == 10) {
        break;
      }
      Utils.sleep(Constants.RETRY_INTERVAL_MS);
    }

    // Wait the bolt's finishing
    Utils.sleep(Constants.TEST_WAIT_TIME_MS);
    Assert.assertEquals(10, tupleExecutedCount.intValue());
    Assert.assertEquals(5, ackCount.intValue());
    Assert.assertEquals(5, failCount.intValue());
    Assert.assertEquals("ABABABABAB", receivedStrings.toString());
  }
}
