package com.twitter.heron.instance.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.ByteString;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.bolt.IBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.api.metric.GlobalMetrics;
import com.twitter.heron.api.serializer.IPluggableSerializer;
import com.twitter.heron.api.utils.Utils;
import com.twitter.heron.common.config.SystemConfig;
import com.twitter.heron.common.basics.Communicator;
import com.twitter.heron.common.basics.Constants;
import com.twitter.heron.common.basics.SingletonRegistry;
import com.twitter.heron.common.basics.SlaveLooper;
import com.twitter.heron.common.utils.metrics.BoltMetrics;
import com.twitter.heron.common.utils.misc.PhysicalPlanHelper;
import com.twitter.heron.common.utils.misc.SerializeDeSerializeHelper;
import com.twitter.heron.common.utils.topology.TopologyContextImpl;
import com.twitter.heron.common.utils.tuple.TickTuple;
import com.twitter.heron.common.utils.tuple.TupleImpl;
import com.twitter.heron.instance.IInstance;
import com.twitter.heron.proto.system.HeronTuples;

public class BoltInstance implements IInstance {
  private static final Logger LOG = Logger.getLogger(BoltInstance.class.getName());

  private final PhysicalPlanHelper helper;
  private final IBolt bolt;
  private final BoltOutputCollectorImpl collector;
  private final IPluggableSerializer serializer;
  private final BoltMetrics boltMetrics;
  // The bolt will read Data tuples from streamInQueue
  private final Communicator<HeronTuples.HeronTupleSet> streamInQueue;

  private final SlaveLooper looper;

  private final SystemConfig systemConfig;

  public BoltInstance(PhysicalPlanHelper helper,
                      Communicator<HeronTuples.HeronTupleSet> streamInQueue,
                      Communicator<HeronTuples.HeronTupleSet> streamOutQueue,
                      SlaveLooper looper) {
    this.helper = helper;
    this.looper = looper;
    this.streamInQueue = streamInQueue;

    this.boltMetrics = new BoltMetrics();
    this.boltMetrics.initMultiCountMetrics(helper);

    if (helper.getMyBolt() == null) {
      throw new RuntimeException("HeronBoltInstance has no bolt in physical plan.");
    }
    TopologyContextImpl topologyContext = helper.getTopologyContext();
    serializer = SerializeDeSerializeHelper.getSerializer(topologyContext.getTopologyConfig());
    systemConfig = (SystemConfig) SingletonRegistry.INSTANCE.getSingleton(
        SystemConfig.HERON_SYSTEM_CONFIG);

    // Get the bolt. Notice, in fact, we will always use the deserialization way to get bolt.
    if (helper.getMyBolt().getComp().hasJavaObject()) {
      bolt = (IBolt) Utils.deserialize(helper.getMyBolt().getComp().getJavaObject().toByteArray());
    } else if (helper.getMyBolt().getComp().hasJavaClassName()) {
      try {
        String boltClassName = helper.getMyBolt().getComp().getJavaClassName();
        bolt = (IBolt) Class.forName(boltClassName).newInstance();
      } catch (ClassNotFoundException ex) {
        throw new RuntimeException(ex + " Bolt class must be in class path.");
      } catch (InstantiationException ex) {
        throw new RuntimeException(ex + " Bolt class must be concrete.");
      } catch (IllegalAccessException ex) {
        throw new RuntimeException(ex + " Bolt class must have a no-arg constructor.");
      }
    } else {
      throw new RuntimeException("Neither java_object nor java_class_name set for bolt");
    }
    collector = new BoltOutputCollectorImpl(serializer, helper, streamOutQueue, boltMetrics);
  }

  @Override
  public void start() {
    TopologyContextImpl topologyContext = helper.getTopologyContext();
    try {
      GlobalMetrics.init(topologyContext, systemConfig.getHeronMetricsExportIntervalSec());
    } catch (RuntimeException e) {
      LOG.log(Level.SEVERE, "Failed to initialize GlobalMetrics. Global Metrics will be lost."
          + "Check the version of heron-api used.", e);
    }
    boltMetrics.registerMetrics(topologyContext);

    // Delegate
    bolt.prepare(topologyContext.getTopologyConfig(), topologyContext, new OutputCollector(collector));

    // Invoke user-defined prepare task hook
    topologyContext.invokeHookPrepare();

    // Init the CustomStreamGrouping
    helper.prepareForCustomStreamGrouping(topologyContext);

    addBoltTasks();
  }

  @Override
  public void stop() {
    // Invoke clean up hook before clean() is called
    helper.getTopologyContext().invokeHookCleanup();

    // Delegate to user-defined clean-up method
    bolt.cleanup();

    // Clean the resources we own
    looper.exitLoop();
    streamInQueue.clear();
    collector.clear();
  }

  private void addBoltTasks() {
    // add the readTupleAndExecute() to tasks
    Runnable boltTasks = new Runnable() {
      @Override
      public void run() {
        // Back-pressure -- only when we could send out tuples will we read & execute tuples
        if (collector.isOutQueuesAvailable()) {
          readTuplesAndExecute(streamInQueue);

          // Though we may execute MAX_READ tuples, finally we will packet it as
          // one outgoingPacket and push to out queues
          collector.sendOutTuples();
          // Here we need to inform the Gateway
        } else {
          boltMetrics.updateOutQueueFullCount();
        }

        // If there are more to read, we will wake up itself next time when it doWait()
        if (collector.isOutQueuesAvailable() && !streamInQueue.isEmpty()) {
          looper.wakeUp();
        }
      }
    };
    looper.addTasksOnWakeup(boltTasks);

    PrepareTickTupleTimer();
  }

  private void handleDataTuple(HeronTuples.HeronDataTuple dataTuple,
                               TopologyContextImpl topologyContext,
                               TopologyAPI.StreamId stream) {
    long startTime = System.nanoTime();

    List<Object> values = new ArrayList<Object>();
    for (ByteString b : dataTuple.getValuesList()) {
      values.add(serializer.deserialize(b.toByteArray()));
    }

    // Decode the tuple
    TupleImpl t = new TupleImpl(topologyContext, stream, dataTuple.getKey(),
        dataTuple.getRootsList(), values);

    long deserializedTime = System.nanoTime();

    // Delegate to the use defined bolt
    bolt.execute(t);

    long executeLatency = System.nanoTime() - deserializedTime;

    // Invoke user-defined execute task hook
    topologyContext.invokeHookBoltExecute(t, executeLatency);

    boltMetrics.deserializeDataTuple(stream.getId(), stream.getComponentName(),
        deserializedTime - startTime);

    // Update metrics
    boltMetrics.executeTuple(stream.getId(), stream.getComponentName(), executeLatency);
  }

  @Override
  public void readTuplesAndExecute(Communicator<HeronTuples.HeronTupleSet> inQueue) {
    long startOfCycle = System.nanoTime();

    long totalDataEmittedInBytesBeforeCycle = collector.getTotalDataEmittedInBytes();

    long instanceExecuteBatchTime
        = systemConfig.getInstanceExecuteBatchTimeMs() * Constants.MILLISECONDS_TO_NANOSECONDS;

    long instanceExecuteBatchSize = systemConfig.getInstanceExecuteBatchSizeBytes();

    // Read data from in Queues
    while (!inQueue.isEmpty()) {
      HeronTuples.HeronTupleSet tuples = inQueue.poll();

      TopologyContextImpl topologyContext = helper.getTopologyContext();
      // Handle the tuples
      if (tuples.hasControl()) {
        throw new RuntimeException("Bolt cannot get acks/fails from other components");
      }
      TopologyAPI.StreamId stream = tuples.getData().getStream();

      for (HeronTuples.HeronDataTuple dataTuple : tuples.getData().getTuplesList()) {
        handleDataTuple(dataTuple, topologyContext, stream);
      }

      // To avoid spending too much time
      if (System.nanoTime() - startOfCycle - instanceExecuteBatchTime > 0) {
        break;
      }

      // To avoid emitting too much data
      if (collector.getTotalDataEmittedInBytes() - totalDataEmittedInBytesBeforeCycle >
          instanceExecuteBatchSize) {
        break;
      }
    }
  }

  @Override
  public void activate() {

  }

  @Override
  public void deactivate() {

  }

  private void PrepareTickTupleTimer() {
    Object tickTupleFreqSecs =
        helper.getTopologyContext().getTopologyConfig().get(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS);

    if (tickTupleFreqSecs != null) {
      int freq = Utils.getInt(tickTupleFreqSecs);

      Runnable r = new Runnable() {
        public void run() {
          SendTickTuple();
        }
      };

      looper.registerTimerEventInSeconds(freq, r);
    }
  }

  private void SendTickTuple() {
    TickTuple t = new TickTuple();
    long startTime = System.nanoTime();
    bolt.execute(t);
    long latency = System.nanoTime() - startTime;
    boltMetrics.executeTuple(t.getSourceStreamId(), t.getSourceComponent(), latency);

    collector.sendOutTuples();
    // reschedule ourselves again
    PrepareTickTupleTimer();
  }
}
