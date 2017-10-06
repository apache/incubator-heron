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

package com.twitter.heron.instance.bolt;

import java.io.Serializable;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.bolt.IBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.api.metric.GlobalMetrics;
import com.twitter.heron.api.serializer.IPluggableSerializer;
import com.twitter.heron.api.state.State;
import com.twitter.heron.api.topology.IStatefulComponent;
import com.twitter.heron.api.topology.IUpdatable;
import com.twitter.heron.api.utils.Utils;
import com.twitter.heron.common.basics.Communicator;
import com.twitter.heron.common.basics.SingletonRegistry;
import com.twitter.heron.common.basics.SlaveLooper;
import com.twitter.heron.common.basics.TypeUtils;
import com.twitter.heron.common.config.SystemConfig;
import com.twitter.heron.common.utils.metrics.BoltMetrics;
import com.twitter.heron.common.utils.metrics.FullBoltMetrics;
import com.twitter.heron.common.utils.misc.PhysicalPlanHelper;
import com.twitter.heron.common.utils.misc.SerializeDeSerializeHelper;
import com.twitter.heron.common.utils.topology.TopologyContextImpl;
import com.twitter.heron.common.utils.tuple.TickTuple;
import com.twitter.heron.common.utils.tuple.TupleImpl;
import com.twitter.heron.instance.IInstance;
import com.twitter.heron.proto.ckptmgr.CheckpointManager;
import com.twitter.heron.proto.system.HeronTuples;

public class BoltInstance implements IInstance {
  private static final Logger LOG = Logger.getLogger(BoltInstance.class.getName());

  protected PhysicalPlanHelper helper;
  protected final IBolt bolt;
  protected final BoltOutputCollectorImpl collector;
  protected final IPluggableSerializer serializer;
  protected final BoltMetrics boltMetrics;
  // The bolt will read Data tuples from streamInQueue
  private final Communicator<Message> streamInQueue;

  private final boolean isTopologyStateful;

  private State<Serializable, Serializable> instanceState;

  private final SlaveLooper looper;

  private final SystemConfig systemConfig;

  public BoltInstance(PhysicalPlanHelper helper,
                      Communicator<Message> streamInQueue,
                      Communicator<Message> streamOutQueue,
                      SlaveLooper looper) {
    this.helper = helper;
    this.looper = looper;
    this.streamInQueue = streamInQueue;
    this.boltMetrics = new FullBoltMetrics();
    this.boltMetrics.initMultiCountMetrics(helper);
    this.serializer =
        SerializeDeSerializeHelper.getSerializer(helper.getTopologyContext().getTopologyConfig());
    this.systemConfig = (SystemConfig) SingletonRegistry.INSTANCE.getSingleton(
        SystemConfig.HERON_SYSTEM_CONFIG);

    Map<String, Object> config = helper.getTopologyContext().getTopologyConfig();
    this.isTopologyStateful = String.valueOf(Config.TopologyReliabilityMode.EFFECTIVELY_ONCE)
        .equals(config.get(Config.TOPOLOGY_RELIABILITY_MODE));

    LOG.info("Is this topology stateful: " + isTopologyStateful);

    if (helper.getMyBolt() == null) {
      throw new RuntimeException("HeronBoltInstance has no bolt in physical plan.");
    }

    // Get the bolt. Notice, in fact, we will always use the deserialization way to get bolt.
    if (helper.getMyBolt().getComp().hasSerializedObject()) {
      bolt = (IBolt) Utils.deserialize(
          helper.getMyBolt().getComp().getSerializedObject().toByteArray());
    } else if (helper.getMyBolt().getComp().hasClassName()) {
      try {
        String boltClassName = helper.getMyBolt().getComp().getClassName();
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
  public void update(PhysicalPlanHelper physicalPlanHelper) {
    if (bolt instanceof IUpdatable) {
      ((IUpdatable) bolt).update(physicalPlanHelper.getTopologyContext());
    }
    collector.updatePhysicalPlanHelper(physicalPlanHelper);

    // Re-prepare the CustomStreamGrouping since the downstream tasks can change
    physicalPlanHelper.prepareForCustomStreamGrouping();
    // transfer potentially changed variables by topology code
    physicalPlanHelper.getTopologyContext().getTopologyConfig()
        .putAll(helper.getTopologyContext().getTopologyConfig());
    // Reset the helper
    helper = physicalPlanHelper;
  }

  @Override
  public void persistState(String checkpointId) {
    LOG.info("Persisting state for checkpoint: " + checkpointId);

    if (!isTopologyStateful) {
      throw new RuntimeException("Could not save a non-stateful topology's state");
    }

    // Checkpoint
    if (bolt instanceof IStatefulComponent) {
      ((IStatefulComponent) bolt).preSave(checkpointId);
    }

    collector.sendOutState(instanceState, checkpointId);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void init(State<Serializable, Serializable> state) {
    TopologyContextImpl topologyContext = helper.getTopologyContext();

    // Initialize the GlobalMetrics
    GlobalMetrics.init(topologyContext, systemConfig.getHeronMetricsExportInterval());

    boltMetrics.registerMetrics(topologyContext);

    // Initialize the instanceState if the bolt is stateful
    if (bolt instanceof IStatefulComponent) {
      this.instanceState = state;
      ((IStatefulComponent<Serializable, Serializable>) bolt).initState(instanceState);
    }

    // Delegate
    bolt.prepare(
        topologyContext.getTopologyConfig(), topologyContext, new OutputCollector(collector));

    // Invoke user-defined prepare task hook
    topologyContext.invokeHookPrepare();

    // Init the CustomStreamGrouping
    helper.prepareForCustomStreamGrouping();
  }

  @Override
  public void start() {
    addBoltTasks();
  }

  @Override
  public void clean() {
    // Invoke clean up hook before clean() is called
    helper.getTopologyContext().invokeHookCleanup();

    // Delegate to user-defined clean-up method
    bolt.cleanup();

    // Clean the resources we own
    streamInQueue.clear();
    collector.clear();
  }

  @Override
  public void shutdown() {
    clean();
    looper.exitLoop();
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

  @Override
  public void readTuplesAndExecute(Communicator<Message> inQueue) {
    TopologyContextImpl topologyContext = helper.getTopologyContext();
    Duration instanceExecuteBatchTime = systemConfig.getInstanceExecuteBatchTime();

    long startOfCycle = System.nanoTime();
    // Read data from in Queues
    while (!inQueue.isEmpty()) {
      Message msg = inQueue.poll();

      if (msg instanceof CheckpointManager.InitiateStatefulCheckpoint) {
        String checkpointId =
            ((CheckpointManager.InitiateStatefulCheckpoint) msg).getCheckpointId();
        persistState(checkpointId);
      }

      if (msg instanceof HeronTuples.HeronTupleSet) {
        HeronTuples.HeronTupleSet tuples = (HeronTuples.HeronTupleSet) msg;
        // Handle the tuples
        if (tuples.hasControl()) {
          throw new RuntimeException("Bolt cannot get acks/fails from other components");
        }

        // Get meta data of tuples
        TopologyAPI.StreamId stream = tuples.getData().getStream();
        int nValues = topologyContext.getComponentOutputFields(
            stream.getComponentName(), stream.getId()).size();
        int sourceTaskId = tuples.getSrcTaskId();

        for (HeronTuples.HeronDataTuple dataTuple : tuples.getData().getTuplesList()) {
          long startExecuteTuple = System.nanoTime();
          // Create the value list and fill the value
          List<Object> values = new ArrayList<>(nValues);
          for (int i = 0; i < nValues; i++) {
            values.add(serializer.deserialize(dataTuple.getValues(i).toByteArray()));
          }

          // Decode the tuple
          TupleImpl t = new TupleImpl(topologyContext, stream, dataTuple.getKey(),
              dataTuple.getRootsList(), values, startExecuteTuple, false, sourceTaskId);

          // Delegate to the use defined bolt
          bolt.execute(t);

          // record the end of a tuple execution
          long endExecuteTuple = System.nanoTime();

          long executeLatency = endExecuteTuple - startExecuteTuple;

          // Invoke user-defined execute task hook
          topologyContext.invokeHookBoltExecute(t, Duration.ofNanos(executeLatency));

          // Update metrics
          boltMetrics.executeTuple(stream.getId(), stream.getComponentName(), executeLatency);
        }

        // To avoid spending too much time
        long currentTime = System.nanoTime();
        if (currentTime - startOfCycle - instanceExecuteBatchTime.toNanos() > 0) {
          break;
        }
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
    Object tickTupleFreqMs =
        helper.getTopologyContext().getTopologyConfig().get(Config.TOPOLOGY_TICK_TUPLE_FREQ_MS);

    if (tickTupleFreqMs != null) {
      Duration freq = TypeUtils.getDuration(tickTupleFreqMs, ChronoUnit.MILLIS);

      Runnable r = new Runnable() {
        public void run() {
          SendTickTuple();
        }
      };

      looper.registerTimerEvent(freq, r);
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
