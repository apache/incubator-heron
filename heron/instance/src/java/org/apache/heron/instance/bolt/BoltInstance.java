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

package org.apache.heron.instance.bolt;

import java.io.Serializable;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import org.apache.heron.api.Config;
import org.apache.heron.api.bolt.IBolt;
import org.apache.heron.api.bolt.OutputCollector;
import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.api.metric.GlobalMetrics;
import org.apache.heron.api.serializer.IPluggableSerializer;
import org.apache.heron.api.state.State;
import org.apache.heron.api.topology.IStatefulComponent;
import org.apache.heron.api.topology.ITwoPhaseStatefulComponent;
import org.apache.heron.api.topology.IUpdatable;
import org.apache.heron.api.utils.Utils;
import org.apache.heron.common.basics.Communicator;
import org.apache.heron.common.basics.ExecutorLooper;
import org.apache.heron.common.basics.FileUtils;
import org.apache.heron.common.basics.SingletonRegistry;
import org.apache.heron.common.basics.TypeUtils;
import org.apache.heron.common.config.SystemConfig;
import org.apache.heron.common.utils.metrics.FullBoltMetrics;
import org.apache.heron.common.utils.metrics.IBoltMetrics;
import org.apache.heron.common.utils.misc.PhysicalPlanHelper;
import org.apache.heron.common.utils.misc.SerializeDeSerializeHelper;
import org.apache.heron.common.utils.topology.TopologyContextImpl;
import org.apache.heron.common.utils.tuple.TickTuple;
import org.apache.heron.common.utils.tuple.TupleImpl;
import org.apache.heron.instance.IInstance;
import org.apache.heron.instance.util.InstanceUtils;
import org.apache.heron.proto.ckptmgr.CheckpointManager;
import org.apache.heron.proto.system.HeronTuples;

public class BoltInstance implements IInstance {
  private static final Logger LOG = Logger.getLogger(BoltInstance.class.getName());

  protected PhysicalPlanHelper helper;
  protected final IBolt bolt;
  protected final BoltOutputCollectorImpl collector;
  protected final IPluggableSerializer serializer;
  protected final IBoltMetrics boltMetrics;
  // The bolt will read Data tuples from streamInQueue
  private final Communicator<Message> streamInQueue;

  private final boolean isTopologyStateful;
  private final boolean spillState;
  private final String spillStateLocation;

  private State<Serializable, Serializable> instanceState;

  // default to false, can only be toggled to true if bolt implements ITwoPhaseStatefulComponent
  private boolean waitingForCheckpointSaved;

  // The reference to topology's config
  private final Map<String, Object> config;

  private final ExecutorLooper looper;

  private final SystemConfig systemConfig;

  /**
   * Construct a BoltInstance basing on given arguments
   */
  public BoltInstance(PhysicalPlanHelper helper,
                      Communicator<Message> streamInQueue,
                      Communicator<Message> streamOutQueue,
                      ExecutorLooper looper) {
    this.helper = helper;
    this.looper = looper;
    this.streamInQueue = streamInQueue;
    this.boltMetrics = new FullBoltMetrics();
    this.boltMetrics.initMultiCountMetrics(helper);
    this.serializer =
        SerializeDeSerializeHelper.getSerializer(helper.getTopologyContext().getTopologyConfig());
    this.systemConfig = (SystemConfig) SingletonRegistry.INSTANCE.getSingleton(
        SystemConfig.HERON_SYSTEM_CONFIG);

    this.config = helper.getTopologyContext().getTopologyConfig();
    this.isTopologyStateful = String.valueOf(Config.TopologyReliabilityMode.EFFECTIVELY_ONCE)
        .equals(config.get(Config.TOPOLOGY_RELIABILITY_MODE));
    LOG.info("Is this topology stateful: " + isTopologyStateful);

    this.spillState =
        Boolean.parseBoolean((String) config.get(Config.TOPOLOGY_STATEFUL_SPILL_STATE));

    this.spillStateLocation = String.format("%s/%s/",
            String.valueOf(config.get(Config.TOPOLOGY_STATEFUL_SPILL_STATE_LOCATION)),
            helper.getMyInstanceId());

    this.waitingForCheckpointSaved = false;

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
    // need to synchronize with other OutgoingTupleCollection operations
    // so that topology emit, ack, fail are thread safe
    collector.lock.lock();
    try {
      if (bolt instanceof IStatefulComponent) {
        ((IStatefulComponent) bolt).preSave(checkpointId);
      }
      if (bolt instanceof ITwoPhaseStatefulComponent) {
        waitingForCheckpointSaved = true;
      }
      collector.sendOutState(instanceState, checkpointId, spillState, spillStateLocation);
    } finally {
      collector.lock.unlock();
    }
    LOG.info("State persisted for checkpoint: " + checkpointId);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void init(State<Serializable, Serializable> state) {
    TopologyContextImpl topologyContext = helper.getTopologyContext();

    // Initialize the GlobalMetrics
    GlobalMetrics.init(topologyContext, systemConfig.getHeronMetricsExportInterval());

    boltMetrics.registerMetrics(topologyContext);

    // Initialize the instanceState if the topology is stateful and bolt is a stateful component
    if (isTopologyStateful && bolt instanceof IStatefulComponent) {
      this.instanceState = state;
      ((IStatefulComponent<Serializable, Serializable>) bolt).initState(instanceState);

      if (spillState) {
        if (FileUtils.isDirectoryExists(spillStateLocation)) {
          FileUtils.cleanDir(spillStateLocation);
        } else {
          FileUtils.createDirectory(spillStateLocation);
        }
      }
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
  public void preRestore(String checkpointId) {
    if (bolt instanceof ITwoPhaseStatefulComponent) {
      ((ITwoPhaseStatefulComponent) bolt).preRestore(checkpointId);
    }
  }

  @Override
  public void onCheckpointSaved(String checkpointId) {
    if (bolt instanceof ITwoPhaseStatefulComponent) {
      ((ITwoPhaseStatefulComponent) bolt).postSave(checkpointId);
      waitingForCheckpointSaved = false;
    }
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
        boltMetrics.updateTaskRunCount();

        // Back-pressure -- only when we could send out tuples will we read & execute tuples
        if (collector.isOutQueuesAvailable()) {
          boltMetrics.updateExecutionCount();
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
          boltMetrics.updateContinueWorkCount();
          looper.wakeUp();
        }
      }
    };
    looper.addTasksOnWakeup(boltTasks);

    PrepareTickTupleTimer();
    InstanceUtils.prepareTimerEvents(looper, helper);
  }

  @Override
  public void readTuplesAndExecute(Communicator<Message> inQueue) {
    TopologyContextImpl topologyContext = helper.getTopologyContext();
    Duration instanceExecuteBatchTime = systemConfig.getInstanceExecuteBatchTime();

    long startOfCycle = System.nanoTime();
    // Read data from in Queues
    while (!inQueue.isEmpty() && !waitingForCheckpointSaved) {
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
          long startTime = System.nanoTime();
          // Create the value list and fill the value
          List<Object> values = new ArrayList<>(nValues);
          for (int i = 0; i < nValues; i++) {
            values.add(serializer.deserialize(dataTuple.getValues(i).toByteArray()));
          }

          // Decode the tuple
          TupleImpl t = new TupleImpl(topologyContext, stream, dataTuple.getKey(),
              dataTuple.getRootsList(), values, System.nanoTime(), false, sourceTaskId);

          long deserializedTime = System.nanoTime();

          // Delegate to the use defined bolt
          bolt.execute(t);

          // record the latency of execution
          long executeLatency = Duration.ofNanos(System.nanoTime()).
              minusNanos(deserializedTime).toNanos();

          // Invoke user-defined execute task hook
          topologyContext.invokeHookBoltExecute(t, Duration.ofNanos(executeLatency));

          // Update metrics
          boltMetrics.deserializeDataTuple(stream.getId(), stream.getComponentName(),
              deserializedTime - startTime);
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

      Runnable r = () -> SendTickTuple();

      looper.registerPeriodicEvent(freq, r);
    }
  }

  private void SendTickTuple() {
    TickTuple t = new TickTuple();
    long startTime = System.nanoTime();
    bolt.execute(t);
    long latency = System.nanoTime() - startTime;
    boltMetrics.executeTuple(t.getSourceStreamId(), t.getSourceComponent(), latency);

    collector.sendOutTuples();
  }
}
