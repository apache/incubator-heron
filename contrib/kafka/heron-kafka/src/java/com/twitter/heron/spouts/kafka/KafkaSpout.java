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

package com.twitter.heron.spouts.kafka;

import java.lang.reflect.Constructor;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.heron.api.metric.MultiCountMetric;
import com.twitter.heron.api.spout.BaseRichSpout;
import com.twitter.heron.api.spout.SpoutOutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.spouts.kafka.common.FilterOperator;
import com.twitter.heron.spouts.kafka.common.GlobalPartitionId;
import com.twitter.heron.spouts.kafka.common.IOExecutorService;
import com.twitter.heron.spouts.kafka.common.TransferCollector;

/**
 * KafkaSpout is a regular spout implementation that reads from a Kafka cluster.
 */
@SuppressWarnings({"serial", "unchecked", "rawtypes"})
// CHECKSTYLE:OFF IllegalCatch
public class KafkaSpout extends BaseRichSpout {

  public static final Logger LOG = LoggerFactory.getLogger(KafkaSpout.class);

  private String uuid = UUID.randomUUID().toString();
  private SpoutConfig spoutConfig;
  protected SpoutOutputCollector collector;
  protected TransferCollector transferCollector;
  protected PartitionCoordinator coordinator;
  protected MultiCountMetric spoutMetrics;
  protected IOExecutorService.SingleThreadIOExecutorService executor;

  /**
   * Ctor
   */
  public KafkaSpout(SpoutConfig spoutConfig) {
    this.spoutConfig = spoutConfig;
  }

  /**
   * Sets the config to tell that its a Kafka spout.
   */
  @Override
  public Map<String, Object> getComponentConfiguration() {
    Map<String, Object> conf = new HashMap<>();
    conf.put("spout.type", "kafka");
    conf.put("spout.source", spoutConfig.topic);

    // TODO: Always change this when any change is made in the spout.
    conf.put("spout.version", "Jan 26, 2016");
    return conf;
  }

  /**
   * Creates cnxns. Called once per executor.
   */
  @Override
  public void open(
      Map conf, final TopologyContext context, final SpoutOutputCollector outputCollector) {
    this.collector = outputCollector;

    int totalTasks = context.getComponentTasks(context.getThisComponentId()).size();
    applyStormConf(conf);
    startCommitThread(conf);
    OffsetStoreManagerFactory offsetStoreManagerFactory = new OffsetStoreManagerFactory(
        spoutConfig);
    addFilterOperator(conf, context.getThisComponentId(), spoutConfig.topic);
    KafkaMetric.OffsetMetric kafkaOffsetMetric = new KafkaMetric.OffsetMetric();

    this.coordinator = new PartitionCoordinator(
        conf, spoutConfig, context.getThisTaskIndex(), totalTasks, uuid,
        offsetStoreManagerFactory.get(),
        kafkaOffsetMetric, context.getThisComponentId());

    spoutMetrics = new MultiCountMetric();

    transferCollector = new TransferCollector(spoutConfig.emitQueueMaxSize);

    context.registerMetric("kafkaSpout", spoutMetrics, 60);
    context.registerMetric("kafkaOffset", kafkaOffsetMetric, 60);
    context.registerMetric("kafkaPartition", new KafkaMetric.PartitionMetric(coordinator), 60);
    context.registerMetric("transferCollector", transferCollector, 60);

    // Start IO thread.
    startIOExecutor();
  }

  @Override
  public void nextTuple() {
    long startTime = System.nanoTime();
    spoutMetrics.scope("nextTupleCalls").incr();
    com.twitter.heron.spouts.kafka.common.TransferCollector.EmitData emitData = transferCollector
        .getEmitItems(1, TimeUnit.MILLISECONDS);
    if (emitData != null) {
      collector.emit(emitData.streamId, emitData.tuple, emitData.messageId);
      LOG.info("Just emitted a new message");
      if (!spoutConfig.shouldAnchorEmits) {
        // Anchoring is disabled, so call manually to maintain PartitionManager states
        ack(emitData.messageId);
      }
      spoutMetrics.scope("nextTupleEmitIdCount").incr();
    } else {
      spoutMetrics.scope("nextTupleZeroEmits").incr();
    }
    long endTime = System.nanoTime();
    spoutMetrics.scope("nextTupleTime").incrBy(endTime - startTime);
  }

  /**
   * Forward successful processing of the tuple to kafka.
   */
  @Override
  public void ack(Object msgId) {
    LOG.info("acknowledged that message was actually sent");
    PartitionManager.KafkaMessageId id = (PartitionManager.KafkaMessageId) msgId;
    PartitionManager m = coordinator.getManagerForId(id.partition);
    if (m != null) {
      m.ack(id.offset);
    }
  }

  /**
   * Replay the offset
   */
  @Override
  public void fail(Object msgId) {
    LOG.info("something went wrong, asked to re-emit the message");
    PartitionManager.KafkaMessageId id = (PartitionManager.KafkaMessageId) msgId;
    PartitionManager m = coordinator.getManagerForId(id.partition);
    if (m != null) {
      m.fail(id.offset);
    }
  }

  @Override
  public synchronized void deactivate() {
    commit();
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(spoutConfig.scheme.getOutputFields());
  }

  @Override
  public synchronized void close() {
    commit();
  }

  /**
   * Returns all partition managers associated with this spout.
   *
   * @return All partition managers associated with this spout.
   */
  public List<PartitionManager> getMyManagedPartitions() {
    return coordinator.getMyManagedPartitions();
  }

  /**
   * Returns the IDs of all partitions associated with the Kafka topic managed by this spout.
   *
   * @return The IDs of all partitions associated with the Kafka topic managed by this spout.
   */
  public Set<GlobalPartitionId> getAllPartitionIds() {
    return coordinator.getAllPartitionIds();
  }

  /**
   * Determines if the spout can read from the Kafka partition associated with the given partition
   * manager.
   *
   * @param manager The partition manager.
   * @return {@code true} if the spout can read from the Kafka partition associated with the given
   * partition manager; {@code false} otherwise.
   */
  protected boolean canReadPartition(PartitionManager manager) {
    return true;
  }

  private void applyStormConf(Map conf) {
    if (conf.containsKey(SpoutConfig.SPOUT_SHOULD_REPLAY_FAILED_OFFSETS)) {
      spoutConfig.shouldReplay = Boolean.parseBoolean(
          conf.get(SpoutConfig.SPOUT_SHOULD_REPLAY_FAILED_OFFSETS).toString());
    }
    if (conf.containsKey(SpoutConfig.SPOUT_MAX_FAILED_OFFSETS)) {
      spoutConfig.maxFailedTupleCount = Integer.parseInt(
          conf.get(SpoutConfig.SPOUT_MAX_FAILED_OFFSETS).toString());
    }

    if (conf.containsKey(SpoutConfig.START_OFFSET_DELTA)) {
      long offsetMillis = getWithDefault(
          conf,
          SpoutConfig.START_OFFSET_DELTA,
          spoutConfig.startOffsetTime
      );

      long startOffsetTime = System.currentTimeMillis() - offsetMillis;
      spoutConfig.forceStartOffsetTime(startOffsetTime);

      LOG.info("Using kafka offset with time " + startOffsetTime);
    }
  }

  private void addFilterOperator(Map conf, String spoutName, String topic) {
    if (conf.containsKey(SpoutConfig.TOPOLOGY_FILTER_CONFIG)) {
      JSONArray jsons = (JSONArray) JSONValue.parse(
          conf.get(SpoutConfig.TOPOLOGY_FILTER_CONFIG).toString());
      for (Object jsonObj : jsons) {
        JSONObject json = (JSONObject) jsonObj;
        String filterFor = json.get("spoutname").toString();
        String topicForFilter = "";
        if (json.get("topic") != null) {
          topicForFilter = json.get("topic").toString();
        }

        if (filterFor.equals(spoutName) || topic.equals(topicForFilter)) {
          try {
            String parameter = json.get("parameter").toString();
            String classname = json.get("classname").toString();
            LOG.info("Initializing class " + classname + " with parameter " + parameter);
            Constructor<?> cons = Class.forName(classname).getConstructor(String.class);
            spoutConfig.filterOperator = (FilterOperator) cons.newInstance(parameter);
          } catch (Exception e) {
            LOG.error("Failed to instantiate filter operator. ", e);
            throw new RuntimeException("Failed to instantiate filter operator.");
          }
        } else {
          LOG.info("Not applying filter for spout = " + spoutName + " topic = " + topic);
        }
      }
    }
  }

  protected void startIOExecutor() {
    IOExecutorService.FnParam<Boolean, TransferCollector.EmitData, PartitionCoordinator> fn =
        new IOExecutorService.FnParam<Boolean, TransferCollector.EmitData, PartitionCoordinator>() {
          @Override
          public Boolean apply(BlockingQueue<TransferCollector.EmitData> stream,
                               PartitionCoordinator context) {
            for (PartitionManager manager : context.getMyManagedPartitions()) {
              manager.next((TransferCollector) stream);
            }
            return true;
          }
        };
    this.executor = new IOExecutorService.SingleThreadIOExecutorService<>(
        fn, coordinator, transferCollector);
    executor.scheduleContinuously();
  }

  /**
   * Initializes storage and starts Thread which will commit offsets with given frequency.
   */
  private void startCommitThread(Map conf) {

    int updateMsec = getWithDefault(conf, SpoutConfig.TOPOLOGY_STORE_UPDATE_MSEC, spoutConfig
        .storeUpdateMsec);

    // Start commit thread.
    Runnable refreshTask = new Runnable() {
      @Override
      public void run() {
        try {
          commit();
        } catch (Exception e) {
          LOG.error("Failed to commit offsets to store ", e);
        }
      }
    };

    Executors.newSingleThreadScheduledExecutor()
        .scheduleAtFixedRate(refreshTask, 0, updateMsec, TimeUnit.MILLISECONDS);
    LOG.info("Added metadata store");
  }

  @SuppressWarnings("unchecked")
  private <T> T getWithDefault(Map conf, String key, T value) {
    if (conf.get(key) != null) {
      return (T) (conf.get(key));
    }
    return value;
  }

  /**
   * Commit the kafka offset processed to persistent store for each partition.
   */
  private synchronized void commit() {
    if (coordinator != null) {
      for (PartitionManager manager : coordinator.getMyManagedPartitions()) {
        if (manager != null) {
          manager.commit();
        }
      }
    }
  }
}
