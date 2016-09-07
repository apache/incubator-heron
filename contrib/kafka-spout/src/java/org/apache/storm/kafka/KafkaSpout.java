/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.kafka;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Strings;

import org.apache.storm.Config;
import org.apache.storm.kafka.PartitionManager.KafkaMessageId;
import org.apache.storm.kafka.trident.GlobalPartitionInformation;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: need to add blacklisting
// TODO: need to make a best effort to not re-emit messages if don't have to
public class KafkaSpout extends BaseRichSpout {
  private static final long serialVersionUID = -5959620764859243404L;

  enum EmitState {
    EMITTED_MORE_LEFT,
    EMITTED_END,
    NO_EMITTED
  }

  private static final Logger LOG = LoggerFactory.getLogger(KafkaSpout.class);

  private SpoutConfig spoutConfig;
  private SpoutOutputCollector collector;
  private PartitionCoordinator coordinator;
  private DynamicPartitionConnections connections;
  private ZkState state;

  private long lastUpdateMs = 0;

  private int currPartitionIndex = 0;

  public KafkaSpout(SpoutConfig spoutConf) {
    spoutConfig = spoutConf;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void open(Map<String, Object> conf,
                   final TopologyContext context,
                   final SpoutOutputCollector aCollector) {
    collector = aCollector;
    String topologyInstanceId = context.getStormId();
    Map<String, Object> stateConf = new HashMap<>(conf);
    List<String> zkServers = spoutConfig.zkServers;
    if (zkServers == null) {
      zkServers = (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS);
    }
    Integer zkPort = spoutConfig.zkPort;
    if (zkPort == null) {
      zkPort = ((Number) conf.get(Config.STORM_ZOOKEEPER_PORT)).intValue();
    }
    stateConf.put(Config.TRANSACTIONAL_ZOOKEEPER_SERVERS, zkServers);
    stateConf.put(Config.TRANSACTIONAL_ZOOKEEPER_PORT, zkPort);
    stateConf.put(Config.TRANSACTIONAL_ZOOKEEPER_ROOT, spoutConfig.zkRoot);
    state = new ZkState(stateConf);

    connections = new DynamicPartitionConnections(spoutConfig,
        KafkaUtils.makeBrokerReader(conf, spoutConfig));

    // using TransactionalState like this is a hack
    int totalTasks = context.getComponentTasks(context.getThisComponentId()).size();
    if (spoutConfig.hosts instanceof StaticHosts) {
      coordinator = new StaticCoordinator(connections, conf,
          spoutConfig, state, context.getThisTaskIndex(),
          totalTasks, topologyInstanceId);
    } else {
      coordinator = new ZkCoordinator(connections, conf,
          spoutConfig, state, context.getThisTaskIndex(),
          totalTasks, topologyInstanceId);
    }

    context.registerMetric("kafkaOffset", new IMetric() {
      private KafkaUtils.KafkaOffsetMetric kafkaOffsetMetric =
          new KafkaUtils.KafkaOffsetMetric(connections);

      @Override
      public Object getValueAndReset() {
        List<PartitionManager> pms = coordinator.getMyManagedPartitions();
        Set<Partition> latestPartitions = new HashSet<>();
        for (PartitionManager pm : pms) {
          latestPartitions.add(pm.getPartition());
        }
        kafkaOffsetMetric.refreshPartitions(latestPartitions);
        for (PartitionManager pm : pms) {
          kafkaOffsetMetric.setOffsetData(pm.getPartition(), pm.getOffsetData());
        }
        return kafkaOffsetMetric.getValueAndReset();
      }
    }, spoutConfig.metricsTimeBucketSizeInSecs);

    context.registerMetric("kafkaPartition", new IMetric() {
      @Override
      public Object getValueAndReset() {
        List<PartitionManager> pms = coordinator.getMyManagedPartitions();
        Map<String, Object> concatMetricsDataMaps = new HashMap<>();
        for (PartitionManager pm : pms) {
          concatMetricsDataMaps.putAll(pm.getMetricsDataMap());
        }
        return concatMetricsDataMaps;
      }
    }, spoutConfig.metricsTimeBucketSizeInSecs);
  }

  @Override
  public void close() {
    state.close();
  }

  @Override
  public void nextTuple() {
    List<PartitionManager> managers = coordinator.getMyManagedPartitions();
    for (int i = 0; i < managers.size(); i++) {

      try {
        // in case the number of managers decreased
        currPartitionIndex = currPartitionIndex % managers.size();
        EmitState emitState = managers.get(currPartitionIndex).next(collector);
        if (emitState != EmitState.EMITTED_MORE_LEFT) {
          currPartitionIndex = (currPartitionIndex + 1) % managers.size();
        }
        if (emitState != EmitState.NO_EMITTED) {
          break;
        }
      } catch (FailedFetchException e) {
        LOG.warn("Fetch failed", e);
        coordinator.refresh();
      }
    }

    long diffWithNow = System.currentTimeMillis() - lastUpdateMs;

        /*
             As far as the System.currentTimeMillis() is dependent on System clock,
             additional check on negative value of diffWithNow in case of external changes.
         */
    if (diffWithNow > spoutConfig.stateUpdateIntervalMs || diffWithNow < 0) {
      commit();
    }
  }

  @Override
  public void ack(Object msgId) {
    KafkaMessageId id = (KafkaMessageId) msgId;
    PartitionManager m = coordinator.getManager(id.partition);
    if (m != null) {
      m.ack(id.offset);
    }
  }

  @Override
  public void fail(Object msgId) {
    KafkaMessageId id = (KafkaMessageId) msgId;
    PartitionManager m = coordinator.getManager(id.partition);
    if (m != null) {
      m.fail(id.offset);
    }
  }

  @Override
  public void deactivate() {
    commit();
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    if (!Strings.isNullOrEmpty(spoutConfig.outputStreamId)) {
      declarer.declareStream(spoutConfig.outputStreamId, spoutConfig.scheme.getOutputFields());
    } else {
      declarer.declare(spoutConfig.scheme.getOutputFields());
    }
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    Map<String, Object> configuration = super.getComponentConfiguration();
    if (configuration == null) {
      configuration = new HashMap<>();
    }
    String configKeyPrefix = "config.";
    configuration.put(configKeyPrefix + "topics", this.spoutConfig.topic);
    StringBuilder zkServers = new StringBuilder();
    if (spoutConfig.zkServers != null && spoutConfig.zkServers.size() > 0) {
      for (String zkServer : this.spoutConfig.zkServers) {
        zkServers.append(zkServer + ":" + this.spoutConfig.zkPort + ",");
      }
      configuration.put(configKeyPrefix + "zkServers", zkServers.toString());
    }
    BrokerHosts brokerHosts = this.spoutConfig.hosts;
    String zkRoot = this.spoutConfig.zkRoot + "/" + this.spoutConfig.id;
    if (brokerHosts instanceof ZkHosts) {
      ZkHosts zkHosts = (ZkHosts) brokerHosts;
      configuration.put(configKeyPrefix + "zkNodeBrokers", zkHosts.brokerZkPath);
    } else if (brokerHosts instanceof StaticHosts) {
      StaticHosts staticHosts = (StaticHosts) brokerHosts;
      GlobalPartitionInformation globalPartitionInformation = staticHosts.getPartitionInformation();
      boolean useTopicNameForPath = globalPartitionInformation.getbUseTopicNameForPartitionPathId();
      if (useTopicNameForPath) {
        zkRoot += "/" + this.spoutConfig.topic;
      }
      List<Partition> partitions = globalPartitionInformation.getOrderedPartitions();
      StringBuilder staticPartitions = new StringBuilder();
      StringBuilder leaderHosts = new StringBuilder();
      for (Partition partition : partitions) {
        staticPartitions.append(partition.partition + ",");
        leaderHosts.append(partition.host.host + ":" + partition.host.port).append(",");
      }
      configuration.put(configKeyPrefix + "partitions", staticPartitions.toString());
      configuration.put(configKeyPrefix + "leaders", leaderHosts.toString());
    }
    configuration.put(configKeyPrefix + "zkRoot", zkRoot);
    return configuration;
  }

  private void commit() {
    lastUpdateMs = System.currentTimeMillis();
    for (PartitionManager manager : coordinator.getMyManagedPartitions()) {
      manager.commit();
    }
  }

}
