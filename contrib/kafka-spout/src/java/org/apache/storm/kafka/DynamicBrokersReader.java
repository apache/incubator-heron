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

import java.io.UnsupportedEncodingException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.storm.Config;
import org.apache.storm.kafka.trident.GlobalPartitionInformation;
import org.apache.storm.utils.Utils;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DynamicBrokersReader {

  private static final Logger LOG = LoggerFactory.getLogger(DynamicBrokersReader.class);

  private CuratorFramework curator;
  private String zkPath;
  private String myTopic;
  private Boolean isWildcardTopic;

  public DynamicBrokersReader(Map<String, Object> conf, String zkStr, String zkPath, String topic) {
    // Check required parameters
    Preconditions.checkNotNull(conf, "conf cannot be null");

    validateConfig(conf);

    Preconditions.checkNotNull(zkStr, "zkString cannot be null");
    Preconditions.checkNotNull(zkPath, "zkPath cannot be null");
    Preconditions.checkNotNull(topic, "topic cannot be null");

    this.zkPath = zkPath;
    this.myTopic = topic;
    this.isWildcardTopic = Utils.getBoolean(conf.get("kafka.topic.wildcard.match"), false);
    try {
      curator = CuratorFrameworkFactory.newClient(
          zkStr,
          Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT)),
          Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT)),
          new RetryNTimes(Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_RETRY_TIMES)),
              Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_RETRY_INTERVAL))));
      curator.start();
      // SUPPRESS CHECKSTYLE IllegalCatch
    } catch (Exception ex) {
      LOG.error("Couldn't connect to zookeeper", ex);
      throw new RuntimeException(ex);
    }
  }

  /**
   * Get all partitions with their current leaders
   */
  public List<GlobalPartitionInformation> getBrokerInfo() throws SocketTimeoutException {
    List<String> topics = getTopics();
    List<GlobalPartitionInformation> partitions = new ArrayList<GlobalPartitionInformation>();

    for (String topic : topics) {
      GlobalPartitionInformation globalPartitionInformation =
          new GlobalPartitionInformation(topic, this.isWildcardTopic);
      try {
        int numPartitionsForTopic = getNumPartitions(topic);
        String brokerInfoPath = brokerPath();
        for (int partition = 0; partition < numPartitionsForTopic; partition++) {
          int leader = getLeaderFor(topic, partition);
          String path = brokerInfoPath + "/" + leader;
          try {
            byte[] brokerData = curator.getData().forPath(path);
            Broker hp = getBrokerHost(brokerData);
            globalPartitionInformation.addPartition(partition, hp);
          } catch (org.apache.zookeeper.KeeperException.NoNodeException e) {
            LOG.error("Node {} does not exist ", path);
          }
        }
      } catch (SocketTimeoutException e) {
        throw e;
        // SUPPRESS CHECKSTYLE IllegalCatch
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      LOG.info("Read partition info from zookeeper: " + globalPartitionInformation);
      partitions.add(globalPartitionInformation);
    }
    return partitions;
  }

  private int getNumPartitions(String topic) {
    try {
      String topicBrokersPath = partitionPath(topic);
      List<String> children = curator.getChildren().forPath(topicBrokersPath);
      return children.size();
      // SUPPRESS CHECKSTYLE IllegalCatch
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private List<String> getTopics() {
    List<String> topics = new ArrayList<String>();
    if (!isWildcardTopic) {
      topics.add(myTopic);
      return topics;
    } else {
      try {
        List<String> children = curator.getChildren().forPath(topicsPath());
        for (String t : children) {
          if (t.matches(myTopic)) {
            LOG.info(String.format("Found matching topic %s", t));
            topics.add(t);
          }
        }
        return topics;
        // SUPPRESS CHECKSTYLE IllegalCatch
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public String topicsPath() {
    return zkPath + "/topics";
  }

  public String partitionPath(String topic) {
    return topicsPath() + "/" + topic + "/partitions";
  }

  public String brokerPath() {
    return zkPath + "/ids";
  }


  /**
   * get /brokers/topics/distributedTopic/partitions/1/state
   * { "controller_epoch":4, "isr":[ 1, 0 ], "leader":1, "leader_epoch":1, "version":1 }
   * @param topic
   * @param partition
   * @return
   */
  @SuppressWarnings("unchecked")
  private int getLeaderFor(String topic, long partition) {
    try {
      String topicBrokersPath = partitionPath(topic);
      byte[] hostPortData =
          curator.getData().forPath(topicBrokersPath + "/" + partition + "/state");
      Map<Object, Object> value =
          (Map<Object, Object>) JSONValue.parse(new String(hostPortData, "UTF-8"));
      Integer leader = ((Number) value.get("leader")).intValue();
      if (leader == -1) {
        throw new IllegalArgumentException("No leader found for partition " + partition);
      }
      return leader;
      // SUPPRESS CHECKSTYLE IllegalCatch
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void close() {
    curator.close();
  }

  /**
   * [zk: localhost:2181(CONNECTED) 56] get /brokers/ids/0
   * { "host":"localhost", "jmx_port":9999, "port":9092, "version":1 }
   *
   * @param contents
   * @return
   */
  @SuppressWarnings("unchecked")
  private Broker getBrokerHost(byte[] contents) {
    try {
      Map<Object, Object> value =
          (Map<Object, Object>) JSONValue.parse(new String(contents, "UTF-8"));
      String host = (String) value.get("host");
      Integer port = ((Long) value.get("port")).intValue();
      return new Broker(host, port);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Validate required parameters in the input configuration Map
   * @param conf
   */
  private void validateConfig(final Map<String, Object> conf) {
    Preconditions.checkNotNull(conf.get(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT),
        "%s cannot be null", Config.STORM_ZOOKEEPER_SESSION_TIMEOUT);
    Preconditions.checkNotNull(conf.get(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT),
        "%s cannot be null", Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT);
    Preconditions.checkNotNull(conf.get(Config.STORM_ZOOKEEPER_RETRY_TIMES),
        "%s cannot be null", Config.STORM_ZOOKEEPER_RETRY_TIMES);
    Preconditions.checkNotNull(conf.get(Config.STORM_ZOOKEEPER_RETRY_INTERVAL),
        "%s cannot be null", Config.STORM_ZOOKEEPER_RETRY_INTERVAL);
  }

}
