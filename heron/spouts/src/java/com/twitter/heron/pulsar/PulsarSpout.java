//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package com.twitter.heron.pulsar;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.yahoo.pulsar.client.api.ClientConfiguration;
import com.yahoo.pulsar.client.api.Consumer;
import com.yahoo.pulsar.client.api.ConsumerConfiguration;
import com.yahoo.pulsar.client.api.Message;
import com.yahoo.pulsar.client.api.MessageId;
import com.yahoo.pulsar.client.api.PulsarClient;
import com.yahoo.pulsar.client.api.PulsarClientException;
import com.yahoo.pulsar.client.api.SubscriptionType;
import com.yahoo.pulsar.client.impl.PulsarClientImpl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.heron.api.metric.IMetric;
import com.twitter.heron.api.spout.BaseRichSpout;
import com.twitter.heron.api.spout.SpoutOutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Values;

/**
 * A spout that subscribes to a topic and receives messages from Pulsar.
 *
 * https://github.com/apache/incubator-pulsar
 *
 * The following values are required:
 * serviceUrl, topic, subscription, messageToValuesMapper.
 *
 * Simple usage:
 *
 * new PulsarSpout.Builder()
 *  .setServiceUrl("pulsar://localhost:6650")
 *  .setTopic("persistent://sample/standalone/ns1/my-topic")
 *  .setSubscription("my-subscription")
 *  .setMessageToValuesMapper(new MyMessageToValuesMapper()
 *  .build();
 *
 *  If a ConsumerConfiguration is not provided then the default one is applied.
 *  The default ConsumerConfiguration has the following properties:
 *  - the subscription type is shared
 *  - the acknowledgement timeout is 60 seconds
 *
 */
@SuppressWarnings("FinalClass")
public class PulsarSpout extends BaseRichSpout implements IMetric<Map<String, Number>> {
  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(PulsarSpout.class);

  private static final int TIMEOUT_MS = 100;
  private static final int DEFAULT_ACKNOWLEDGEMENT_TIMEOUT_SECS = 60;
  private static final int DEFAULT_METRICS_TIME_INTERVAL_IN_SECS = 60;

  private static final String METRIC_MESSAGES_RECEIVED = "messagesReceived";
  private static final String METRIC_MESSAGES_EMITTED = "messagesEmitted";
  private static final String METRIC_MESSAGES_ACKNOWLEDGED = "messagesAcknowledged";
  private static final String METRIC_MESSAGES_FAILED = "messagesFailed";
  private static final String METRIC_MESSAGE_BYTES_RECEIVED = "messageBytesReceived";
  private static final String METRIC_MESSAGE_THROUGHPUT = "messageThroughput";
  private static final String METRIC_MESSAGE_BYTES_THROUGHPUT = "messageBytesThroughput";

  private final String serviceUrl;
  private final String topic;
  private final String subscription;
  private final ClientConfiguration clientConfiguration;
  private final ConsumerConfiguration consumerConfiguration;
  private final MessageToValuesMapper messageToValuesMapper;
  private final int metricsTimeIntervalSeconds;

  private PulsarClient client;
  private Consumer consumer;

  private SpoutOutputCollector collector;

  private String spoutId;

  // pulsar metrics
  private long messagesReceived;
  private long messagesEmitted;
  private long messagesAcknowledged;
  private long messagesFailed;
  private long messageBytesReceived;

  private PulsarSpout(Builder builder) {
    serviceUrl = builder.serviceUrl;
    topic = builder.topic;
    subscription = builder.subscription;
    messageToValuesMapper = builder.messageToValuesMapper;
    clientConfiguration =
        Utils.defaultIfNull(builder.clientConfiguration, getDefaultClientConfiguration());
    consumerConfiguration =
        Utils.defaultIfNull(builder.consumerConfiguration, getDefaultConsumerConfiguration());

    // we must specify a consumer acknowledgement timeout so messages are replayed
    // if one is not set then default to 60 seconds
    if (consumerConfiguration.getAckTimeoutMillis() == 0) {
      consumerConfiguration
          .setAckTimeout(DEFAULT_ACKNOWLEDGEMENT_TIMEOUT_SECS, TimeUnit.SECONDS);
    }

    metricsTimeIntervalSeconds =
        builder.metricsTimeIntervalInSeconds > 0
            ? builder.metricsTimeIntervalInSeconds : DEFAULT_METRICS_TIME_INTERVAL_IN_SECS;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    messageToValuesMapper.declareOutputFields(declarer);
  }

  @Override
  public void open(Map<String, Object> conf, TopologyContext context,
      SpoutOutputCollector spoutOutputCollector) {
    spoutId = String.format("%s-%d", context.getThisComponentId(), context.getThisTaskId());
    try {
      client = createClient();
      consumer = client.subscribe(topic, subscription, consumerConfiguration);
      collector = spoutOutputCollector;

      final String metricName = String.format("PulsarSpoutMetrics-%s", spoutId);
      context.registerMetric(metricName, this, metricsTimeIntervalSeconds);

      LOG.info("[{}] Created a pulsar consumer on topic {} "
              + "to receive messages with subscription {}",
              spoutId, topic, subscription);
    } catch (PulsarClientException pce) {
      LOG.error("[{}] Error creating pulsar consumer on topic {}", spoutId, topic, pce);
      throw new RuntimeException(pce);
    }
  }

  @Override
  public void nextTuple() {
    try {
      final Message message = consumer.receive(TIMEOUT_MS, TimeUnit.MILLISECONDS);
      if (message != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("[{}] Received message {}", spoutId, message.getMessageId());
        }
        messagesReceived++;
        messageBytesReceived += message.getData().length;

        final Values values = messageToValuesMapper.toValues(message);

        collector.emit(values, message.getMessageId());
        messagesEmitted++;
      }
    } catch (PulsarClientException pce) {
      LOG.error("[{}] Error emitting next tuple", spoutId, pce);
      throw new RuntimeException(pce);
    }
  }

  @Override
  public void ack(Object msgId) {
    if (msgId instanceof MessageId) {
      final MessageId messageId = (MessageId) msgId;
      if (LOG.isDebugEnabled()) {
        LOG.debug("[{}] Received ack for message {}", spoutId, messageId);
      }
      consumer.acknowledgeAsync(messageId);

      messagesAcknowledged++;
    }
  }

  @Override
  public void fail(Object msgId) {
    if (msgId instanceof MessageId) {
      LOG.warn("[{}] Error processing message {}", spoutId, msgId.toString());
      messagesFailed++;
    }
  }

  @Override
  public void close() {
    Utils.closeSilently(consumer);
    Utils.closeSilently(client);
  }

  @Override
  public Map<String, Number> getValueAndReset() {
    final Map<String, Number> metrics = new HashMap<>();
    metrics.put(METRIC_MESSAGES_RECEIVED, messagesReceived);
    metrics.put(METRIC_MESSAGES_EMITTED, messagesEmitted);
    metrics.put(METRIC_MESSAGES_ACKNOWLEDGED, messagesAcknowledged);
    metrics.put(METRIC_MESSAGES_FAILED, messagesFailed);
    metrics.put(METRIC_MESSAGE_BYTES_RECEIVED, messageBytesReceived);

    metrics.put(METRIC_MESSAGE_THROUGHPUT,
        (double) messagesReceived / metricsTimeIntervalSeconds);
    metrics.put(METRIC_MESSAGE_BYTES_THROUGHPUT,
        (double) messageBytesReceived / metricsTimeIntervalSeconds);

    // reset pulsar metrics
    messagesReceived = 0;
    messagesEmitted = 0;
    messagesAcknowledged = 0;
    messagesFailed = 0;
    messageBytesReceived = 0;

    return metrics;
  }

  PulsarClient createClient()
      throws PulsarClientException {
    return new PulsarClientImpl(serviceUrl, clientConfiguration);
  }

  public static ConsumerConfiguration getDefaultConsumerConfiguration() {
    return new ConsumerConfiguration()
        .setSubscriptionType(SubscriptionType.Shared)
        .setAckTimeout(DEFAULT_ACKNOWLEDGEMENT_TIMEOUT_SECS, TimeUnit.SECONDS);
  }

  public static ClientConfiguration getDefaultClientConfiguration() {
    return new ClientConfiguration();
  }

  public static class Builder {
    private String serviceUrl;
    private String topic;
    private String subscription;
    private ClientConfiguration clientConfiguration;
    private ConsumerConfiguration consumerConfiguration;
    private MessageToValuesMapper messageToValuesMapper;
    private int metricsTimeIntervalInSeconds;

    @SuppressWarnings("HiddenField")
    public Builder setServiceUrl(String serviceUrl) {
      this.serviceUrl = serviceUrl;
      return this;
    }

    @SuppressWarnings("HiddenField")
    public Builder setTopic(String topic) {
      this.topic = topic;
      return this;
    }

    @SuppressWarnings("HiddenField")
    public Builder setSubscription(String subscription) {
      this.subscription = subscription;
      return this;
    }

    @SuppressWarnings("HiddenField")
    public Builder setClientConfiguration(
        ClientConfiguration clientConfiguration) {
      this.clientConfiguration = clientConfiguration;
      return this;
    }

    @SuppressWarnings("HiddenField")
    public Builder setConsumerConfiguration(
        ConsumerConfiguration consumerConfiguration) {
      this.consumerConfiguration = consumerConfiguration;
      return this;
    }

    @SuppressWarnings("HiddenField")
    public Builder setMessageToValuesMapper(
        MessageToValuesMapper messageToValuesMapper) {
      this.messageToValuesMapper = messageToValuesMapper;
      return this;
    }

    public Builder setMetricsTimeInterval(int seconds) {
      metricsTimeIntervalInSeconds = seconds;
      return this;
    }

    public PulsarSpout build() {
      Utils.checkNotNull(serviceUrl, "A service url must be provided");
      Utils.checkNotNull(topic, "A topic must be provided");
      Utils.checkNotNull(subscription, "A subscription must be provided");
      Utils.checkNotNull(messageToValuesMapper,
          "A MessageToValuesMapper must be provided");
      return new PulsarSpout(this);
    }
  }
}
