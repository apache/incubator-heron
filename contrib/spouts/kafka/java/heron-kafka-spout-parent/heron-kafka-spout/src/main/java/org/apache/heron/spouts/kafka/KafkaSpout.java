/*
 * Copyright 2019
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.heron.spouts.kafka;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.spout.BaseRichSpout;
import com.twitter.heron.api.spout.SpoutOutputCollector;
import com.twitter.heron.api.state.State;
import com.twitter.heron.api.topology.IStatefulComponent;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.common.basics.SingletonRegistry;
import com.twitter.heron.common.config.SystemConfig;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

/**
 * Kafka spout to consume data from Kafka topic(s), each record is converted into a tuple via {@link ConsumerRecordTransformer}, and emitted into a topology
 *
 * @param <K> the type of the key field of the Kafka record
 * @param <V> the type of the value field of the Kafka record
 */
public class KafkaSpout<K, V> extends BaseRichSpout implements IStatefulComponent<TopicPartition, Long> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSpout.class);
    private static final long serialVersionUID = -2271355516537883361L;
    private int metricsIntervalInSecs = 60;
    private KafkaConsumerFactory<K, V> kafkaConsumerFactory;
    private TopicPatternProvider topicPatternProvider;
    private Collection<String> topicNames;
    private ConsumerRecordTransformer<K, V> consumerRecordTransformer = new DefaultConsumerRecordTransformer<>();
    private transient SpoutOutputCollector collector;
    private transient TopologyContext topologyContext;
    private transient Queue<ConsumerRecord<K, V>> buffer;
    private transient Consumer<K, V> consumer;
    private transient Set<TopicPartition> assignedPartitions;
    private transient Set<MetricName> reportedMetrics;
    private transient Map<TopicPartition, NavigableMap<Long, Long>> ackRegistry;
    private transient Map<TopicPartition, Long> failureRegistry;
    private Config.TopologyReliabilityMode topologyReliabilityMode = Config.TopologyReliabilityMode.ATMOST_ONCE;
    private long previousKafkaMetricsUpdatedTimestamp = 0;
    private State<TopicPartition, Long> state;

    /**
     * create a KafkaSpout instance that subscribes to a list of topics
     *
     * @param kafkaConsumerFactory kafka consumer factory
     * @param topicNames           list of topic names
     */
    public KafkaSpout(KafkaConsumerFactory<K, V> kafkaConsumerFactory, Collection<String> topicNames) {
        this.kafkaConsumerFactory = kafkaConsumerFactory;
        this.topicNames = topicNames;
    }

    /**
     * create a KafkaSpout instance that subscribe to all topics matching the topic pattern
     *
     * @param kafkaConsumerFactory kafka consumer factory
     * @param topicPatternProvider provider of the topic matching pattern
     */
    @SuppressWarnings("WeakerAccess")
    public KafkaSpout(KafkaConsumerFactory<K, V> kafkaConsumerFactory, TopicPatternProvider topicPatternProvider) {
        this.kafkaConsumerFactory = kafkaConsumerFactory;
        this.topicPatternProvider = topicPatternProvider;
    }

    /**
     * @return the Kafka record transformer instance used by this Kafka Spout
     */
    @SuppressWarnings("WeakerAccess")
    public ConsumerRecordTransformer<K, V> getConsumerRecordTransformer() {
        return consumerRecordTransformer;
    }

    /**
     * set the Kafka record transformer
     *
     * @param consumerRecordTransformer kafka record transformer
     */
    @SuppressWarnings("WeakerAccess")
    public void setConsumerRecordTransformer(ConsumerRecordTransformer<K, V> consumerRecordTransformer) {
        this.consumerRecordTransformer = consumerRecordTransformer;
    }

    @Override
    public void initState(State<TopicPartition, Long> state) {
        this.state = state;
        LOG.info("initial state {}", state);
    }

    @Override
    public void preSave(String checkpointId) {
        LOG.info("save state {}", state);
        consumer.commitAsync(state.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> new OffsetAndMetadata(entry.getValue() + 1))), null);
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.topologyContext = context;
        this.topologyReliabilityMode = Config.TopologyReliabilityMode.valueOf(conf.get(Config.TOPOLOGY_RELIABILITY_MODE).toString());
        metricsIntervalInSecs = (int) ((SystemConfig) SingletonRegistry.INSTANCE.getSingleton(SystemConfig.HERON_SYSTEM_CONFIG)).getHeronMetricsExportInterval().getSeconds();
        consumer = kafkaConsumerFactory.create();
        if (topicNames != null) {
            consumer.subscribe(topicNames, new KafkaConsumerRebalanceListener());
        } else {
            consumer.subscribe(topicPatternProvider.create(), new KafkaConsumerRebalanceListener());
        }
        buffer = new ArrayDeque<>(500);
        ackRegistry = new ConcurrentHashMap<>();
        failureRegistry = new ConcurrentHashMap<>();
        assignedPartitions = new HashSet<>();
        reportedMetrics = new HashSet<>();
    }

    @Override
    public void nextTuple() {
        ConsumerRecord<K, V> record = buffer.poll();
        if (record != null) {
            // there are still records remaining for emission from the previous poll
            emitConsumerRecord(record);
        } else {
            //all the records from previous poll have been emitted or this is very first time to poll
            if (topologyReliabilityMode == Config.TopologyReliabilityMode.ATLEAST_ONCE) {
                ackRegistry.forEach((key, value) -> {
                    if (value != null) {
                        //seek back to the earliest failed offset if there is any
                        rewindAndDiscardAck(key, value);
                        //commit based on the first continuous acknowledgement range
                        manualCommit(key, value);
                    }
                });
            }
            poll().forEach(kvConsumerRecord -> buffer.offer(kvConsumerRecord));
        }
    }

    @Override
    public void activate() {
        super.activate();
        if (!assignedPartitions.isEmpty()) {
            consumer.resume(assignedPartitions);
        }
    }

    @Override
    public void deactivate() {
        super.deactivate();
        if (!assignedPartitions.isEmpty()) {
            consumer.pause(assignedPartitions);
        }
    }

    @Override
    public void ack(Object msgId) {
        super.ack(msgId);
        long start = System.nanoTime();
        ConsumerRecordMessageId consumerRecordMessageId = (ConsumerRecordMessageId) msgId;
        TopicPartition topicPartition = consumerRecordMessageId.getTopicPartition();
        long offset = consumerRecordMessageId.getOffset();
        ackRegistry.putIfAbsent(topicPartition, new ConcurrentSkipListMap<>());
        NavigableMap<Long, Long> navigableMap = ackRegistry.get(topicPartition);

        Map.Entry<Long, Long> floorRange = navigableMap.floorEntry(offset);
        Map.Entry<Long, Long> ceilingRange = navigableMap.ceilingEntry(offset);

        long floorBottom = floorRange != null ? floorRange.getKey() : Long.MIN_VALUE;
        long floorTop = floorRange != null ? floorRange.getValue() : Long.MIN_VALUE;
        long ceilingBottom = ceilingRange != null ? ceilingRange.getKey() : Long.MAX_VALUE;
        long ceilingTop = ceilingRange != null ? ceilingRange.getValue() : Long.MAX_VALUE;

        /*
          the ack is for a message that has already been acknowledged. This happens when a failed tuple has caused
          Kafka consumer to seek back to earlier position and some messages are replayed.
         */
        if ((offset >= floorBottom && offset <= floorTop) || (offset >= ceilingBottom && offset <= ceilingTop))
            return;
        if (ceilingBottom - floorTop == 2) {
            /*
            the ack connects the two adjacent range
             */
            navigableMap.put(floorBottom, ceilingTop);
            navigableMap.remove(ceilingBottom);
        } else if (offset == floorTop + 1) {
            /*
            the acknowledged offset is the immediate neighbour of the upper bound of the floor range
             */
            navigableMap.put(floorBottom, offset);
        } else if (offset == ceilingBottom - 1) {
            /*
            the acknowledged offset is the immediate neighbour of the lower bound of the ceiling range
             */
            navigableMap.remove(ceilingBottom);
            navigableMap.put(offset, ceilingTop);
        } else {
            /*
            it is a new born range
             */
            navigableMap.put(offset, offset);
        }
        LOG.debug("ack {} in {} ns", msgId, System.nanoTime() - start);
        LOG.debug("{}", ackRegistry.get(consumerRecordMessageId.getTopicPartition()));
    }

    @Override
    public void fail(Object msgId) {
        super.fail(msgId);
        ConsumerRecordMessageId consumerRecordMessageId = (ConsumerRecordMessageId) msgId;
        TopicPartition topicPartition = consumerRecordMessageId.getTopicPartition();
        long offset = consumerRecordMessageId.getOffset();
        failureRegistry.put(topicPartition, Math.min(failureRegistry.getOrDefault(topicPartition, Long.MAX_VALUE), offset));
        LOG.warn("fail {}", msgId);
    }

    @Override
    public void close() {
        consumer.close();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        consumerRecordTransformer.getOutputStreams()
                .forEach(s -> declarer.declareStream(s, new Fields(consumerRecordTransformer.getFieldNames(s))));
    }

    private void emitConsumerRecord(ConsumerRecord<K, V> record) {
        consumerRecordTransformer.transform(record)
                .forEach((s, objects) -> {
                    if (topologyReliabilityMode != Config.TopologyReliabilityMode.ATLEAST_ONCE) {
                        collector.emit(s, objects);
                        //only in effective once mode, we need to track the offset of the record that is just emitted into the topology
                        if (topologyReliabilityMode == Config.TopologyReliabilityMode.EFFECTIVELY_ONCE) {
                            state.put(new TopicPartition(record.topic(), record.partition()), record.offset());
                        }
                    } else {
                        //build message id based on topic, partition, offset of the consumer record
                        ConsumerRecordMessageId consumerRecordMessageId = new ConsumerRecordMessageId(new TopicPartition(record.topic(), record.partition()), record.offset());
                        //emit tuple with the message id
                        collector.emit(s, objects, consumerRecordMessageId);
                    }
                });
    }

    private void rewindAndDiscardAck(TopicPartition topicPartition, NavigableMap<Long, Long> ackRanges) {
        if (failureRegistry.containsKey(topicPartition)) {
            long earliestFailedOffset = failureRegistry.get(topicPartition);
            //rewind back to the earliest failed offset
            consumer.seek(topicPartition, earliestFailedOffset);
            //discard the ack whose offset is greater than the earliest failed offset if there is any because we've rewound the consumer back
            SortedMap<Long, Long> sortedMap = ackRanges.headMap(earliestFailedOffset);
            if (!sortedMap.isEmpty()) {
                sortedMap.put(sortedMap.lastKey(), Math.min(earliestFailedOffset, sortedMap.get(sortedMap.lastKey())));
            }
            ackRegistry.put(topicPartition, new ConcurrentSkipListMap<>(sortedMap));
            //failure for this partition has been dealt with
            failureRegistry.remove(topicPartition);
        }
    }

    private void manualCommit(TopicPartition topicPartition, NavigableMap<Long, Long> ackRanges) {
        //the first entry in the acknowledgement registry keeps track of the lowest possible offset that can be committed
        Map.Entry<Long, Long> firstEntry = ackRanges.firstEntry();
        if (firstEntry != null) {
            consumer.commitAsync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(firstEntry.getValue() + 1)), null);
        }
    }

    private Iterable<ConsumerRecord<K, V>> poll() {
        ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(200));
        if (!records.isEmpty()) {
            /*
            since the Kafka Consumer metrics are built gradually based on the partitions it consumes,
            we need to periodically check whether there's any new metrics to register after each polling.
             */
            if (System.currentTimeMillis() - previousKafkaMetricsUpdatedTimestamp > metricsIntervalInSecs) {
                registerConsumerMetrics();
                previousKafkaMetricsUpdatedTimestamp = System.currentTimeMillis();
            }
            if (topologyReliabilityMode == Config.TopologyReliabilityMode.ATMOST_ONCE) {
                consumer.commitAsync();
            }
            return records;
        }
        return Collections.emptyList();
    }

    private void registerConsumerMetrics() {
        consumer.metrics().forEach((metricName, o) -> {
            if (!reportedMetrics.contains(metricName)) {
                reportedMetrics.add(metricName);
                String exposedName = extractKafkaMetricName(metricName);
                LOG.info("register Kakfa Consumer metric {}", exposedName);
                topologyContext.registerMetric(exposedName, new KafkaMetricDecorator<>(o), metricsIntervalInSecs);
            }
        });
    }

    private String extractKafkaMetricName(MetricName metricName) {
        StringBuilder builder = new StringBuilder()
                .append(metricName.name())
                .append('-')
                .append(metricName.group());
        metricName.tags().forEach((s, s2) -> builder.append('-')
                .append(s)
                .append('-')
                .append(s2));
        LOG.info("register Kakfa Consumer metric {}", builder);
        return builder.toString();
    }

    static class ConsumerRecordMessageId {
        private TopicPartition topicPartition;
        private long offset;

        ConsumerRecordMessageId(TopicPartition topicPartition, long offset) {
            this.topicPartition = topicPartition;
            this.offset = offset;
        }

        @Override
        public String toString() {
            return "ConsumerRecordMessageId{" +
                    "topicPartition=" + topicPartition +
                    ", offset=" + offset +
                    '}';
        }

        TopicPartition getTopicPartition() {
            return topicPartition;
        }

        long getOffset() {
            return offset;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ConsumerRecordMessageId that = (ConsumerRecordMessageId) o;

            if (offset != that.offset) return false;
            return topicPartition.equals(that.topicPartition);
        }

        @Override
        public int hashCode() {
            int result = topicPartition.hashCode();
            result = 31 * result + (int) (offset ^ (offset >>> 32));
            return result;
        }
    }

    class KafkaConsumerRebalanceListener implements ConsumerRebalanceListener {

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> collection) {
            assignedPartitions.removeAll(collection);
            if (topologyReliabilityMode == Config.TopologyReliabilityMode.ATLEAST_ONCE) {
                collection.forEach(topicPartition -> {
                    NavigableMap<Long, Long> navigableMap = ackRegistry.remove(topicPartition);
                    if (navigableMap != null) {
                        Map.Entry<Long, Long> entry = navigableMap.firstEntry();
                        if (entry != null) {
                            consumer.commitAsync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(Math.min(failureRegistry.getOrDefault(topicPartition, Long.MAX_VALUE), entry.getValue()) + 1)), null);
                        }
                    }
                    failureRegistry.remove(topicPartition);
                });
            } else if (topologyReliabilityMode == Config.TopologyReliabilityMode.EFFECTIVELY_ONCE) {
                collection.forEach(topicPartition -> state.remove(topicPartition));
            }
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> collection) {
            assignedPartitions.addAll(collection);
            if (topologyReliabilityMode == Config.TopologyReliabilityMode.ATLEAST_ONCE) {
                collection.forEach(topicPartition -> {
                    try {
                        long nextRecordPosition = consumer.position(topicPartition, Duration.ofSeconds(5));
                        ackRegistry.put(topicPartition, new ConcurrentSkipListMap<>(Collections.singletonMap(nextRecordPosition - 1, nextRecordPosition - 1)));
                    } catch (TimeoutException e) {
                        LOG.warn("can not get the position of the next record to consume for partition {}", topicPartition);
                        ackRegistry.remove(topicPartition);
                    }
                    failureRegistry.remove(topicPartition);
                });
            } else if (topologyReliabilityMode == Config.TopologyReliabilityMode.EFFECTIVELY_ONCE) {
                collection.forEach(topicPartition -> {
                    if (state.containsKey(topicPartition)) {
                        consumer.seek(topicPartition, state.get(topicPartition));
                    }
                });
            }
        }
    }
}
