package org.apache.heron.spouts.kafka;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.spout.BaseRichSpout;
import com.twitter.heron.api.spout.SpoutOutputCollector;
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

public class KafkaSpout<K, V> extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSpout.class);
    private static final long serialVersionUID = -2271355516537883361L;
    private KafkaConsumerFactory<K, V> kafkaConsumerFactory;
    private TopicPatternProvider topicPatternProvider;
    private Collection<String> topicNames;
    private ConsumerRecordTransformer<K, V> consumerRecordTransformer = new DefaultConsumerRecordTransformer<>();
    private transient SpoutOutputCollector collector;
    private transient TopologyContext topologyContext;
    private transient Queue<ConsumerRecord<K, V>> buffer;
    private transient Consumer<K, V> consumer;
    private transient Set<TopicPartition> assignedPartitions;
    private transient Map<TopicPartition, NavigableMap<Long, Long>> ackRegistry;
    private transient Map<TopicPartition, Long> failureRegistry;
    private Config.TopologyReliabilityMode topologyReliabilityMode = Config.TopologyReliabilityMode.ATMOST_ONCE;
    private boolean isConsumerMetricsOutOfDate = false;

    @SuppressWarnings("WeakerAccess")
    public KafkaSpout(KafkaConsumerFactory<K, V> kafkaConsumerFactory, Collection<String> topicNames) {
        this.kafkaConsumerFactory = kafkaConsumerFactory;
        this.topicNames = topicNames;
    }

    @SuppressWarnings("WeakerAccess")
    public KafkaSpout(KafkaConsumerFactory<K, V> kafkaConsumerFactory, TopicPatternProvider topicPatternProvider) {
        this.kafkaConsumerFactory = kafkaConsumerFactory;
        this.topicPatternProvider = topicPatternProvider;
    }

    @SuppressWarnings("WeakerAccess")
    public ConsumerRecordTransformer<K, V> getConsumerRecordTransformer() {
        return consumerRecordTransformer;
    }

    @SuppressWarnings("WeakerAccess")
    public void setConsumerRecordTransformer(ConsumerRecordTransformer<K, V> consumerRecordTransformer) {
        this.consumerRecordTransformer = consumerRecordTransformer;
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.topologyContext = context;
        this.topologyReliabilityMode = Config.TopologyReliabilityMode.valueOf(conf.get(Config.TOPOLOGY_RELIABILITY_MODE).toString());
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
    }

    @Override
    public void nextTuple() {
        ConsumerRecord<K, V> record = buffer.poll();
        if (record != null) {
            // there are still records remaining for emission from the previous poll
            emitConsumerRecord(record);
        } else {
            //all the records from previous poll have been emitted or this is very first time to poll
            if (topologyReliabilityMode != Config.TopologyReliabilityMode.ATMOST_ONCE) {
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
        declarer.declareStream(consumerRecordTransformer.getOutputStream(), new Fields(consumerRecordTransformer.getFieldNames()));
    }

    private void emitConsumerRecord(ConsumerRecord<K, V> record) {
        List<Object> values = consumerRecordTransformer.transform(record);
        String streamId = consumerRecordTransformer.getOutputStream();
        if (topologyReliabilityMode == Config.TopologyReliabilityMode.ATMOST_ONCE) {
            collector.emit(streamId, values);
        } else {
            //build message id based on topic, partition, offset of the consumer record
            ConsumerRecordMessageId consumerRecordMessageId = new ConsumerRecordMessageId(new TopicPartition(record.topic(), record.partition()), record.offset());
            //emit tuple with the message id
            collector.emit(streamId, values, consumerRecordMessageId);
        }
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
            if (isConsumerMetricsOutOfDate) {
                registerConsumerMetrics();
                isConsumerMetricsOutOfDate = false;
            }
            if (topologyReliabilityMode == Config.TopologyReliabilityMode.ATMOST_ONCE) {
                consumer.commitAsync();
            }
            return records;
        }
        return Collections.emptyList();
    }

    private void registerConsumerMetrics() {
        SystemConfig systemConfig = (SystemConfig) SingletonRegistry.INSTANCE.getSingleton(SystemConfig.HERON_SYSTEM_CONFIG);
        int interval = (int) systemConfig.getHeronMetricsExportInterval().getSeconds();
        consumer.metrics().forEach((metricName, o) -> topologyContext.registerMetric(extractKafkaMetricName(metricName), new KafkaMetricDecorator<>(o), interval));
    }

    private String extractKafkaMetricName(MetricName metricName) {
        StringBuilder builder = new StringBuilder()
                .append(metricName.name())
                .append('-')
                .append(metricName.group())
                .append('-');
        metricName.tags().forEach((s, s2) -> builder.append(s)
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
            isConsumerMetricsOutOfDate = true;
            assignedPartitions.removeAll(collection);
            if (topologyReliabilityMode != Config.TopologyReliabilityMode.ATMOST_ONCE) {
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
            }
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> collection) {
            isConsumerMetricsOutOfDate = true;
            assignedPartitions.addAll(collection);
            if (topologyReliabilityMode != Config.TopologyReliabilityMode.ATMOST_ONCE) {
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
            }
        }
    }
}
