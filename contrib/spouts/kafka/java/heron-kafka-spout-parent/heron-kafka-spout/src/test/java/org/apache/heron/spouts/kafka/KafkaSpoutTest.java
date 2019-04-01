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
import com.twitter.heron.api.metric.IMetric;
import com.twitter.heron.api.spout.SpoutOutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.common.basics.SingletonRegistry;
import com.twitter.heron.common.config.SystemConfig;
import com.twitter.heron.common.config.SystemConfigKey;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.*;
import java.util.regex.Pattern;

import static com.twitter.heron.api.Config.TopologyReliabilityMode.ATLEAST_ONCE;
import static com.twitter.heron.api.Config.TopologyReliabilityMode.ATMOST_ONCE;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class KafkaSpoutTest {
    private static final Random random = new Random();
    private static final String DUMMY_TOPIC_NAME = "topic";
    private KafkaSpout<String, byte[]> kafkaSpout;
    @Mock
    private KafkaConsumerFactory<String, byte[]> kafkaConsumerFactory;
    @Mock
    private Consumer<String, byte[]> consumer;
    @Mock
    private TopologyContext topologyContext;
    @Mock
    private SpoutOutputCollector collector;
    @Mock
    private Metric metric;
    @Captor
    private ArgumentCaptor<Pattern> patternArgumentCaptor;
    @Captor
    private ArgumentCaptor<IMetric<Object>> kafkaMetricDecoratorArgumentCaptor;
    @Mock
    private OutputFieldsDeclarer declarer;
    @Captor
    private ArgumentCaptor<Fields> fieldsArgumentCaptor;
    @Captor
    private ArgumentCaptor<List<Object>> listArgumentCaptor;
    @Captor
    private ArgumentCaptor<ConsumerRebalanceListener> consumerRebalanceListenerArgumentCaptor;

    @BeforeAll
    static void setUpAll() {
        if (!SingletonRegistry.INSTANCE.containsSingleton(SystemConfig.HERON_SYSTEM_CONFIG)) {
            SingletonRegistry.INSTANCE.registerSingleton(SystemConfig.HERON_SYSTEM_CONFIG, SystemConfig.newBuilder(true)
                    .put(SystemConfigKey.HERON_METRICS_EXPORT_INTERVAL, 60)
                    .build());
        }
    }

    @BeforeEach
    void setUp() {
        kafkaSpout = new KafkaSpout<>(kafkaConsumerFactory, Collections.singleton(DUMMY_TOPIC_NAME));
    }

    @Test
    void getConsumerRecordTransformer() {
        assertTrue(kafkaSpout.getConsumerRecordTransformer() instanceof DefaultConsumerRecordTransformer);

    }

    @Test
    void setConsumerRecordTransformer() {
        ConsumerRecordTransformer<String, byte[]> consumerRecordTransformer = new DefaultConsumerRecordTransformer<>();
        kafkaSpout.setConsumerRecordTransformer(consumerRecordTransformer);
        assertEquals(consumerRecordTransformer, kafkaSpout.getConsumerRecordTransformer());
    }

    @Test
    void open() {
        when(kafkaConsumerFactory.create()).thenReturn(consumer);

        kafkaSpout.open(Collections.singletonMap(Config.TOPOLOGY_RELIABILITY_MODE, ATMOST_ONCE.name()), topologyContext, collector);
        verify(consumer).subscribe(eq(Collections.singleton(DUMMY_TOPIC_NAME)), any(KafkaSpout.KafkaConsumerRebalanceListener.class));

        kafkaSpout = new KafkaSpout<>(kafkaConsumerFactory, new DefaultTopicPatternProvider("a"));
        kafkaSpout.open(Collections.singletonMap(Config.TOPOLOGY_RELIABILITY_MODE, ATMOST_ONCE.name()), topologyContext, collector);
        verify(consumer).subscribe(patternArgumentCaptor.capture(), any(KafkaSpout.KafkaConsumerRebalanceListener.class));
        assertEquals("a", patternArgumentCaptor.getValue().pattern());
    }

    @Test
    void nextTuple() {
        when(kafkaConsumerFactory.create()).thenReturn(consumer);
        ConsumerRecords<String, byte[]> consumerRecords = new ConsumerRecords<>(Collections.singletonMap(new TopicPartition(DUMMY_TOPIC_NAME, 0), Collections.singletonList(new ConsumerRecord<>(DUMMY_TOPIC_NAME, 0, 0, "key", new byte[]{0xF}))));
        when(consumer.poll(any(Duration.class))).thenReturn(consumerRecords);
        doReturn(Collections.singletonMap(new MetricName("name", "group", "description", Collections.singletonMap("name", "value")), metric)).when(consumer).metrics();
        when(metric.metricValue()).thenReturn("sample value");

        kafkaSpout.open(Collections.singletonMap(Config.TOPOLOGY_RELIABILITY_MODE, ATMOST_ONCE.name()), topologyContext, collector);
        verify(consumer).subscribe(eq(Collections.singleton(DUMMY_TOPIC_NAME)), consumerRebalanceListenerArgumentCaptor.capture());
        ConsumerRebalanceListener consumerRebalanceListener = consumerRebalanceListenerArgumentCaptor.getValue();
        TopicPartition topicPartition = new TopicPartition(DUMMY_TOPIC_NAME, 0);
        consumerRebalanceListener.onPartitionsAssigned(Collections.singleton(topicPartition));

        kafkaSpout.nextTuple();
        verify(consumer).commitAsync();
        verify(topologyContext).registerMetric(eq("name-group-name-value"), kafkaMetricDecoratorArgumentCaptor.capture(), eq(60));
        assertEquals("sample value", kafkaMetricDecoratorArgumentCaptor.getValue().getValueAndReset());

        kafkaSpout.nextTuple();
        verify(collector).emit(eq("default"), listArgumentCaptor.capture());
        assertEquals("key", listArgumentCaptor.getValue().get(0));
        assertArrayEquals(new byte[]{0xF}, (byte[]) listArgumentCaptor.getValue().get(1));
    }

    @Test
    void ack() {
        when(kafkaConsumerFactory.create()).thenReturn(consumer);
        TopicPartition topicPartition = new TopicPartition(DUMMY_TOPIC_NAME, 0);
        List<ConsumerRecord<String, byte[]>> recordList = new ArrayList<>();
        byte[] randomBytes = new byte[1];
        for (int i = 0; i < 5; i++) {
            random.nextBytes(randomBytes);
            recordList.add(new ConsumerRecord<>(DUMMY_TOPIC_NAME, 0, i, "key", Arrays.copyOf(randomBytes, randomBytes.length)));
        }
        ConsumerRecords<String, byte[]> consumerRecords = new ConsumerRecords<>(Collections.singletonMap(topicPartition, recordList));
        when(consumer.poll(any(Duration.class))).thenReturn(consumerRecords);

        kafkaSpout.open(Collections.singletonMap(Config.TOPOLOGY_RELIABILITY_MODE, ATLEAST_ONCE.name()), topologyContext, collector);
        //poll the topic
        kafkaSpout.nextTuple();
        //emit all of the five records
        for (int i = 0; i < 5; i++) {
            kafkaSpout.nextTuple();
        }
        //ack came in out of order and the third record is not acknowledged
        kafkaSpout.ack(new KafkaSpout.ConsumerRecordMessageId(topicPartition, 4));
        kafkaSpout.ack(new KafkaSpout.ConsumerRecordMessageId(topicPartition, 0));
        kafkaSpout.ack(new KafkaSpout.ConsumerRecordMessageId(topicPartition, 1));
        kafkaSpout.ack(new KafkaSpout.ConsumerRecordMessageId(topicPartition, 3));
        //commit and poll
        kafkaSpout.nextTuple();
        verify(consumer).commitAsync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(2)), null);
    }

    @Test
    void fail() {
        when(kafkaConsumerFactory.create()).thenReturn(consumer);
        TopicPartition topicPartition = new TopicPartition(DUMMY_TOPIC_NAME, 0);
        List<ConsumerRecord<String, byte[]>> recordList = new ArrayList<>();
        byte[] randomBytes = new byte[1];
        for (int i = 0; i < 5; i++) {
            random.nextBytes(randomBytes);
            recordList.add(new ConsumerRecord<>(DUMMY_TOPIC_NAME, 0, i, "key", Arrays.copyOf(randomBytes, randomBytes.length)));
        }
        ConsumerRecords<String, byte[]> consumerRecords = new ConsumerRecords<>(Collections.singletonMap(topicPartition, recordList));
        when(consumer.poll(any(Duration.class))).thenReturn(consumerRecords);

        kafkaSpout.open(Collections.singletonMap(Config.TOPOLOGY_RELIABILITY_MODE, ATLEAST_ONCE.name()), topologyContext, collector);
        //poll the topic
        kafkaSpout.nextTuple();
        //emit all of the five records
        for (int i = 0; i < 5; i++) {
            kafkaSpout.nextTuple();
        }
        //ack came in out of order, second and third record fails
        kafkaSpout.ack(new KafkaSpout.ConsumerRecordMessageId(topicPartition, 4));
        kafkaSpout.ack(new KafkaSpout.ConsumerRecordMessageId(topicPartition, 0));
        kafkaSpout.fail(new KafkaSpout.ConsumerRecordMessageId(topicPartition, 1));
        kafkaSpout.ack(new KafkaSpout.ConsumerRecordMessageId(topicPartition, 3));
        kafkaSpout.fail(new KafkaSpout.ConsumerRecordMessageId(topicPartition, 2));
        //commit and poll
        kafkaSpout.nextTuple();
        verify(consumer).seek(topicPartition, 1);
        verify(consumer).commitAsync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(1)), null);
    }

    @Test
    void close() {
        when(kafkaConsumerFactory.create()).thenReturn(consumer);
        kafkaSpout.open(Collections.singletonMap(Config.TOPOLOGY_RELIABILITY_MODE, ATMOST_ONCE.name()), topologyContext, collector);
        kafkaSpout.close();
        verify(consumer).close();
    }

    @Test
    void declareOutputFields() {
        kafkaSpout.declareOutputFields(declarer);
        verify(declarer).declareStream(eq("default"), fieldsArgumentCaptor.capture());
        assertEquals(Arrays.asList("key", "value"), fieldsArgumentCaptor.getValue().toList());
    }

    @Test
    void consumerRebalanceListener() {
        when(kafkaConsumerFactory.create()).thenReturn(consumer);

        kafkaSpout.open(Collections.singletonMap(Config.TOPOLOGY_RELIABILITY_MODE, ATLEAST_ONCE.name()), topologyContext, collector);
        verify(consumer).subscribe(eq(Collections.singleton(DUMMY_TOPIC_NAME)), consumerRebalanceListenerArgumentCaptor.capture());
        ConsumerRebalanceListener consumerRebalanceListener = consumerRebalanceListenerArgumentCaptor.getValue();
        TopicPartition topicPartition = new TopicPartition(DUMMY_TOPIC_NAME, 0);
        consumerRebalanceListener.onPartitionsAssigned(Collections.singleton(topicPartition));
        verify(consumer).position(topicPartition, Duration.ofSeconds(5));

        kafkaSpout.ack(new KafkaSpout.ConsumerRecordMessageId(topicPartition, 0));
        kafkaSpout.ack(new KafkaSpout.ConsumerRecordMessageId(topicPartition, 1));
        consumerRebalanceListener.onPartitionsRevoked(Collections.singleton(topicPartition));
        verify(consumer).commitAsync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(2)), null);
    }

    @Test
    void activate() {
        when(kafkaConsumerFactory.create()).thenReturn(consumer);
        kafkaSpout.open(Collections.singletonMap(Config.TOPOLOGY_RELIABILITY_MODE, ATMOST_ONCE.name()), topologyContext, collector);
        verify(consumer).subscribe(eq(Collections.singleton(DUMMY_TOPIC_NAME)), consumerRebalanceListenerArgumentCaptor.capture());
        ConsumerRebalanceListener consumerRebalanceListener = consumerRebalanceListenerArgumentCaptor.getValue();
        TopicPartition topicPartition = new TopicPartition(DUMMY_TOPIC_NAME, 0);
        consumerRebalanceListener.onPartitionsAssigned(Collections.singleton(topicPartition));
        kafkaSpout.activate();
        verify(consumer).resume(Collections.singleton(topicPartition));
    }

    @Test
    void deactivate() {
        when(kafkaConsumerFactory.create()).thenReturn(consumer);
        kafkaSpout.open(Collections.singletonMap(Config.TOPOLOGY_RELIABILITY_MODE, ATMOST_ONCE.name()), topologyContext, collector);
        verify(consumer).subscribe(eq(Collections.singleton(DUMMY_TOPIC_NAME)), consumerRebalanceListenerArgumentCaptor.capture());
        ConsumerRebalanceListener consumerRebalanceListener = consumerRebalanceListenerArgumentCaptor.getValue();
        TopicPartition topicPartition = new TopicPartition(DUMMY_TOPIC_NAME, 0);
        consumerRebalanceListener.onPartitionsAssigned(Collections.singleton(topicPartition));
        kafkaSpout.deactivate();
        verify(consumer).pause(Collections.singleton(topicPartition));
    }
}