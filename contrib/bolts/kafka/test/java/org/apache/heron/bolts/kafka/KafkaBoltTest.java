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
package org.apache.heron.bolts.kafka;

import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import org.apache.heron.api.Config;
import org.apache.heron.api.bolt.OutputCollector;
import org.apache.heron.api.tuple.Tuple;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;

import static org.apache.heron.api.Config.TopologyReliabilityMode.ATLEAST_ONCE;
import static org.apache.heron.api.Config.TopologyReliabilityMode.ATMOST_ONCE;
import static org.apache.heron.api.Config.TopologyReliabilityMode.EFFECTIVELY_ONCE;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KafkaBoltTest {
  @Mock
  private KafkaProducerFactory<String, byte[]> kafkaProducerFactory;
  @Mock
  private Producer<String, byte[]> producer;
  @Mock
  private TupleTransformer<String, byte[]> tupleTransformer;
  @Mock
  private OutputCollector outputCollector;
  @Mock
  private Tuple tuple;
  @Mock
  private Future<RecordMetadata> future;
  private KafkaBolt<String, byte[]> kafkaBolt;

  @Before
  public void setUp() {
    when(kafkaProducerFactory.create()).thenReturn(producer);
    kafkaBolt = new KafkaBolt<>(kafkaProducerFactory, tupleTransformer);
  }

  @Test
  public void cleanup() {
    kafkaBolt.prepare(Collections.emptyMap(), null, outputCollector);
    kafkaBolt.cleanup();
    verify(producer).close();
  }

  @Test
  public void executeATLEASTONCE() {
    kafkaBolt.prepare(Collections.singletonMap(Config.TOPOLOGY_RELIABILITY_MODE, ATLEAST_ONCE),
        null, outputCollector);
    when(tupleTransformer.transformToKey(tuple)).thenReturn("key");
    byte[] value = new byte[]{1, 2, 3};
    when(tupleTransformer.transformToValue(tuple)).thenReturn(value);
    when(tupleTransformer.getTopicName(tuple)).thenReturn("topic");

    ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>("topic", "key", value);
    when(producer.send(eq(producerRecord), any(Callback.class)))
        .then((Answer<Future<RecordMetadata>>) invocationOnMock -> {
          invocationOnMock.getArgumentAt(1, Callback.class)
              .onCompletion(new RecordMetadata(null, 0, 0, 0, null, 0, 0), null);
          return future;
        });
    kafkaBolt.execute(tuple);
    verify(outputCollector).ack(tuple);

    when(producer.send(eq(producerRecord), any(Callback.class)))
        .then((Answer<Future<RecordMetadata>>) invocationOnMock -> {
          invocationOnMock.getArgumentAt(1, Callback.class)
              .onCompletion(new RecordMetadata(null, 0, 0, 0, null, 0, 0), new Exception());
          return future;
        });
    kafkaBolt.execute(tuple);
    verify(outputCollector).fail(tuple);
  }

  @Test(expected = KafkaException.class)
  public void executeEFFECTIVEONCE() throws ExecutionException, InterruptedException {
    kafkaBolt.prepare(Collections.singletonMap(Config.TOPOLOGY_RELIABILITY_MODE, EFFECTIVELY_ONCE),
        null, outputCollector);
    when(tupleTransformer.transformToKey(tuple)).thenReturn("key");
    byte[] value = new byte[]{1, 2, 3};
    when(tupleTransformer.transformToValue(tuple)).thenReturn(value);
    when(tupleTransformer.getTopicName(tuple)).thenReturn("topic");

    ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>("topic", "key", value);
    when(producer.send(producerRecord)).thenReturn(future);
    kafkaBolt.execute(tuple);
    verify(future).get();

    when(future.get()).thenThrow(ExecutionException.class);
    kafkaBolt.execute(tuple);
  }

  @Test
  public void executeATMOSTONCE() {
    kafkaBolt.prepare(Collections.singletonMap(Config.TOPOLOGY_RELIABILITY_MODE, ATMOST_ONCE),
        null, outputCollector);
    when(tupleTransformer.transformToKey(tuple)).thenReturn("key");
    byte[] value = new byte[]{1, 2, 3};
    when(tupleTransformer.transformToValue(tuple)).thenReturn(value);
    when(tupleTransformer.getTopicName(tuple)).thenReturn("topic");

    ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>("topic", "key", value);
    kafkaBolt.execute(tuple);
    verify(producer).send(eq(producerRecord), any(Callback.class));
  }
}
