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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.yahoo.pulsar.client.api.Consumer;
import com.yahoo.pulsar.client.api.ConsumerConfiguration;
import com.yahoo.pulsar.client.api.Message;
import com.yahoo.pulsar.client.api.MessageId;
import com.yahoo.pulsar.client.api.PulsarClient;
import com.yahoo.pulsar.client.api.PulsarClientException;
import com.yahoo.pulsar.client.api.SubscriptionType;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.twitter.heron.api.spout.SpoutOutputCollector;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Values;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;

public class PulsarSpoutTests {

  private static final String SERVICE_URL = "pulsar://broker:6650";
  private static final String TOPIC = "persistent://sample/standalone/ns1/my-topic";
  private static final String SUBSCRIPTION = "my-subscription";

  private Consumer mockConsumer;
  private MessageToValuesMapper mockMessageToValuesMapper;

  private final Map<String, Object> emptyConfig = new HashMap<>();
  private TopologyContext mockTopologyContext;
  private SpoutOutputCollector mockSpoutOutputCollector;

  @Before
  public void before() {
    mockMessageToValuesMapper = Mockito.mock(MessageToValuesMapper.class);
    mockTopologyContext = Mockito.mock(TopologyContext.class);
    mockSpoutOutputCollector = Mockito.mock(SpoutOutputCollector.class);
  }

  @Test(expected = NullPointerException.class)
  public void testMissingServiceUrl() {
    new PulsarSpout.Builder()
        .setTopic(TOPIC)
        .setSubscription(SUBSCRIPTION)
        .setMessageToValuesMapper(mockMessageToValuesMapper)
        .build();
  }

  @Test(expected = NullPointerException.class)
  public void testMissingTopic() {
    new PulsarSpout.Builder()
        .setServiceUrl(SERVICE_URL)
        .setSubscription(SUBSCRIPTION)
        .setMessageToValuesMapper(mockMessageToValuesMapper)
        .build();
  }

  @Test(expected = NullPointerException.class)
  public void testMissingSubscription() {
    new PulsarSpout.Builder()
        .setServiceUrl(SERVICE_URL)
        .setTopic(TOPIC)
        .setMessageToValuesMapper(mockMessageToValuesMapper)
        .build();
  }

  @Test(expected = NullPointerException.class)
  public void testMissingMessageToValuesMapper() {
    new PulsarSpout.Builder()
        .setServiceUrl(SERVICE_URL)
        .setTopic(TOPIC)
        .setSubscription(SUBSCRIPTION)
        .build();
  }

  @Test(expected = RuntimeException.class)
  public void testFailedToSubscribe() throws Exception {
    final PulsarSpout spout =
        create(createMessages().iterator(), true);
    spout.open(emptyConfig, mockTopologyContext, mockSpoutOutputCollector);
  }

  @Test
  public void testNextTupleEmitted() throws PulsarClientException {
    final Message message = createMockMessage();
    final PulsarSpout spout =
        create(createMessages(message).iterator(), false);

    final Values values = Mockito.mock(Values.class);
    Mockito.when(mockMessageToValuesMapper.toValues(any(Message.class)))
        .thenReturn(values);

    spout.open(emptyConfig, mockTopologyContext, mockSpoutOutputCollector);

    spout.nextTuple();
    Mockito.verify(mockSpoutOutputCollector, Mockito.times(1))
        .emit(values, message.getMessageId());
  }

  @Test
  public void testAcknowledgeAsyncCalledOnAck() throws PulsarClientException {
    final Message message = createMockMessage();
    final PulsarSpout spout =
        create(createMessages(message).iterator(), false);
    spout.open(emptyConfig, mockTopologyContext, mockSpoutOutputCollector);

    spout.ack(message.getMessageId());
    Mockito.verify(mockConsumer, Mockito.times(1)).acknowledgeAsync(message.getMessageId());
  }

  @Test
  public void testDefaultConsumerConfiguration() {
    final ConsumerConfiguration configuration = PulsarSpout.getDefaultConsumerConfiguration();
    assertEquals(configuration.getSubscriptionType(), SubscriptionType.Shared);
    assertEquals(configuration.getAckTimeoutMillis(), 60000);
  }

  private PulsarSpout create(final Iterator<Message> messages,
      final boolean failToSubscribe)
      throws PulsarClientException {


    final PulsarClient client = Mockito.mock(PulsarClient.class);
    final Answer<Consumer> answer = new Answer<Consumer>() {
      @Override
      public Consumer answer(InvocationOnMock invocationOnMock) throws Throwable {
        if (failToSubscribe) {
          throw new PulsarClientException("Failed to subscribe");
        }
        mockConsumer = createConsumer(messages);
        return mockConsumer;
      }
    };
    Mockito.when(client.subscribe(anyString(), anyString(),
        any(ConsumerConfiguration.class))).then(answer);

    final PulsarSpout spout =
        Mockito.spy(new PulsarSpout.Builder()
        .setServiceUrl(SERVICE_URL)
        .setTopic(TOPIC)
        .setSubscription(SUBSCRIPTION)
        .setMessageToValuesMapper(mockMessageToValuesMapper)
        .build());

    Mockito.doReturn(client).when(spout).createClient();

    return spout;
  }

  private Consumer createConsumer(final Iterator<Message> messages)
      throws PulsarClientException {
    final Consumer consumer = Mockito.mock(Consumer.class);

    final Answer<Message> answer = new Answer<Message>() {
      @Override
      public Message answer(InvocationOnMock invocationOnMock) throws Throwable {
        if (messages.hasNext()) {
          return messages.next();
        }
        throw new PulsarClientException("No more messages");
      }
    };
    Mockito.when(consumer.receive(anyInt(), any(TimeUnit.class))).then(answer);

    return consumer;
  }

  private List<Message> createMessages(Message... msgs) {
    final List<Message> messages = new ArrayList<>();
    if (msgs != null) {
      for (Message message : msgs) {
        messages.add(message);
      }
    }

    return messages;
  }

  private Message createMockMessage() {
    final Message message = Mockito.mock(Message.class);
    Mockito.when(message.getData()).thenReturn(new byte[]{});
    Mockito.when(message.getMessageId()).thenReturn(Mockito.mock(MessageId.class));

    return message;
  }
}
