//  Copyright 2018 Twitter. All rights reserved.
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
package com.twitter.heron.eco.builder;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.twitter.heron.eco.definition.EcoExecutionContext;
import com.twitter.heron.eco.definition.EcoTopologyDefinition;
import com.twitter.heron.eco.definition.GroupingDefinition;
import com.twitter.heron.eco.definition.StreamDefinition;

import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class StreamBuilderTest {

  @Mock
  private EcoTopologyDefinition mockDefinition;
  @Mock
  private EcoExecutionContext mockContext;
  @Mock
  private TopologyBuilder mockTopologyBuilder;
  @Mock
  private ObjectBuilder mockObjectBuilder;
  @Mock
  private BoltDeclarer mockBoltDeclarer;

  private StreamBuilder subject;

  @Before
  public void setUpForEachTestCase() {
    subject = new StreamBuilder();
  }

  @After
  public void ensureNoUnexpectedMockInteractions() {
    Mockito.verifyNoMoreInteractions(mockDefinition,
        mockContext,
        mockTopologyBuilder,
        mockObjectBuilder,
        mockBoltDeclarer);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void buildStreams_SpoutToIRichBolt() throws ClassNotFoundException,
      InvocationTargetException, NoSuchFieldException,
      InstantiationException, IllegalAccessException {
    final int iRichBoltParallelism = 1;
    final String to = "to";
    final String from = "from";
    final String streamId  = "id";
    StreamDefinition streamDefinition  = new StreamDefinition();
    streamDefinition.setFrom(from);
    streamDefinition.setTo(to);
    streamDefinition.setId(streamId);
    List<StreamDefinition> streams = new ArrayList<>();
    streams.add(streamDefinition);
    GroupingDefinition groupingDefinition = new GroupingDefinition();
    groupingDefinition.setType(GroupingDefinition.Type.SHUFFLE);
    groupingDefinition.setStreamId(streamId);
    streamDefinition.setGrouping(groupingDefinition);
    MockIRichBolt mockIRichBolt = new MockIRichBolt();

    when(mockContext.getTopologyDefinition()).thenReturn(mockDefinition);
    when(mockContext.getBolt(eq(to))).thenReturn(mockIRichBolt);
    when(mockDefinition.getStreams()).thenReturn(streams);
    when(mockDefinition.parallelismForBolt(eq(to))).thenReturn(iRichBoltParallelism);
    when(mockTopologyBuilder.setBolt(eq(to),
        eq(mockIRichBolt), eq(iRichBoltParallelism))).thenReturn(mockBoltDeclarer);

    subject.buildStreams(mockContext, mockTopologyBuilder, mockObjectBuilder );

    verify(mockContext).getTopologyDefinition();
    verify(mockContext).getBolt(eq(to));
    verify(mockDefinition).parallelismForBolt(eq(to));
    verify(mockTopologyBuilder).setBolt(eq(to), eq(mockIRichBolt), eq(iRichBoltParallelism));
    verify(mockBoltDeclarer).shuffleGrouping(eq(from), eq(streamId));
    verify(mockContext).setStreams(anyMap());
    verify(mockDefinition).getStreams();
  }


  @SuppressWarnings({"rawtypes", "unchecked", "serial"})
  private class MockIRichBolt implements IRichBolt {

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

    }

    @Override
    public void execute(Tuple input) {

    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
      return null;
    }
  }
}