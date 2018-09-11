/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.heron.eco.builder.storm;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import org.apache.heron.eco.builder.ObjectBuilder;
import org.apache.heron.eco.definition.EcoExecutionContext;
import org.apache.heron.eco.definition.EcoTopologyDefinition;
import org.apache.heron.eco.definition.GroupingDefinition;
import org.apache.heron.eco.definition.ObjectDefinition;
import org.apache.heron.eco.definition.StreamDefinition;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IWindowedBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TimestampExtractor;
import org.apache.storm.windowing.TupleWindow;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class StormStreamBuilderTest {

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
  public void buildStreams_SpoutToIRichBolt_ShuffleGrouping() throws ClassNotFoundException,
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

    subject.buildStreams(mockContext, mockTopologyBuilder, mockObjectBuilder);

    verify(mockContext).getTopologyDefinition();
    verify(mockContext).getBolt(eq(to));
    verify(mockDefinition).parallelismForBolt(eq(to));
    verify(mockTopologyBuilder).setBolt(eq(to), eq(mockIRichBolt), eq(iRichBoltParallelism));
    verify(mockBoltDeclarer).shuffleGrouping(eq(from), eq(streamId));
    verify(mockContext).setStreams(anyMap());
    verify(mockDefinition).getStreams();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void buildStreams_SpoutToIBasicBolt_FieldsGroupingWithArgs() throws
      ClassNotFoundException,
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
    groupingDefinition.setType(GroupingDefinition.Type.FIELDS);
    List<String> args = new ArrayList<>();
    args.add("arg1");
    groupingDefinition.setArgs(args);
    groupingDefinition.setStreamId(streamId);
    streamDefinition.setGrouping(groupingDefinition);
    MockIBasicBolt mockIBasicBolt = new MockIBasicBolt();

    when(mockContext.getTopologyDefinition()).thenReturn(mockDefinition);
    when(mockContext.getBolt(eq(to))).thenReturn(mockIBasicBolt);
    when(mockDefinition.getStreams()).thenReturn(streams);
    when(mockDefinition.parallelismForBolt(eq(to))).thenReturn(iRichBoltParallelism);
    when(mockTopologyBuilder.setBolt(eq(to),
        eq(mockIBasicBolt), eq(iRichBoltParallelism))).thenReturn(mockBoltDeclarer);

    subject.buildStreams(mockContext, mockTopologyBuilder, mockObjectBuilder);

    verify(mockContext).getTopologyDefinition();
    verify(mockContext).getBolt(eq(to));
    verify(mockDefinition).parallelismForBolt(eq(to));
    verify(mockTopologyBuilder).setBolt(eq(to), eq(mockIBasicBolt), eq(iRichBoltParallelism));
    verify(mockBoltDeclarer).fieldsGrouping(eq(from), eq(streamId), any(Fields.class));
    verify(mockContext).setStreams(anyMap());
    verify(mockDefinition).getStreams();
  }

  @Test(expected = IllegalArgumentException.class)
  @SuppressWarnings("unchecked")
  public void buildStreams_SpoutToIBasicBolt_FieldsGroupingWithoutArgs_ExceptionThrown() throws
      ClassNotFoundException,
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
    groupingDefinition.setType(GroupingDefinition.Type.FIELDS);

    groupingDefinition.setStreamId(streamId);
    streamDefinition.setGrouping(groupingDefinition);
    MockIBasicBolt mockIBasicBolt = new MockIBasicBolt();
    try {
      when(mockContext.getTopologyDefinition()).thenReturn(mockDefinition);
      when(mockContext.getBolt(eq(to))).thenReturn(mockIBasicBolt);
      when(mockDefinition.getStreams()).thenReturn(streams);
      when(mockDefinition.parallelismForBolt(eq(to))).thenReturn(iRichBoltParallelism);
      when(mockTopologyBuilder.setBolt(eq(to),
          eq(mockIBasicBolt), eq(iRichBoltParallelism))).thenReturn(mockBoltDeclarer);

      subject.buildStreams(mockContext, mockTopologyBuilder, mockObjectBuilder);
    } finally {

      verify(mockContext).getTopologyDefinition();
      verify(mockContext).getBolt(eq(to));
      verify(mockDefinition).parallelismForBolt(eq(to));
      verify(mockTopologyBuilder).setBolt(eq(to), eq(mockIBasicBolt), eq(iRichBoltParallelism));
      verify(mockDefinition).getStreams();
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void buildStreams_SpoutToIWindowedBolt_CustomGrouping() throws ClassNotFoundException,
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
    groupingDefinition.setType(GroupingDefinition.Type.CUSTOM);
    MockCustomObjectDefinition mockCustomObjectDefinition = new MockCustomObjectDefinition();

    groupingDefinition.setCustomClass(mockCustomObjectDefinition);
    List<String> args = new ArrayList<>();
    args.add("arg1");
    groupingDefinition.setArgs(args);
    groupingDefinition.setStreamId(streamId);
    streamDefinition.setGrouping(groupingDefinition);
    MockIWindowedBolt mockIWindowedBolt = new MockIWindowedBolt();
    MockCustomStreamGrouping mockCustomStreamGrouping = new MockCustomStreamGrouping();

    when(mockContext.getTopologyDefinition()).thenReturn(mockDefinition);
    when(mockContext.getBolt(eq(to))).thenReturn(mockIWindowedBolt);
    when(mockDefinition.getStreams()).thenReturn(streams);
    when(mockDefinition.parallelismForBolt(eq(to))).thenReturn(iRichBoltParallelism);
    when(mockTopologyBuilder.setBolt(eq(to),
        eq(mockIWindowedBolt), eq(iRichBoltParallelism))).thenReturn(mockBoltDeclarer);
    when(mockObjectBuilder.buildObject(eq(mockCustomObjectDefinition),
        eq(mockContext))).thenReturn(mockCustomStreamGrouping);

    subject.buildStreams(mockContext, mockTopologyBuilder, mockObjectBuilder);

    verify(mockContext).getTopologyDefinition();
    verify(mockContext).getBolt(eq(to));
    verify(mockDefinition).parallelismForBolt(eq(to));
    verify(mockTopologyBuilder).setBolt(eq(to), eq(mockIWindowedBolt), eq(iRichBoltParallelism));
    verify(mockBoltDeclarer).customGrouping(eq(from), eq(streamId), eq(mockCustomStreamGrouping));
    verify(mockContext).setStreams(anyMap());
    verify(mockDefinition).getStreams();
    verify(mockObjectBuilder).buildObject(same(mockCustomObjectDefinition), same(mockContext));
  }

  private class MockCustomObjectDefinition extends ObjectDefinition {

  }

  @SuppressWarnings("serial")
  private class MockCustomStreamGrouping implements CustomStreamGrouping {

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream,
                        List<Integer> targetTasks) {

    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
      return null;
    }
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

  @SuppressWarnings({"rawtypes", "unchecked", "serial"})
  private class MockIWindowedBolt implements IWindowedBolt {
    @Override
    public void prepare(Map topoConf,
                        TopologyContext context, OutputCollector collector) {

    }

    @Override
    public void execute(TupleWindow inputWindow) {

    }

    @Override
    public void cleanup() {

    }

    @Override
    public TimestampExtractor getTimestampExtractor() {
      return null;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
      return null;
    }
  }


  @SuppressWarnings({"rawtypes", "unchecked", "serial"})
  public class MockIBasicBolt implements IBasicBolt {
    @Override
    public void prepare(Map stormConf, TopologyContext context) {

    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {

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
