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
import org.apache.heron.eco.definition.SpoutDefinition;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class StormSpoutBuilderTest {

  @Mock
  private EcoExecutionContext mockContext;
  @Mock
  private TopologyBuilder mockTopologyBuilder;
  @Mock
  private ObjectBuilder mockObjectBuilder;

  private SpoutBuilder subject;

  @Before
  public void setUpForEachTestCase() {
    subject = new SpoutBuilder();
  }

  @After
  public void ensureNoUnexpectedMockInteractions() {
    Mockito.verifyNoMoreInteractions(mockContext,
        mockTopologyBuilder,
        mockObjectBuilder);
  }

  @Test
  public void testBuildSpouts_AllGood_BehavesAsExpected() throws ClassNotFoundException,
      InvocationTargetException, NoSuchFieldException,
      InstantiationException, IllegalAccessException {
    EcoTopologyDefinition topologyDefinition = new EcoTopologyDefinition();

    SpoutDefinition spoutDefinition = new SpoutDefinition();
    final String id = "id";
    final int parallelism = 2;
    spoutDefinition.setId(id);
    spoutDefinition.setParallelism(parallelism);
    SpoutDefinition spoutDefinition1 = new SpoutDefinition();
    final String id1 = "id1";
    final int parallelism1 = 3;
    spoutDefinition1.setId(id1);
    spoutDefinition1.setParallelism(parallelism1);
    List<SpoutDefinition> spoutDefinitions = new ArrayList<>();
    spoutDefinitions.add(spoutDefinition);
    spoutDefinitions.add(spoutDefinition1);
    topologyDefinition.setSpouts(spoutDefinitions);
    MockSpout mockSpout = new MockSpout();
    MockSpout mockSpout1 = new MockSpout();

    when(mockObjectBuilder.buildObject(eq(spoutDefinition),
        eq(mockContext))).thenReturn(mockSpout);
    when(mockObjectBuilder.buildObject(eq(spoutDefinition1),
        eq(mockContext))).thenReturn(mockSpout1);
    when(mockContext.getTopologyDefinition()).thenReturn(topologyDefinition);

    subject.buildSpouts(mockContext, mockTopologyBuilder, mockObjectBuilder);

    verify(mockContext).getTopologyDefinition();
    verify(mockObjectBuilder).buildObject(same(spoutDefinition), same(mockContext));
    verify(mockObjectBuilder).buildObject(same(spoutDefinition1), same(mockContext));
    verify(mockTopologyBuilder).setSpout(eq(id), eq(mockSpout), eq(parallelism));
    verify(mockTopologyBuilder).setSpout(eq(id1), eq(mockSpout1), eq(parallelism1));
    verify(mockContext).addSpout(eq(id), anyObject());
    verify(mockContext).addSpout(eq(id1), anyObject());
  }

  @SuppressWarnings("serial")
  private class MockSpout implements IRichSpout {

    @Override
    public void open(Map conf,
                     TopologyContext context, SpoutOutputCollector collector) {

    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void nextTuple() {

    }

    @Override
    public void ack(Object msgId) {

    }

    @Override
    public void fail(Object msgId) {

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
