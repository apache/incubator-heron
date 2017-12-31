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
package com.twitter.heron.eco.builder;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;


import org.apache.storm.Config;
import org.apache.storm.topology.TopologyBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;


import com.twitter.heron.eco.definition.EcoExecutionContext;
import com.twitter.heron.eco.definition.EcoTopologyDefinition;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(MockitoJUnitRunner.class)
public class EcoBuilderTest {

  @Mock
  private SpoutBuilder mockSpoutBuilder;
  @Mock
  private BoltBuilder mockBoltBuilder;
  @Mock
  private StreamBuilder mockStreamBuilder;
  @Mock
  private ComponentBuilder mockComponentBuilder;
  @InjectMocks
  private EcoBuilder subject;

  private Map<String, Object> configMap;

  private EcoTopologyDefinition ecoTopologyDefinition;

  @Before
  public void setUpForEachTestCase() {
    configMap = new HashMap<>();
    ecoTopologyDefinition = new EcoTopologyDefinition();
    ecoTopologyDefinition.setConfig(configMap);
  }

  @After
  public void ensureNoUnexpectedMockInteractions() {
    verifyNoMoreInteractions(mockSpoutBuilder,
        mockBoltBuilder,
        mockStreamBuilder,
        mockComponentBuilder);
  }

  @Test
  public void testBuild_EmptyConfigMap_ReturnsDefaultConfigs() {
    // do nothing with config map on purpose

    Config config = subject.buildConfig(ecoTopologyDefinition);

    assertThat(config.get(Config.TOPOLOGY_DEBUG), is(nullValue()));
  }

  @Test
  public void testBuild_CustomConfigMap_ReturnsCorrectConfigs() {
    configMap.put(Config.TOPOLOGY_DEBUG, false);
    final String environment = "dev";
    final int spouts = 3;
    configMap.put(Config.TOPOLOGY_ENVIRONMENT, environment);
    configMap.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, spouts);

    Config config = subject.buildConfig(ecoTopologyDefinition);

    assertThat(config.get(Config.TOPOLOGY_DEBUG), is(equalTo(false)));
    assertThat(config.get(Config.TOPOLOGY_ENVIRONMENT), is(equalTo(environment)));
    assertThat(config.get(Config.TOPOLOGY_MAX_SPOUT_PENDING), is(equalTo(spouts)));
  }

  @Test
  public void testBuildTopologyBuilder_BuildsAsExpected()
      throws IllegalAccessException, ClassNotFoundException,
      InstantiationException, NoSuchFieldException, InvocationTargetException {
    Config config = new Config();
    EcoExecutionContext context = new EcoExecutionContext(ecoTopologyDefinition, config);
    ObjectBuilder objectBuilder = new ObjectBuilder();
    subject.buildTopologyBuilder(context, objectBuilder);

    verify(mockSpoutBuilder).addSpoutsToExecutionContext(same(context),
        any(TopologyBuilder.class), same(objectBuilder));
    verify(mockBoltBuilder).buildBolts(same(context), same(objectBuilder));
    verify(mockStreamBuilder).buildStreams(same(context), any(TopologyBuilder.class),
        same(objectBuilder));
    verify(mockComponentBuilder).buildComponents(same(context));
  }

}
