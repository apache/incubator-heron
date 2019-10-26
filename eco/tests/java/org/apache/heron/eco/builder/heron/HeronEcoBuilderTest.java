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

package org.apache.heron.eco.builder.heron;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import org.apache.heron.api.Config;
import org.apache.heron.api.topology.TopologyBuilder;
import org.apache.heron.eco.builder.BoltBuilder;
import org.apache.heron.eco.builder.ComponentBuilder;
import org.apache.heron.eco.builder.ConfigBuilder;
import org.apache.heron.eco.builder.ObjectBuilder;
import org.apache.heron.eco.definition.EcoExecutionContext;
import org.apache.heron.eco.definition.EcoTopologyDefinition;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class HeronEcoBuilderTest {

  @Mock
  private SpoutBuilder mockSpoutBuilder;
  @Mock
  private BoltBuilder mockBoltBuilder;
  @Mock
  private StreamBuilder mockStreamBuilder;
  @Mock
  private ComponentBuilder mockComponentBuilder;
  @Mock
  private ConfigBuilder mockConfigBuilder;
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
        mockComponentBuilder,
        mockConfigBuilder);
  }

  @Test
  public void testBuild_EmptyConfigMap_ReturnsDefaultConfigs() throws Exception {

    Config config = new Config();
    when(mockConfigBuilder.buildConfig(eq(ecoTopologyDefinition))).thenReturn(config);

    Config returnedConfig = subject.buildConfig(ecoTopologyDefinition);

    verify(mockConfigBuilder).buildConfig(same(ecoTopologyDefinition));

    assertThat(returnedConfig.get(Config.TOPOLOGY_DEBUG), is(nullValue()));
    assertThat(config, sameInstance(returnedConfig));
  }

  @Test
  public void testBuild_CustomConfigMap_ReturnsCorrectConfigs() throws Exception {
    configMap.put(Config.TOPOLOGY_DEBUG, false);
    final String environment = "dev";
    final int spouts = 3;
    configMap.put(Config.TOPOLOGY_ENVIRONMENT, environment);
    configMap.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, spouts);

    Config config = new Config();

    when(mockConfigBuilder.buildConfig(eq(ecoTopologyDefinition))).thenReturn(config);

    assertThat(subject.buildConfig(ecoTopologyDefinition), sameInstance(config));

    verify(mockConfigBuilder).buildConfig(same(ecoTopologyDefinition));
  }

  @Test
  public void testBuildTopologyBuilder_BuildsAsExpected()
      throws IllegalAccessException, ClassNotFoundException,
      InstantiationException, NoSuchFieldException, InvocationTargetException {
    Config config = new Config();
    EcoExecutionContext context = new EcoExecutionContext(ecoTopologyDefinition, config);
    ObjectBuilder objectBuilder = new ObjectBuilder();
    subject.buildTopologyBuilder(context, objectBuilder);

    verify(mockSpoutBuilder).buildSpouts(same(context),
        any(TopologyBuilder.class), same(objectBuilder));
    verify(mockBoltBuilder).buildBolts(same(context), same(objectBuilder));
    verify(mockStreamBuilder).buildStreams(same(context), any(TopologyBuilder.class),
        same(objectBuilder));
    verify(mockComponentBuilder).buildComponents(same(context), same(objectBuilder));
  }
}
