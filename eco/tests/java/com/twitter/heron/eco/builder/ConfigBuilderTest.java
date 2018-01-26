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

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.twitter.heron.api.Config;
import com.twitter.heron.eco.definition.EcoTopologyDefinition;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Unit tests for {@link ConfigBuilder}
 */
public class ConfigBuilderTest {

  private ConfigBuilder subject;

  private static final String objectString = "{id=spout-1, ram=256MB, cpu=0.5, disk=4GB}";

  @Before
  public void setUpForEachTestCase() {
    subject = new ConfigBuilder();
  }

  @Test
  public void testBuildConfig_ConfigIsNotDefined_ReturnsEmptyConfig() throws Exception {
    EcoTopologyDefinition ecoTopologyDefinition = new EcoTopologyDefinition();

    Config config = subject.buildConfig(ecoTopologyDefinition);

    assertThat(0, is(equalTo(config.size())));
  }

  @Test
  public void testBuildConfig_ConfigIsDefined_ReturnsCorrectValues() throws Exception {
    EcoTopologyDefinition ecoTopologyDefinition = new EcoTopologyDefinition();
    Map<String, Object> topologyDefinitionConfig = new HashMap<>();
    topologyDefinitionConfig.put(Config.TOPOLOGY_COMPONENT_PARALLELISM, 2);
    topologyDefinitionConfig.put(Config.TOPOLOGY_CONTAINER_CPU_REQUESTED, 4);
    ecoTopologyDefinition.setConfig(topologyDefinitionConfig);

    Config config = subject.buildConfig(ecoTopologyDefinition);

    assertThat(config.get(Config.TOPOLOGY_COMPONENT_PARALLELISM), is(equalTo(2)));
    assertThat(config.get(Config.TOPOLOGY_CONTAINER_CPU_REQUESTED), is(equalTo(4)));
  }
}
