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

import java.util.HashMap;
import java.util.Map;


import org.junit.Before;
import org.junit.Test;

import com.twitter.heron.api.Config;
import com.twitter.heron.eco.definition.EcoTopologyDefinition;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.*;

public class EcoBuilderTest {

  private Map<String, Object> configMap;

  private EcoTopologyDefinition ecoTopologyDefinition;

  @Before
  public void setUpForEachTestCase() {
    configMap = new HashMap<>();
    ecoTopologyDefinition = new EcoTopologyDefinition();
    ecoTopologyDefinition.setConfig(configMap);
  }

  @Test
  public void testBuild_EmptyConfigMap_ReturnsDefaultConfigs() {
    // do nothing with config map on purpose for this test

    Config config = EcoBuilder.buildConfig(ecoTopologyDefinition);

    assertThat(config.get(Config.TOPOLOGY_DEBUG), is(nullValue()));
  }

  @Test
  public void testBuild_CustomConfigMap_ReturnsCorrectConfigs() {
    configMap.put(Config.TOPOLOGY_DEBUG, false);
    final String environment = "dev";
    final int spouts = 3;
    configMap.put(Config.TOPOLOGY_ENVIRONMENT, environment);
    configMap.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, spouts);

    Config config = EcoBuilder.buildConfig(ecoTopologyDefinition);

    assertThat(config.get(Config.TOPOLOGY_DEBUG), is(equalTo(false)));
    assertThat(config.get(Config.TOPOLOGY_ENVIRONMENT), is(equalTo(environment)));
    assertThat(config.get(Config.TOPOLOGY_MAX_SPOUT_PENDING), is(equalTo(spouts)));
  }

  @Test
  public void testBuildTopologyBuilder_BuildsAsExpected() {


    assertNotNull(EcoBuilder.buildConfig(ecoTopologyDefinition));

  }

}
