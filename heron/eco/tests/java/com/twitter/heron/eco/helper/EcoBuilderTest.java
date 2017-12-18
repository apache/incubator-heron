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
package com.twitter.heron.eco.helper;

import java.util.HashMap;
import java.util.Map;


import org.junit.Test;

import com.twitter.heron.api.Config;
import com.twitter.heron.eco.definition.EcoTopologyDefinition;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.*;

public class EcoBuilderTest {


  @Test
  public void testBuildEmptyConfigMapReturnsDefaultConfigs() {
    Map<String, Object> configMap = new HashMap<>();
    EcoTopologyDefinition ecoTopologyDefinition = new EcoTopologyDefinition();
    ecoTopologyDefinition.setConfig(configMap);

    Config config = EcoBuilder.buildConfig(ecoTopologyDefinition);
    assertThat(config.get(Config.TOPOLOGY_DEBUG), is(nullValue()));
  }

}
