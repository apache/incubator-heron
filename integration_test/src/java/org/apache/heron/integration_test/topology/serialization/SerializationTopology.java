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

package org.apache.heron.integration_test.topology.serialization;

import java.net.MalformedURLException;

import org.apache.heron.api.Config;
import org.apache.heron.integration_test.common.AbstractTestTopology;
import org.apache.heron.integration_test.common.bolt.IncrementBolt;
import org.apache.heron.integration_test.core.TestTopologyBuilder;

/**
 * Topology to test Customized Java Serialization
 */
public final class SerializationTopology extends AbstractTestTopology {

  private SerializationTopology(String[] args) throws MalformedURLException {
    super(args);
  }

  @Override
  protected TestTopologyBuilder buildTopology(TestTopologyBuilder builder) {

    CustomObject[] inputObjects = new CustomObject[]{
        new CustomObject("A", 10),
        new CustomObject("B", 20),
        new CustomObject("C", 30)
    };

    builder.setSpout("custom-spout", new CustomSpout(inputObjects), 1);
    builder.setBolt("check-bolt", new CustomCheckBolt(inputObjects), 1)
        .shuffleGrouping("custom-spout");
    builder.setBolt("count-bolt", new IncrementBolt(), 1)
        .shuffleGrouping("check-bolt");
    return builder;
  }

  @Override
  protected Config buildConfig(Config config) {
    config.setSerializationClassName("org.apache.heron.api.serializer.JavaSerializer");
    return config;
  }

  public static void main(String[] args) throws Exception {
    SerializationTopology topology = new SerializationTopology(args);
    topology.submit();
  }
}
