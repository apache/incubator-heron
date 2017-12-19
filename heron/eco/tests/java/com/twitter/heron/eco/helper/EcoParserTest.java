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

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.junit.Test;

import com.twitter.heron.eco.definition.EcoTopologyDefinition;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;


public class EcoParserTest {

  private static final String BOLT_1 = "bolt-1";
  private static final String YAML_STR = "# Licensed to the Apache Software Foundation"
      + " (ASF) under one\n"
      + "# or more contributor license agreements.  See the NOTICE file\n"
      + "# distributed with this work for additional information\n"
      + "# regarding copyright ownership.  The ASF licenses this file\n"
      + "# to you under the Apache License, Version 2.0 (the\n"
      + "# \"License\"); you may not use this file except in compliance\n"
      + "# with the License.  You may obtain a copy of the License at\n"
      + "#\n"
      + "# http://www.apache.org/licenses/LICENSE-2.0\n"
      + "#\n"
      + "# Unless required by applicable law or agreed to in writing, software\n"
      + "# distributed under the License is distributed on an \"AS IS\" BASIS,\n"
      + "# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n"
      + "# See the License for the specific language governing permissions and\n"
      + "# limitations under the License.\n"
      + "\n"
      + "---\n"
      + "\n"
      + "# topology definition\n"
      + "# name to be used when submitting\n"
      + "name: \"yaml-topology\"\n"
      + "\n"
      + "# topology configuration\n"
      + "# this will be passed to the submitter as a map of config options\n"
      + "#\n"
      + "config:\n"
      + "  topology.workers: 1\n"
      + "\n"
      + "# spout definitions\n"
      + "spouts:\n"
      + "  - id: \"spout-1\"\n"
      + "    className: \"com.twitter.heron.sample.TestWordSpout\"\n"
      + "    parallelism: 1\n"
      + "\n"
      + "# bolt definitions\n"
      + "bolts:\n"
      + "  - id: \"bolt-1\"\n"
      + "    className: \"com.twitter.heron.sample.TestWordCounter\"\n"
      + "    parallelism: 2\n"
      + "\n"
      + "  - id: \"bolt-2\"\n"
      + "    className: \"com.twitter.heron.sample.LogInfoBolt\"\n"
      + "    parallelism: 1\n"
      + "\n"
      + "#stream definitions\n"
      + "# stream definitions define connections between spouts and bolts.\n"
      + "# note that such connections can be cyclical\n"
      + "streams:\n"
      + "  - name: \"spout-1 --> bolt-1\" # name isn't used (placeholder for logging, UI, etc.)\n"
      + "#    id: \"connection-1\"\n"
      + "    from: \"spout-1\"\n"
      + "    to: \"bolt-1\"\n"
      + "    grouping:\n"
      + "      type: FIELDS\n"
      + "      args: [\"word\"]\n"
      + "\n"
      + "  - name: \"bolt-1 --> bolt2\"\n"
      + "    from: \"bolt-1\"\n"
      + "    to: \"bolt-2\"\n"
      + "    grouping:\n"
      + "      type: SHUFFLE";

  @Test
  public void parseFromInputStream() {

    InputStream inputStream = new ByteArrayInputStream(YAML_STR.getBytes());

    EcoTopologyDefinition topologyDefinition = EcoParser.parseFromInputStream(inputStream);

    assertNotNull(topologyDefinition.getBolt(BOLT_1));
    assertEquals(topologyDefinition.getBolt(BOLT_1).getParallelism(), 2);




  }
}
