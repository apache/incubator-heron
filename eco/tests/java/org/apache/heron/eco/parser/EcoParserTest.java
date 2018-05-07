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
package org.apache.heron.eco.parser;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;

import org.apache.heron.eco.definition.BeanDefinition;
import org.apache.heron.eco.definition.BeanReference;
import org.apache.heron.eco.definition.BoltDefinition;
import org.apache.heron.eco.definition.EcoTopologyDefinition;
import org.apache.heron.eco.definition.GroupingDefinition;
import org.apache.heron.eco.definition.PropertyDefinition;
import org.apache.heron.eco.definition.StreamDefinition;

import static junit.framework.TestCase.assertNotNull;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;


/**
 * Unit tests for {@link EcoParser}
 */
public class EcoParserTest {

  private static final String BOLT_1 = "bolt-1";
  private static final String BOLT_2 = "bolt-2";
  private static final String YAML_NO_CONFIG_STR = "# topology definition\n"
      + "# name to be used when submitting\n"
      + "name: \"yaml-topology\"\n"
      + "\n"
      + "# topology configuration\n"
      + "# this will be passed to the submitter as a map of config options\n"
      + "#\n"
      + "# spout definitions\n"
      + "spouts:\n"
      + "  - id: \"spout-1\"\n"
      + "    className: \"org.apache.heron.sample.TestWordSpout\"\n"
      + "    parallelism: 1\n"
      + "\n"
      + "# bolt definitions\n"
      + "bolts:\n"
      + "  - id: \"bolt-1\"\n"
      + "    className: \"org.apache.heron.sample.TestWordCounter\"\n"
      + "    parallelism: 2\n"
      + "\n"
      + "  - id: \"bolt-2\"\n"
      + "    className: \"org.apache.heron.sample.LogInfoBolt\"\n"
      + "    parallelism: 1\n"
      + "\n"
      + "#stream definitions\n"
      + "# stream definitions define connections between spouts and bolts.\n"
      + "# note that such connections can be cyclical\n"
      + "streams:\n"
      + "  - name: \"spout-1 --> bolt-1\" # name isn't used (placeholder for logging, UI, etc.)\n"
      + "    id: \"connection-1\"\n"
      + "    from: \"spout-1\"\n"
      + "    to: \"bolt-1\"\n"
      + "    grouping:\n"
      + "      type: FIELDS\n"
      + "      args: [\"word\"]\n"
      + "\n"
      + "  - name: \"bolt-1 --> bolt2\"\n"
      + "    id: \"connection-2\"\n"
      + "    from: \"bolt-1\"\n"
      + "    to: \"bolt-2\"\n"
      + "    grouping:\n"
      + "      type: SHUFFLE";
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
      + "    className: \"org.apache.heron.sample.TestWordSpout\"\n"
      + "    parallelism: 1\n"
      + "\n"
      + "# bolt definitions\n"
      + "bolts:\n"
      + "  - id: \"bolt-1\"\n"
      + "    className: \"org.apache.heron.sample.TestWordCounter\"\n"
      + "    parallelism: 2\n"
      + "\n"
      + "  - id: \"bolt-2\"\n"
      + "    className: \"org.apache.heron.sample.LogInfoBolt\"\n"
      + "    parallelism: 1\n"
      + "\n"
      + "#stream definitions\n"
      + "# stream definitions define connections between spouts and bolts.\n"
      + "# note that such connections can be cyclical\n"
      + "streams:\n"
      + "  - name: \"spout-1 --> bolt-1\" # name isn't used (placeholder for logging, UI, etc.)\n"
      + "    id: \"connection-1\"\n"
      + "    from: \"spout-1\"\n"
      + "    to: \"bolt-1\"\n"
      + "    grouping:\n"
      + "      type: FIELDS\n"
      + "      args: [\"word\"]\n"
      + "\n"
      + "  - name: \"bolt-1 --> bolt2\"\n"
      + "    id: \"connection-2\"\n"
      + "    from: \"bolt-1\"\n"
      + "    to: \"bolt-2\"\n"
      + "    grouping:\n"
      + "      type: SHUFFLE";

  private static final String YAML_STR_1 = "# Test ability to wire together shell spouts/bolts\n"
      + "---\n"
      + "\n"
      + "name: \"kafka-topology\"\n"
      + "type: \"heron\"\n"
      + "\n"
      + "# Components\n"
      + "# Components are analagous to Spring beans. They are meant to be used as constructor,\n"
      + "# property(setter), and builder arguments.\n"
      + "#\n"
      + "# for the time being, components must be declared in the order they are referenced\n"
      + "components:\n"
      + "  - id: \"stringScheme\"\n"
      + "    className: \"org.apache.storm.kafka.StringScheme\"\n"
      + "\n"
      + "  - id: \"stringMultiScheme\"\n"
      + "    className: \"org.apache.storm.spout.SchemeAsMultiScheme\"\n"
      + "    constructorArgs:\n"
      + "      - ref: \"stringScheme\"\n"
      + "\n"
      + "  - id: \"zkHosts\"\n"
      + "    className: \"org.apache.storm.kafka.ZkHosts\"\n"
      + "    constructorArgs:\n"
      + "      - \"localhost:2181\"\n"
      + "\n"
      + "# Alternative kafka config\n"
      + "#  - id: \"kafkaConfig\"\n"
      + "#    className: \"org.apache.storm.kafka.KafkaConfig\"\n"
      + "#    constructorArgs:\n"
      + "#      # brokerHosts\n"
      + "#      - ref: \"zkHosts\"\n"
      + "#      # topic\n"
      + "#      - \"myKafkaTopic\"\n"
      + "#      # clientId (optional)\n"
      + "#      - \"myKafkaClientId\"\n"
      + "\n"
      + "  - id: \"spoutConfig\"\n"
      + "    className: \"org.apache.storm.kafka.SpoutConfig\"\n"
      + "    constructorArgs:\n"
      + "      # brokerHosts\n"
      + "      - ref: \"zkHosts\"\n"
      + "      # topic\n"
      + "      - \"myKafkaTopic\"\n"
      + "      # zkRoot\n"
      + "      - \"/kafkaSpout\"\n"
      + "      # id\n"
      + "      - \"myId\"\n"
      + "    properties:\n"
      + "      - name: \"ignoreZkOffsets\"\n"
      + "        value: true\n"
      + "      - name: \"scheme\"\n"
      + "        ref: \"stringMultiScheme\"\n"
      + "\n"
      + "\n"
      + "\n"
      + "# topology configuration\n"
      + "# this will be passed to the submitter as a map of config options\n"
      + "#\n"
      + "config:\n"
      + "  topology.workers: 1\n"
      + "  # ...\n"
      + "\n"
      + "# spout definitions\n"
      + "spouts:\n"
      + "  - id: \"kafka-spout\"\n"
      + "    className: \"org.apache.storm.kafka.KafkaSpout\"\n"
      + "    constructorArgs:\n"
      + "      - ref: \"spoutConfig\"\n"
      + "\n"
      + "# bolt definitions\n"
      + "bolts:\n"
      + "  - id: \"splitsentence\"\n"
      + "    className: \"org.apache.storm.flux.wrappers.bolts.FluxShellBolt\"\n"
      + "    constructorArgs:\n"
      + "      # command line\n"
      + "      - [\"python\", \"splitsentence.py\"]\n"
      + "      # output fields\n"
      + "      - [\"word\"]\n"
      + "    parallelism: 1\n"
      + "    # ...\n"
      + "\n"
      + "  - id: \"log\"\n"
      + "    className: \"org.apache.storm.flux.wrappers.bolts.LogInfoBolt\"\n"
      + "    parallelism: 1\n"
      + "    # ...\n"
      + "\n"
      + "  - id: \"count\"\n"
      + "    className: \"org.apache.storm.testing.TestWordCounter\"\n"
      + "    parallelism: 1\n"
      + "    # ...\n"
      + "\n"
      + "#stream definitions\n"
      + "# stream definitions define connections between spouts and bolts.\n"
      + "# note that such connections can be cyclical\n"
      + "# custom stream groupings are also supported\n"
      + "\n"
      + "streams:\n"
      + "  - name: \"kafka --> split\" # name isn't used (placeholder for logging, UI, etc.)\n"
      + "    id: \"stream1\"\n"
      + "    from: \"kafka-spout\"\n"
      + "    to: \"splitsentence\"\n"
      + "    grouping:\n"
      + "      type: SHUFFLE\n"
      + "\n"
      + "  - name: \"split --> count\"\n"
      + "    id: \"stream2\"\n"
      + "    from: \"splitsentence\"\n"
      + "    to: \"count\"\n"
      + "    grouping:\n"
      + "      type: FIELDS\n"
      + "      args: [\"word\"]\n"
      + "\n"
      + "  - name: \"count --> log\"\n"
      + "    id: \"stream3\"\n"
      + "    from: \"count\"\n"
      + "    to: \"log\"\n"
      + "    grouping:\n"
      + "      type: SHUFFLE";

  private static final String PROPERTY_SUBSTITUION_YAML = "name: \"fibonacci-topology\"\n"
      + "\n"
      + "config:\n"
      + "  topology.workers: 1\n"
      + "\n"
      + "components:\n"
      + "  - id: \"property-holder\"\n"
      + "    className: \"org.apache.heron.examples.eco.TestPropertyHolder\"\n"
      + "    constructorArgs:\n"
      + "      - \"some argument\"\n"
      + "    properties:\n"
      + "      - name: \"numberProperty\"\n"
      + "        value: 11\n"
      + "      - name: \"publicProperty\"\n"
      + "        value: \"This is public property\"\n"
      + "\n"
      + "spouts:\n"
      + "  - id: \"spout-1\"\n"
      + "    className: \"org.apache.heron.examples.eco.TestFibonacciSpout\"\n"
      + "    constructorArgs:\n"
      + "      - ref: \"property-holder\"\n"
      + "    parallelism: 1\n"
      + "\n"
      + "bolts:\n"
      + "  - id: \"even-and-odd-bolt\"\n"
      + "    className: \"org.apache.heron.examples.eco.EvenAndOddBolt\"\n"
      + "    parallelism: 1\n"
      + "\n"
      + "  - id: \"ibasic-print-bolt\"\n"
      + "    className: \"org.apache.heron.examples.eco.TestIBasicPrintBolt\"\n"
      + "    parallelism: 1\n"
      + "    configMethods:\n"
      + "      - name: \"sampleConfigurationMethod\"\n"
      + "        args:\n"
      + "          - \"${ecoPropertyOne}\"\n"
      + "\n"
      + "  - id: \"sys-out-bolt\"\n"
      + "    className: \"org.apache.heron.examples.eco.TestPrintBolt\"\n"
      + "    parallelism: 1";

  private static final String SAMPLE_PROPERTIES = "ecoPropertyOne=ecoValueOne\n"
      + "\n"
      + "ecoPropertyTwo=1234\n"
      + "\n";

  private EcoParser subject;

  @Before
  public void setUpBeforeEachTestCase() {
    subject = new EcoParser();
  }


  @Test
  public void testParseFromInputStream_VerifyComponents_MapsAsExpected() throws Exception {

    InputStream inputStream = new ByteArrayInputStream(YAML_STR_1.getBytes());
    FileInputStream mockPropsStream = PowerMockito.mock(FileInputStream.class);
    EcoTopologyDefinition topologyDefinition =
        subject.parseFromInputStream(inputStream, mockPropsStream, false);
    List<BeanDefinition> components = topologyDefinition.getComponents();

    assertEquals("kafka-topology", topologyDefinition.getName());
    assertEquals("heron", topologyDefinition.getType());
    assertEquals(4, components.size());

    BeanDefinition stringSchemeComponent = components.get(0);
    assertEquals("stringScheme", stringSchemeComponent.getId());
    assertEquals("org.apache.storm.kafka.StringScheme", stringSchemeComponent.getClassName());


    BeanDefinition stringMultiSchemeComponent = components.get(1);
    assertEquals("stringMultiScheme", stringMultiSchemeComponent.getId());
    assertEquals("org.apache.storm.spout.SchemeAsMultiScheme",
        stringMultiSchemeComponent.getClassName());
    assertEquals(1, stringMultiSchemeComponent.getConstructorArgs().size());
    BeanReference multiStringReference =
        (BeanReference) stringMultiSchemeComponent.getConstructorArgs().get(0);
    assertEquals("stringScheme", multiStringReference.getId());

    BeanDefinition zkHostsComponent = components.get(2);
    assertEquals("zkHosts", zkHostsComponent.getId());
    assertEquals("org.apache.storm.kafka.ZkHosts", zkHostsComponent.getClassName());
    assertEquals(1, zkHostsComponent.getConstructorArgs().size());
    assertEquals("localhost:2181", zkHostsComponent.getConstructorArgs().get(0));

    BeanDefinition spoutConfigComponent = components.get(3);
    List<Object> spoutConstructArgs = spoutConfigComponent.getConstructorArgs();
    assertEquals("spoutConfig", spoutConfigComponent.getId());
    assertEquals("org.apache.storm.kafka.SpoutConfig", spoutConfigComponent.getClassName());
    BeanReference spoutBrokerHostComponent = (BeanReference) spoutConstructArgs.get(0);
    assertEquals("zkHosts", spoutBrokerHostComponent.getId());
    assertEquals("myKafkaTopic", spoutConstructArgs.get(1));
    assertEquals("/kafkaSpout", spoutConstructArgs.get(2));
    List<PropertyDefinition> properties = spoutConfigComponent.getProperties();
    assertEquals("ignoreZkOffsets", properties.get(0).getName());
    assertEquals(true, properties.get(0).getValue());
    assertEquals("scheme", properties.get(1).getName());
    assertEquals(true, properties.get(1).isReference());
    assertEquals("stringMultiScheme", properties.get(1).getRef());
  }

  @Test
  public void testParseFromInputStream_VerifyAllButComponents_MapsAsExpected() throws Exception {

    InputStream inputStream = new ByteArrayInputStream(YAML_STR.getBytes());
    FileInputStream mockPropsStream = PowerMockito.mock(FileInputStream.class);
    EcoTopologyDefinition topologyDefinition = subject.parseFromInputStream(inputStream,
        mockPropsStream, false);

    assertEquals("yaml-topology", topologyDefinition.getName());
    assertEquals(1, topologyDefinition.getConfig().size());
    assertEquals(1, topologyDefinition.getConfig().get("topology.workers"));

    BoltDefinition bolt1 = topologyDefinition.getBolt(BOLT_1);
    assertNotNull(bolt1);
    assertEquals(2, bolt1.getParallelism());
    assertEquals("org.apache.heron.sample.TestWordCounter", bolt1.getClassName());
    assertEquals(BOLT_1, bolt1.getId());


    BoltDefinition bolt2 = topologyDefinition.getBolt(BOLT_2);
    assertEquals(1, bolt2.getParallelism());
    assertEquals("org.apache.heron.sample.LogInfoBolt", bolt2.getClassName());
    assertEquals(BOLT_2, bolt2.getId());

    List<StreamDefinition> streamDefinitions = topologyDefinition.getStreams();
    StreamDefinition streamDefinitionOne = streamDefinitions.get(0);
    GroupingDefinition groupingDefinitionOne = streamDefinitionOne.getGrouping();
    StreamDefinition streamDefinitionTwo = streamDefinitions.get(1);
    GroupingDefinition groupingDefinitionTwo = streamDefinitionTwo.getGrouping();

    assertEquals(2, streamDefinitions.size());

    assertEquals(BOLT_1, streamDefinitionOne.getTo());
    assertEquals("spout-1", streamDefinitionOne.getFrom());
    assertEquals(GroupingDefinition.Type.FIELDS, groupingDefinitionOne.getType());
    assertEquals(1, groupingDefinitionOne.getArgs().size());
    assertEquals("word", groupingDefinitionOne.getArgs().get(0));
    assertEquals("connection-1", streamDefinitionOne.getId());

    assertEquals(BOLT_2, streamDefinitionTwo.getTo());
    assertEquals("bolt-1", streamDefinitionTwo.getFrom());
    assertEquals(GroupingDefinition.Type.SHUFFLE, groupingDefinitionTwo.getType());
    assertEquals("connection-2", streamDefinitionTwo.getId());
    assertNull(groupingDefinitionTwo.getArgs());

  }

  @Test
  public void testPartFromInputStream_NoConfigSpecified_ConfigMapIsEmpty() throws Exception {
    InputStream inputStream = new ByteArrayInputStream(YAML_NO_CONFIG_STR.getBytes());
    FileInputStream mockPropsStream = PowerMockito.mock(FileInputStream.class);

    EcoTopologyDefinition topologyDefinition =
        subject.parseFromInputStream(inputStream, mockPropsStream, false);

    assertNotNull(topologyDefinition.getConfig());
    assertEquals(0, topologyDefinition.getConfig().size());
  }

  @Test(expected = Exception.class)
  public void testParseFromInputStream_StreamIsNull_ExceptionThrown() throws Exception {
    InputStream inputStream = null;
    EcoTopologyDefinition ecoTopologyDefinition = null;
    FileInputStream mockPropsStream = PowerMockito.mock(FileInputStream.class);
    try {
      ecoTopologyDefinition = subject.parseFromInputStream(inputStream,
          mockPropsStream, false);
    } finally {
      assertNull(ecoTopologyDefinition);
    }
  }

  @Test
  public void testParseFromInputStream_PropertyFiltering_SubstitutesAsExpected() throws Exception {
    InputStream inputStream = new ByteArrayInputStream(PROPERTY_SUBSTITUION_YAML.getBytes());
    InputStream propsStream = new ByteArrayInputStream(SAMPLE_PROPERTIES.getBytes());
    EcoTopologyDefinition ecoTopologyDefinition =
        subject.parseFromInputStream(inputStream, propsStream, false);

    BoltDefinition bolt = ecoTopologyDefinition.getBolt("ibasic-print-bolt");
    List<Object> args = bolt.getConfigMethods().get(0).getArgs();


    assertThat(args.get(0), is(equalTo("ecoValueOne")));
  }
}
