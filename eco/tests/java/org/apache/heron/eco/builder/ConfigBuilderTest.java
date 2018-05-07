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

package org.apache.heron.eco.builder;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;

import org.apache.heron.api.Config;
import org.apache.heron.eco.definition.EcoTopologyDefinition;
import org.apache.heron.eco.parser.EcoParser;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

/**
 * Unit tests for {@link ConfigBuilder}
 */
public class ConfigBuilderTest {

  private ConfigBuilder subject;

  private static final String YAML_PROPERTIES = "config:\n"
      + "  topology.workers: 1\n"
      + "  topology.component.resourcemap:\n"
      + "\n"
      + "    - id: \"spout-1\"\n"
      + "      ram: 256000000B\n"
      + "      cpu: 0.5\n"
      + "      disk: 4GB\n"
      + "\n"
      + "    - id: \"bolt-1\"\n"
      + "      ram: 256000000B\n"
      + "      cpu: 0.5\n"
      + "      disk: 2GB";

  private static final String INCORRECT_BYTES_FORMAT_YAML = "config:\n"
      + "  topology.workers: 1\n"
      + "  topology.component.resourcemap:\n"
      + "\n"
      + "    - id: \"spout-1\"\n"
      + "      ram: B256\n"
      + "      cpu: 0.5\n"
      + "      disk: 4GB\n"
      + "\n"
      + "    - id: \"bolt-1\"\n"
      + "      ram: 128B\n"
      + "      cpu: 0.5\n"
      + "      disk: 2GB";

  private static final String INCORRECT_MB_FORMAT_YAML = "config:\n"
      + "  topology.workers: 1\n"
      + "  topology.component.resourcemap:\n"
      + "\n"
      + "    - id: \"spout-1\"\n"
      + "      ram: 25MB6\n"
      + "      cpu: 0.5\n"
      + "      disk: 4GB\n"
      + "\n"
      + "    - id: \"bolt-1\"\n"
      + "      ram: 256B\n"
      + "      cpu: 0.5\n"
      + "      disk: 2GB";

  private static final String INCORRECT_GB_FORMAT_YAML = "config:\n"
      + "  topology.workers: 1\n"
      + "  topology.component.resourcemap:\n"
      + "\n"
      + "    - id: \"spout-1\"\n"
      + "      ram: GB256\n"
      + "      cpu: 0.5\n"
      + "      disk: 4GB\n"
      + "\n"
      + "    - id: \"bolt-1\"\n"
      + "      ram: 256GB\n"
      + "      cpu: 0.5\n"
      + "      disk: 2GB";


  private static final String JVM_OPTIONS_CONFIG = "config:\n"
      + "  topology.workers: 1\n"
      + "  topology.component.resourcemap:\n"
      + "\n"
      + "    - id: \"spout-1\"\n"
      + "      ram: 256MB\n"
      + "      cpu: 0.5\n"
      + "      disk: 4GB\n"
      + "\n"
      + "    - id: \"bolt-1\"\n"
      + "      ram: 256MB\n"
      + "      cpu: 0.5\n"
      + "      disk: 2GB\n"
      + "\n"
      + "  topology.component.jvmoptions:\n"
      + "\n"
      + "   - id: \"spout-1\"\n"
      + "     options: [\"-XX:NewSize=300m\", \"-Xms2g\"]";

  private static final String INCORRECT_JVM_OPTIONS_CONFIG = "config:\n"
      + "  topology.workers: 1\n"
      + "  topology.component.resourcemap:\n"
      + "\n"
      + "    - id: \"spout-1\"\n"
      + "      ram: 256MB\n"
      + "      cpu: 0.5\n"
      + "      disk: 4GB\n"
      + "\n"
      + "    - id: \"bolt-1\"\n"
      + "      ram: 256MB\n"
      + "      cpu: 0.5\n"
      + "      disk: 2GB\n"
      + "\n"
      + "  topology.component.jvmoptions:\n"
      + "\n"
      + "   - id: \"spout-1\"\n"
      + "     something: \"-XX:NewSize=300m -Xms2g\"";

  private static final String INCORRECT_RAM_BYTES = "config:\n"
      + "  topology.workers: 1\n"
      + "  topology.component.resourcemap:\n"
      + "\n"
      + "    - id: \"spout-1\"\n"
      + "      ram: 25600000B\n"
      + "      cpu: 0.5\n"
      + "      disk: 4GB\n"
      + "\n"
      + "    - id: \"bolt-1\"\n"
      + "      ram: 256000000B\n"
      + "      cpu: 0.5\n"
      + "      disk: 2GB";

  private static final String INCORRECT_RAM_MEGABYTES = "config:\n"
      + "  topology.workers: 1\n"
      + "  topology.component.resourcemap:\n"
      + "\n"
      + "    - id: \"spout-1\"\n"
      + "      ram: 192MB\n"
      + "      cpu: 0.5\n"
      + "      disk: 4GB\n"
      + "\n"
      + "    - id: \"bolt-1\"\n"
      + "      ram: 256000000B\n"
      + "      cpu: 0.5\n"
      + "      disk: 2GB";


  private static final String INCORRECT_RAM_GIGABYTES = "config:\n"
      + "  topology.workers: 1\n"
      + "  topology.component.resourcemap:\n"
      + "\n"
      + "    - id: \"spout-1\"\n"
      + "      ram: 256MB\n"
      + "      cpu: 0.5\n"
      + "      disk: 4GB\n"
      + "\n"
      + "    - id: \"bolt-1\"\n"
      + "      ram: 0.255GB\n"
      + "      cpu: 0.5\n"
      + "      disk: 2GB";



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

  @Test
  public void testBuildConfig_SpecifyingComponentResources_ReturnsCorrectValues()
      throws Exception {
    EcoParser ecoParser = new EcoParser();
    InputStream inputStream = new ByteArrayInputStream(YAML_PROPERTIES.getBytes());
    FileInputStream mockPropsStream = PowerMockito.mock(FileInputStream.class);

    EcoTopologyDefinition ecoTopologyDefinition =
        ecoParser.parseFromInputStream(inputStream, mockPropsStream, false);

    Config config = subject.buildConfig(ecoTopologyDefinition);

    assertThat(config.get(Config.TOPOLOGY_COMPONENT_RAMMAP),
        is(equalTo("spout-1:256000000,bolt-1:256000000")));

  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuildConfig_IncorrectByteResourceFormat_ExceptionThrow() throws Exception {
    Config config = null;
    try {
      EcoParser ecoParser = new EcoParser();
      InputStream inputStream = new ByteArrayInputStream(INCORRECT_BYTES_FORMAT_YAML.getBytes());
      FileInputStream mockPropsStream = PowerMockito.mock(FileInputStream.class);

      EcoTopologyDefinition ecoTopologyDefinition =
          ecoParser.parseFromInputStream(inputStream, mockPropsStream, false);
      config = subject.buildConfig(ecoTopologyDefinition);
    } finally {
      assertNull(config);
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuildConfig_IncorrectGBResourceFormat_ExceptionThrow() throws Exception {
    Config config = null;
    try {
      EcoParser ecoParser = new EcoParser();
      InputStream inputStream = new ByteArrayInputStream(INCORRECT_GB_FORMAT_YAML.getBytes());
      FileInputStream mockPropsStream = PowerMockito.mock(FileInputStream.class);

      EcoTopologyDefinition ecoTopologyDefinition =
          ecoParser.parseFromInputStream(inputStream, mockPropsStream, false);
      config = subject.buildConfig(ecoTopologyDefinition);
    } finally {
      assertNull(config);
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuildConfig_IncorrectMBResourceFormat_ExceptionThrow() throws Exception {
    Config config = null;
    try {
      EcoParser ecoParser = new EcoParser();
      InputStream inputStream = new ByteArrayInputStream(INCORRECT_MB_FORMAT_YAML.getBytes());
      FileInputStream mockPropsStream = PowerMockito.mock(FileInputStream.class);

      EcoTopologyDefinition ecoTopologyDefinition =
          ecoParser.parseFromInputStream(inputStream, mockPropsStream, false);
      config = subject.buildConfig(ecoTopologyDefinition);
    } finally {
      assertNull(config);
    }
  }

  @Test
  public void testBuildConfig_SpecifyingComponentJVMOptions_ReturnsCorrectValues()
      throws Exception {
    EcoParser ecoParser = new EcoParser();
    InputStream inputStream = new ByteArrayInputStream(JVM_OPTIONS_CONFIG.getBytes());
    FileInputStream mockPropsStream = PowerMockito.mock(FileInputStream.class);

    EcoTopologyDefinition ecoTopologyDefinition =
        ecoParser.parseFromInputStream(inputStream, mockPropsStream, false);

    Config config = subject.buildConfig(ecoTopologyDefinition);

    assertThat(config.get(Config.TOPOLOGY_COMPONENT_JVMOPTS),
        is(equalTo("{\"c3BvdXQtMSw=\":\"LVhYOk5ld1NpemU9MzAwbSwgLVhtczJnIA==\"}")));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuildConfig_IncorrectComponentJVMOptions_ExceptionThrown() throws Exception {
    Config config = null;
    try {
      EcoParser ecoParser = new EcoParser();
      InputStream inputStream = new ByteArrayInputStream(INCORRECT_JVM_OPTIONS_CONFIG.getBytes());
      FileInputStream mockPropsStream = PowerMockito.mock(FileInputStream.class);

      EcoTopologyDefinition ecoTopologyDefinition =
          ecoParser.parseFromInputStream(inputStream, mockPropsStream, false);

      config = subject.buildConfig(ecoTopologyDefinition);
    } finally {
      assertNull(config);
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuildConfig_ByteAllocationTooSmall_ExceptionThrown() throws Exception {
    Config config = null;
    try {
      EcoParser ecoParser = new EcoParser();
      InputStream inputStream = new ByteArrayInputStream(INCORRECT_RAM_BYTES.getBytes());
      FileInputStream mockPropsStream = PowerMockito.mock(FileInputStream.class);

      EcoTopologyDefinition ecoTopologyDefinition =
          ecoParser.parseFromInputStream(inputStream, mockPropsStream, false);

      config = subject.buildConfig(ecoTopologyDefinition);
    } finally {
      assertNull(config);
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuildConfig_MBAllocationTooSmall_ExceptionThrown() throws Exception {
    Config config = null;
    try {
      EcoParser ecoParser = new EcoParser();
      InputStream inputStream = new ByteArrayInputStream(INCORRECT_RAM_MEGABYTES.getBytes());
      FileInputStream mockPropsStream = PowerMockito.mock(FileInputStream.class);
      EcoTopologyDefinition ecoTopologyDefinition =
          ecoParser.parseFromInputStream(inputStream, mockPropsStream, false);

      config = subject.buildConfig(ecoTopologyDefinition);
    } finally {
      assertNull(config);
    }
  }

}
