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

package org.apache.heron.eco;

import java.io.FileInputStream;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.api.mockito.PowerMockito;

import org.apache.heron.api.Config;
import org.apache.heron.api.HeronTopology;
import org.apache.heron.eco.definition.EcoTopologyDefinition;
import org.apache.heron.eco.parser.EcoParser;
import org.apache.heron.eco.submit.EcoSubmitter;
import org.apache.storm.generated.StormTopology;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(MockitoJUnitRunner.class)
public class EcoTest {

  @Mock
  private EcoParser mockEcoParser;
  @Mock
  private EcoSubmitter mockEcoSubmitter;
  @InjectMocks
  private Eco subject;

  @After
  public void ensureNoUnexpectedMockInteractions() {
    Mockito.verifyNoMoreInteractions(mockEcoParser,
        mockEcoSubmitter);
  }

  @Test
  public void testSubmit_StormTopologyType_BehavesAsExpected() throws Exception {
    FileInputStream mockStream = PowerMockito.mock(FileInputStream.class);
    FileInputStream mockPropsStream = PowerMockito.mock(FileInputStream.class);

    final String topologyName = "the name";
    EcoTopologyDefinition topologyDefinition = new EcoTopologyDefinition();
    topologyDefinition.setName(topologyName);

    when(mockEcoParser.parseFromInputStream(eq(mockStream), eq(mockPropsStream), eq(false)))
        .thenReturn(topologyDefinition);

    subject.submit(mockStream, mockPropsStream, false);

    verify(mockEcoParser).parseFromInputStream(same(mockStream),
        same(mockPropsStream), eq(false));

    verify(mockEcoSubmitter).submitStormTopology(any(String.class), any(Config.class),
        any(StormTopology.class));
  }

  @Test
  public void testSubmit_HeronTopologyType_BehavesAsExpected() throws Exception {
    FileInputStream mockStream = PowerMockito.mock(FileInputStream.class);
    FileInputStream mockPropsStream = PowerMockito.mock(FileInputStream.class);

    final String topologyName = "the name";
    EcoTopologyDefinition topologyDefinition = new EcoTopologyDefinition();
    topologyDefinition.setName(topologyName);
    topologyDefinition.setType("heron");

    when(mockEcoParser.parseFromInputStream(eq(mockStream), eq(mockPropsStream), eq(false)))
        .thenReturn(topologyDefinition);

    subject.submit(mockStream, mockPropsStream, false);

    verify(mockEcoParser).parseFromInputStream(same(mockStream),
        same(mockPropsStream), eq(false));

    verify(mockEcoSubmitter).submitHeronTopology(any(String.class), any(Config.class),
        any(HeronTopology.class));
  }
}
