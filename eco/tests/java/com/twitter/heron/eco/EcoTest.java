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
package com.twitter.heron.eco;

import java.io.FileInputStream;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.api.mockito.PowerMockito;

import com.twitter.heron.api.Config;
import com.twitter.heron.eco.builder.EcoBuilder;
import com.twitter.heron.eco.builder.ObjectBuilder;
import com.twitter.heron.eco.definition.EcoExecutionContext;
import com.twitter.heron.eco.definition.EcoTopologyDefinition;
import com.twitter.heron.eco.parser.EcoParser;
import com.twitter.heron.eco.submit.EcoSubmitter;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(MockitoJUnitRunner.class)
public class EcoTest {

  @Mock
  private EcoBuilder mockEcoBuilder;
  @Mock
  private EcoParser mockEcoParser;
  @Mock
  private TopologyBuilder mockTopologyBuilder;
  @Mock
  private EcoSubmitter mockEcoSubmitter;
  @InjectMocks
  private Eco subject;

  @After
  public void ensureNoUnexpectedMockInteractions() {
    Mockito.verifyNoMoreInteractions(mockEcoBuilder,
        mockEcoParser,
        mockTopologyBuilder,
        mockEcoSubmitter);
  }

  @Test
  public void testSubmit_AllGood_BehavesAsExpected() throws Exception {
    FileInputStream mockStream = PowerMockito.mock(FileInputStream.class);
    FileInputStream mockPropsStream = PowerMockito.mock(FileInputStream.class);

    final String topologyName = "the name";
    EcoTopologyDefinition topologyDefinition = new EcoTopologyDefinition();
    topologyDefinition.setName(topologyName);
    Config config = new Config();

    when(mockEcoParser.parseFromInputStream(eq(mockStream), eq(mockPropsStream)))
        .thenReturn(topologyDefinition);
    when(mockEcoBuilder.buildConfig(eq(topologyDefinition))).thenReturn(config);
    when(mockEcoBuilder.buildTopologyBuilder(any(EcoExecutionContext.class),
        any(ObjectBuilder.class))).thenReturn(mockTopologyBuilder);

    subject.submit(mockStream, mockPropsStream);

    verify(mockEcoParser).parseFromInputStream(same(mockStream), same(mockPropsStream));
    verify(mockEcoBuilder).buildConfig(same(topologyDefinition));
    verify(mockEcoBuilder).buildTopologyBuilder(any(EcoExecutionContext.class),
        any(ObjectBuilder.class));
    verify(mockTopologyBuilder).createTopology();
    verify(mockEcoSubmitter).submitTopology(any(String.class), any(Config.class),
        any(StormTopology.class));
  }
}
