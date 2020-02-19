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

package org.apache.heron.eco.submit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import org.apache.heron.api.Config;
import org.apache.heron.api.HeronSubmitter;
import org.apache.heron.api.HeronTopology;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.internal.verification.VerificationModeFactory.times;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("jdk.internal.reflect.*")
@PrepareForTest({StormSubmitter.class, HeronSubmitter.class})
public class EcoSubmitterTest {

  private EcoSubmitter subject;

  @Before
  public void setUpForEachTestCase() {
    subject = new EcoSubmitter();
  }

  @Test
  public void submitStormTopology_AllGood_BehavesAsExpected()
      throws Exception {
    Config config = new Config();
    StormTopology topology = new StormTopology();
    PowerMockito.spy(StormSubmitter.class);
    PowerMockito.doNothing().when(StormSubmitter.class, "submitTopology",
        any(String.class), any(Config.class), any(StormTopology.class));

    subject.submitStormTopology("name", config, topology);
    PowerMockito.verifyStatic(times(1));
    StormSubmitter.submitTopology(anyString(), any(Config.class), any(StormTopology.class));

  }

  @Test
  public void submitHeronTopology_AllGood_BehavesAsExpected()
      throws Exception {
    Config config = new Config();
    HeronTopology topology = new HeronTopology(null);
    PowerMockito.spy(HeronSubmitter.class);
    PowerMockito.doNothing().when(HeronSubmitter.class, "submitTopology",
        any(String.class), any(Config.class), any(HeronTopology.class));

    subject.submitHeronTopology("name", config, topology);
    PowerMockito.verifyStatic(times(1));
    HeronSubmitter.submitTopology(anyString(), any(Config.class), any(HeronTopology.class));

  }
}
