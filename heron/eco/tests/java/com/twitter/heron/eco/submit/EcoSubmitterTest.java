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
package com.twitter.heron.eco.submit;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.internal.verification.VerificationModeFactory.times;

@RunWith(PowerMockRunner.class)
@PrepareForTest(StormSubmitter.class)
public class EcoSubmitterTest {

  private EcoSubmitter subject;

  @Before
  public void setUpForEachTestCase() {
    subject = new EcoSubmitter();
  }

  @Test
  public void submitTopology_AllGood_BehavesAsExpected()
      throws Exception {
    Config config = new Config();
    StormTopology topology = new StormTopology();
    PowerMockito.spy(StormSubmitter.class);
    PowerMockito.doNothing().when(StormSubmitter.class, "submitTopology",
        any(String.class), any(Config.class), any(StormTopology.class));

    subject.submitTopology("name", config, topology );
    PowerMockito.verifyStatic(times(1));
    StormSubmitter.submitTopology(anyString(), any(Config.class), any(StormTopology.class));

  }
}

/*
  PowerMockito.spy(TMasterUtils.class);
    PowerMockito.doNothing().when(TMasterUtils.class, "sendToTMaster",
        any(String.class), eq(TOPOLOGY_NAME),
        eq(mockStateMgr), any(NetworkUtils.TunnelConfig.class));
 */