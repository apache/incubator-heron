// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.twitter.heron.integration_test.common;

import com.twitter.heron.api.Config;

/**
 * A basic configuration for heron topology
 */
public class BasicConfig extends Config {
  private static final long serialVersionUID = -3583884076092048052L;
  private static final int DEFAULT_NUM_STMGRS = 1;

  public BasicConfig() {
    this(true, DEFAULT_NUM_STMGRS);
  }

  public BasicConfig(boolean isDebug, int numStmgrs) {
    super();
    super.setTeamEmail("streaming-compute@twitter.com");
    super.setTeamName("stream-computing");
    super.setTopologyProjectName("heron-integration-test");
    super.setDebug(isDebug);
    super.setNumStmgrs(numStmgrs);
    super.setEnableAcking(true);
  }
}
