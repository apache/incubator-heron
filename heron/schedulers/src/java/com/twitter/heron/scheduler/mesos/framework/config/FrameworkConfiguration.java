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

package com.twitter.heron.scheduler.mesos.framework.config;

public final class FrameworkConfiguration {
  private FrameworkConfiguration() {

  }

  /**
   * TODO(mfu): To add more static factory method to instantiate FrameworkConfiguration
   * Sometime similar to:
   * public static getFrameworkConfiguration getFrameworkConfigurationFromJSON(String JSONString) {}
   * public static getFrameworkConfiguration getFrameworkConfigurationFromYAML(String YAMLString) {}
   * Currently we would just construct a empty one and then set it by using public methods.
   */
  public static FrameworkConfiguration getFrameworkConfiguration() {
    return new FrameworkConfiguration();
  }

  /**
   * ---------------------------------------------------------------
   * Mesos related config
   * Start
   * ---------------------------------------------------------------
   */

  public String master;

  public String user;

  public int failoverTimeoutSeconds;

  public String hostname;

  public long failureRetryDelayMs;

  public String schedulerName;

  public String authenticationPrincipal = null;

  public String authenticationSecretFile = null;

  public String role = "*";

  public boolean checkpoint = true;
  /**
   * ---------------------------------------------------------------
   * Mesos related config
   * End
   * ---------------------------------------------------------------
   */

  public String clusterName;
  public long reconciliationIntervalInMs;

  // If isPersist is true, scheduler would persist scheduled jobs to persistence store
  // It would allow scheduler recovers from the state from last failure
  public boolean isPersist = false;
}
