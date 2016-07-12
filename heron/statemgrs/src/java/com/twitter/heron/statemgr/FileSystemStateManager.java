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

package com.twitter.heron.statemgr;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.statemgr.IStateManager;

public abstract class FileSystemStateManager implements IStateManager {
  private static final Logger LOG = Logger.getLogger(FileSystemStateManager.class.getName());

  // Store the root address of the hierarchical file system
  protected String rootAddress;

  @Override
  public void initialize(Config config) {
    this.rootAddress = Context.stateManagerRootPath(config);
    LOG.log(Level.FINE, "File system state manager root address: {0}", rootAddress);
  }

  protected String getTMasterLocationDir() {
    return concatPath(rootAddress, "tmasters");
  }

  protected String getTopologyDir() {
    return concatPath(rootAddress, "topologies");
  }

  protected String getPhysicalPlanDir() {
    return concatPath(rootAddress, "pplans");
  }

  protected String getExecutionStateDir() {
    return concatPath(rootAddress, "executionstate");
  }

  protected String getSchedulerLocationDir() {
    return concatPath(rootAddress, "schedulers");
  }

  protected String getTMasterLocationPath(String topologyName) {
    return concatPath(getTMasterLocationDir(), topologyName);
  }

  protected String getTopologyPath(String topologyName) {
    return concatPath(getTopologyDir(), topologyName);
  }

  protected String getPhysicalPlanPath(String topologyName) {
    return concatPath(getPhysicalPlanDir(), topologyName);
  }

  protected String getExecutionStatePath(String topologyName) {
    return concatPath(getExecutionStateDir(), topologyName);
  }

  protected String getSchedulerLocationPath(String topologyName) {
    return concatPath(getSchedulerLocationDir(), topologyName);
  }

  private static String concatPath(String basePath, String appendPath) {
    return String.format("%s/%s", basePath, appendPath);
  }
}
