package com.twitter.heron.statemgr;

import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.statemgr.IStateManager;

public abstract class FileSystemStateManager implements IStateManager {
  public static final String ROOT_ADDRESS = "state.root.address";

  protected String rootAddress;

  @Override
  public void initialize(Context cxt) {
    this.rootAddress = cxt.getStringValue(ROOT_ADDRESS);
  }

  protected String getTMasterLocationDir() {
    return rootAddress + "/tmasters";
  }

  protected String getTopologyDir() {
    return rootAddress + "/topologies";
  }

  protected String getPhysicalPlanDir() {
    return rootAddress + "/pplans";
  }

  protected String getExecutionStateDir() {
    return rootAddress + "/executionstate";
  }

  protected String getSchedulerLocationDir() {
    return rootAddress + "/schedulers";
  }

  protected String getTMasterLocationPath(String topologyName) {
    return getTMasterLocationDir() + "/" + topologyName;
  }

  protected String getTopologyPath(String topologyName) {
    return String.format("%s/%s", getTopologyDir(), topologyName);
  }

  protected String getPhysicalPlanPath(String topologyName) {
    return String.format("%s/%s", getPhysicalPlanDir(), topologyName);
  }

  protected String getExecutionStatePath(String topologyName) {
    return String.format("%s/%s", getExecutionStateDir(), topologyName);
  }

  protected String getSchedulerLocationPath(String topologyName) {
    return String.format("%s/%s", getSchedulerLocationDir(), topologyName);
  }
}
