package com.twitter.heron.spi.statemgr;

import java.util.Map;

public abstract class FileSystemStateManager implements IStateManager {
  public static final String ROOT_ADDRESS = "state.root.address";

  protected String rootAddress;

  @Override
  public void initialize(Map<Object, Object> conf) {
    this.rootAddress = (String) conf.get(ROOT_ADDRESS);
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
