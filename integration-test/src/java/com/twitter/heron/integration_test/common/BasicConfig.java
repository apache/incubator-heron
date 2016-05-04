package com.twitter.heron.integration_test.common;

import com.twitter.heron.api.Config;

/**
 * A basic configuration for heron topology
 */
public class BasicConfig extends Config {
  private static final long serialVersionUID = -3583884076092048052L;
  final static int DEFAULT_NUM_STMGRS = 1;

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
