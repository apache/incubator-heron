package com.twitter.heron.scheduler.reef;

import com.twitter.heron.proto.scheduler.Scheduler.KillTopologyRequest;
import com.twitter.heron.proto.scheduler.Scheduler.RestartTopologyRequest;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.PackingPlan;
import com.twitter.heron.spi.scheduler.IScheduler;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * {@link ReefScheduler} in invoked by Heron Scheduler to perform topology actions on REEF cluster. This instance will
 * delegate all topology management functions to {@link HeronMasterDriver}.
 */
public class ReefScheduler implements IScheduler {
  private static final Logger LOG = Logger.getLogger(ReefScheduler.class.getName());

  private HeronMasterDriver driver;

  @Override
  public void initialize(Config config, Config runtime) {
    this.driver = HeronMasterDriver.getInstance();
  }

  @Override
  public boolean onSchedule(PackingPlan packing) {
    LOG.log(Level.INFO, "Launching topology master for packing: {0}", packing.id);
    driver.scheduleTopologyMaster();
    driver.scheduleHeronWorkers(packing);
    return true;
  }

  @Override
  public boolean onKill(KillTopologyRequest request) {
    driver.killTopology();
    return true;
  }

  @Override
  public boolean onRestart(RestartTopologyRequest request) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void close() {
    //TODO
  }
}
