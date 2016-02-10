package com.twitter.heron.scheduler.util;

import java.util.HashMap;

import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.proto.system.ExecutionEnvironment;

import com.twitter.heron.spi.common.PackingPlan;
import com.twitter.heron.spi.packing.IPackingAlgorithm;
import com.twitter.heron.spi.scheduler.ILauncher;
import com.twitter.heron.spi.scheduler.IScheduler;
import com.twitter.heron.spi.scheduler.context.LaunchContext;
import com.twitter.heron.spi.uploader.IUploader;


/**
 * Empty implementation of Uploader, Launcher and Scheduler.
 */
public class Nullity {

  public static class EmptyPacking implements IPackingAlgorithm {
    public PackingPlan pack(LaunchContext context) {
      return new PackingPlan(
          "",
          new HashMap<String, PackingPlan.ContainerPlan>(),
          new PackingPlan.Resource(0.0, 0L, 0L));
    }
  }

  public static class NullUploader implements IUploader {
    @Override
    public void initialize(LaunchContext context) {
    }

    @Override
    public boolean uploadPackage(String topologyPackage) {
      return true;
    }

    @Override
    public void undo() {
    }
  }

  public static class NullLauncher implements ILauncher {
    @Override
    public void initialize(LaunchContext context) {
    }

    @Override
    public boolean prepareLaunch(PackingPlan packing) {
      return true;
    }

    @Override
    public boolean launchTopology(PackingPlan packing) {
      return true;
    }

    @Override
    public boolean postLaunch(PackingPlan packing) {
      return true;
    }

    @Override
    public void undo() {
    }

    @Override
    public ExecutionEnvironment.ExecutionState updateExecutionState(
        ExecutionEnvironment.ExecutionState executionState) {
      return executionState;
    }
  }

  public static class NullScheduler implements IScheduler {
    @Override
    public void initialize(LaunchContext context) {
    }

    @Override
    public void schedule(PackingPlan packing) {
    }

    @Override
    public void onHealthCheck(String healthCheckResponse) {
    }

    @Override
    public boolean onKill(Scheduler.KillTopologyRequest request) {
      return true;
    }

    @Override
    public boolean onActivate(Scheduler.ActivateTopologyRequest request) {
      return true;
    }

    @Override
    public boolean onDeactivate(Scheduler.DeactivateTopologyRequest request) {
      return true;
    }

    @Override
    public boolean onRestart(Scheduler.RestartTopologyRequest request) {
      return true;
    }
  }
}
