package com.twitter.heron.scheduler.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.scheduler.api.IConfigLoader;
import com.twitter.heron.scheduler.api.ILauncher;
import com.twitter.heron.scheduler.api.IPackingAlgorithm;
import com.twitter.heron.scheduler.api.IRuntimeManager;
import com.twitter.heron.scheduler.api.IScheduler;
import com.twitter.heron.scheduler.api.IUploader;
import com.twitter.heron.state.IStateManager;

public class Factory {
  public static IStateManager makeStateManager(String stateManagerClass) throws
      ClassNotFoundException, InstantiationException, IllegalAccessException {
    return (IStateManager) Class.forName(stateManagerClass).newInstance();
  }

  public static IConfigLoader makeConfigLoader(String configLoaderClass) throws
      ClassNotFoundException, InstantiationException, IllegalAccessException {
    return (IConfigLoader) Class.forName(configLoaderClass).newInstance();
  }

  public static IScheduler makeScheduler(String schedulerClass) throws
      ClassNotFoundException, IllegalAccessException, InstantiationException {
    return (IScheduler) Class.forName(schedulerClass).newInstance();
  }

  public static IUploader makeUploader(String uploaderClass) throws
      ClassNotFoundException, IllegalAccessException, InstantiationException {
    return (IUploader) Class.forName(uploaderClass).newInstance();
  }

  public static ILauncher makeLauncher(String launcherClass) throws
      ClassNotFoundException, IllegalAccessException, InstantiationException {
    return (ILauncher) Class.forName(launcherClass).newInstance();
  }

  public static IRuntimeManager makeRuntimeManager(String runtimeManagerClass) throws
      ClassNotFoundException, IllegalAccessException, InstantiationException {
    return (IRuntimeManager) Class.forName(runtimeManagerClass).newInstance();
  }

  public static IPackingAlgorithm makePackingAlgorithm(String packingAlgorithmClass) throws
      ClassNotFoundException, IllegalAccessException, InstantiationException {
    return (IPackingAlgorithm) Class.forName(packingAlgorithmClass).newInstance();
  }

  public static IRuntimeManager.Command makeCommand(String commandString) {
    return IRuntimeManager.Command.valueOf(commandString.toUpperCase());
  }

  public static TopologyAPI.Topology getTopology(String topologyDefnFile) {
    try {
      byte[] topologyDefn = Files.readAllBytes(Paths.get(topologyDefnFile));
      TopologyAPI.Topology topology = TopologyAPI.Topology.parseFrom(topologyDefn);
      if (!TopologyUtility.verifyTopology(topology)) {
        throw new RuntimeException("Topology object is Malformed");
      }

      return topology;
    } catch (IOException e) {
      throw new RuntimeException("Failed to read/parse content of " + topologyDefnFile);
    }
  }
}
