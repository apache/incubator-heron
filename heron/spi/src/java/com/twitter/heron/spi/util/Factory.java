package com.twitter.heron.spi.util;

import com.twitter.heron.spi.packing.IPackingAlgorithm;
import com.twitter.heron.spi.scheduler.IConfigLoader;
import com.twitter.heron.spi.scheduler.ILauncher;
import com.twitter.heron.spi.scheduler.IRuntimeManager;
import com.twitter.heron.spi.scheduler.IScheduler;
import com.twitter.heron.spi.uploader.IUploader;
import com.twitter.heron.spi.statemgr.IStateManager;

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
}
