package com.twitter.heron.spi.scheduler;

import java.util.Map;

/**
 * Config loader interface which will be used to parse and load scheduler config.
 */
public interface IConfigLoader {
  /**
   * Loads config file from disk.
   *
   * @param configFile location of scheduler config file.
   * @param cmdlineOverrides Commandline overrides passed to submitter. The override is encoded as
   * String. SubmitterMain and SchedulerMain will pass all extra arguments
   * as override.
   * @return true, If successful in loading config. False if config verify failed.
   */
  boolean load(String configFile, String cmdlineOverrides);

  /**
   * Returns uploader class name from config file.
   *
   * @return uploader class name. (Must implement IUploader)
   */
  String getUploaderClass();

  /**
   * Returns launcher class name from config file.
   *
   * @return launcher class name. (Must implement ILauncher)
   */
  String getLauncherClass();

  /**
   * Returns scheduler class name from config file.
   *
   * @return scheduler class name. (Must implement IScheduler).
   */
  String getSchedulerClass();

  /**
   * Returns IRuntimeManager class name from config file.
   *
   * @return IRuntimeManager class name. (Must implement IRuntimeManager).
   */
  String getRuntimeManagerClass();

  /**
   * Returns packing algorithm class.
   */
  String getPackingAlgorithmClass();

  /**
   * Returns IStateManager class name from config file.
   *
   * @return IStateManager class name. (Must implement IStateManager).
   */
  String getStateManagerClass();

  /**
   * @return true to show the running details.
   */
  boolean isVerbose();

  /**
   * Return Map of (k,v) config loaded via IConfigLoader
   *
   * @return Map of (k,v) config loaded via IConfigLoader
   */
  Map<Object, Object> getConfig();
}
