package com.twitter.heron.scheduler.local;

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Misc;

public class LocalContext {
  public static String workingDirectory(Config cfg) {
    String workingDirectory = cfg.getStringValue(
        LocalKeys.WORKING_DIRECTORY, 
        LocalDefaults.WORKING_DIRECTORY);
    return Misc.substitute(cfg, workingDirectory);
  }
}
