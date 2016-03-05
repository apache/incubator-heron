package com.twitter.heron.scheduler.local;

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.Misc;

public class LocalContext extends Context {
  public static String workingDirectory(Config config) {
    String workingDirectory = config.getStringValue(
        LocalKeys.get("WORKING_DIRECTORY"), LocalDefaults.get("WORKING_DIRECTORY"));
    return Misc.substitute(config, workingDirectory);
  }
}
