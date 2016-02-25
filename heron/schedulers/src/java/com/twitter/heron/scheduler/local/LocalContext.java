package com.twitter.heron.scheduler.local;

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Misc;

public class LocalContext {
  public static String workingDirectory(Config cfg) {
    String workingDirectory = cfg.getStringValue(
        LocalSchedulerKeys.WORKING_DIRECTORY, 
        LocalSchedulerDefaults.WORKING_DIRECTORY);
    return return Misc.substitute(cfg, workingDirectory);
  }

  public static String corePackageUri(Config cfg) {
    String packageUri = cfg.getStringValue(LocalSchedulerKeys.CORE_PACKAGE_URI, 
        LocalSchedulerDefaults.CORE_PACKAGE_URI);
    return Misc.substitute(cfg, packageUri);
  }
}
