package com.twitter.heron.scheduler.aurora;

import java.io.File;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.spi.common.Constants;
import com.twitter.heron.scheduler.util.AbstractPropertiesConfigLoader;
import com.twitter.heron.scheduler.util.ConfigLoaderUtils;
import com.twitter.heron.scheduler.util.PropertiesFileConfigLoader;

public class AuroraConfigLoader extends AbstractPropertiesConfigLoader {
  public static final String AURORA_SCHEDULER_CONF = "aurora_scheduler.conf";
  public static final String AURORA_BIND_CONF = "aurora_bind.conf";

  private static final Logger LOG = Logger.getLogger(AuroraConfigLoader.class.getName());

  @Override
  public boolean load(String configPath, String configOverride) {
    File schedulerConfFile = Paths.get(configPath, AURORA_SCHEDULER_CONF).toFile();
    File bindConfFile = Paths.get(configPath, AURORA_BIND_CONF).toFile();

    // The if condition must be evaluated in order to ensure
    // the correct overriding logic from the lowest to the highest:
    //    aurora_scheduler.conf
    //    cluster.conf
    //    cmdline option --config-property
    PropertiesFileConfigLoader baseLoader = new PropertiesFileConfigLoader();
    if (baseLoader.load(schedulerConfFile.toString(), configOverride)) {
      Properties baseProperties = baseLoader.getProperties();
      Properties bindProperties = new Properties();
      String cluster = baseProperties.getProperty(Constants.CLUSTER);

      if (ConfigLoaderUtils.loadPropertiesFile(baseProperties, getClusterConfFile(configPath, cluster).toString()) &&
          ConfigLoaderUtils.applyConfigPropertyOverride(baseProperties) &&
          ConfigLoaderUtils.loadPropertiesFile(bindProperties, bindConfFile.toString()) &&
          addAuroraBindProperties(baseProperties, bindProperties)) {
        properties.putAll(baseProperties);
        return true;
      }
    }

    return false;
  }

  /* Get the string path: <HERON_CONFIG_PATH>/cluster/<CLUSTER>.conf
   */
  private File getClusterConfFile(String configPath, String clusterName) {
    return Paths.get(configPath, "cluster", String.format("%s.conf", clusterName)).toFile();
  }

  /* Given the following condition
   *   - a bind property definition in aurora_bind.conf:
   *     HERON_PACKAGE: heron.release.package
   *   - a property definition in aurora_scheduler.conf:
   *     heron.release.package: heron-core-release.tar.gz
   *
   * a new property like the following is added:
   *     heron.aurora.bind.HERON_PACKAGE: heron-core-release.tar.gz
   */
  private boolean addAuroraBindProperties(Properties target, Properties bindProperties) {
    for (String bindKey: bindProperties.stringPropertyNames()) {
      String key = bindProperties.getProperty(bindKey);
      if (target.containsKey(key)) {
        String key2 = Constants.HERON_AURORA_BIND_PREFIX + bindKey;
        target.put(key2, target.get(key));
      } else {
        LOG.log(Level.SEVERE, "Value not found for property key: " + key);
        return false;
      }
    }

    return true;
  }
}
