package com.twitter.heron.scheduler.aurora;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.twitter.heron.scheduler.api.Constants;
import com.twitter.heron.scheduler.util.DefaultConfigLoader;

public class AuroraConfigLoader extends DefaultConfigLoader {
  public static final String AURORA_SCHEDULER_CONF = "aurora_scheduler.conf";
  public static final String AURORA_BIND_CONF = "aurora_bind.conf";

  private static final Logger LOG = Logger.getLogger(AuroraConfigLoader.class.getName());
  private static final ObjectMapper mapper = new ObjectMapper();

  @Override
  public boolean load(String configPath, String configOverride) {
    File schedulerConfFile = Paths.get(configPath, AURORA_SCHEDULER_CONF).toFile();
    File bindConfFile = Paths.get(configPath, AURORA_BIND_CONF).toFile();

    // The if condition must be evaluated in order to ensure
    // the correct overriding logic from the lowest to the highest:
    //    aurora_scheduler.conf
    //    cluster.conf
    //    cmdline option --config-property
    if (super.load(schedulerConfFile.toString(), configOverride) &&
        loadProperties(properties, getClusterConfFile(configPath)) &&
        applyConfigPropertyOverride()) {
      Properties bindProperties = loadProperties(bindConfFile);
      return addAuroraBindProperties(bindProperties);
    } else {
      return false;
    }
  }

  /* Get the string path: <HERON_CONFIG_PATH>/cluster/<CLUSTER>.conf
   */
  private File getClusterConfFile(String configPath) {
    String cluster = properties.get(Constants.DC).toString();
    return Paths.get(configPath, "cluster", String.format("%s.conf", cluster)).toFile();
  }

  /* Loads a Java properties file into a specified Properties p.
   */
  private boolean loadProperties(Properties p, File file) {
    if (file != null && file.exists() && file.isFile()) {
      try {
        p.load(new FileInputStream(file));
      } catch (IOException e) {
        LOG.log(Level.SEVERE, "Failed to load properties file: " + file, e);
        return false;
      }
      return true;
    } else {
      LOG.log(Level.SEVERE, "Failed to load properties file: " + file);
      return false;
    }
  }

  /* Loads a Java properties file.
   * Returns an empty properties if the file is not found or not properly formatted.
   */
  private Properties loadProperties(File file) {
    Properties p = new Properties();
    if (file != null && file.exists() && file.isFile()) {
      try {
        p.load(new FileInputStream(file));
      } catch (IOException e) {
        LOG.log(Level.SEVERE, "Failed to load properties file: " + file, e);
      }
    }
    return p;
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
  private boolean addAuroraBindProperties(Properties bindProperties) {
    for (String bindKey: bindProperties.stringPropertyNames()) {
      String key = bindProperties.getProperty(bindKey);
      if (properties.containsKey(key)) {
        String key2 = Constants.HERON_AURORA_BIND_PREFIX + bindKey;
        properties.put(key2, properties.get(key));
      } else {
        LOG.log(Level.SEVERE, "Value not found for property key: " + key);
        return false;
      }
    }

    return true;
  }

  @Override
  public boolean applyConfigOverride(String configOverride) {
    // Follow the style dc/role/environ
    String[] parts = configOverride.trim().split(" ", 2);
    if (parts.length == 0) {
      LOG.severe("dc/role/environ is required.");
      return false;
    }
    String clusterInfo = parts[0];
    String[] clusterParts = clusterInfo.split("/");
    if (clusterParts.length != 3) {
      LOG.severe("Cluster parts must be dc/role/environ (without spaces)");
      return false;
    }

    properties.setProperty(Constants.DC, clusterParts[0]);
    properties.setProperty(Constants.ROLE, clusterParts[1]);
    properties.setProperty(Constants.ENVIRON, clusterParts[2]);
    if (parts.length == 2 && !parts[1].isEmpty()) {
      if (!super.applyConfigOverride(parts[1])) {
        return false;
      }
    }

    return true;
  }
}
