package com.twitter.heron.metricsmgr;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

public class MetricsSinksConfig {
  public static final String CONFIG_KEY_METRICS_SINKS = "sinks";
  public static final String CONFIG_KEY_CLASSNAME = "class";
  public static final String CONFIG_KEY_FLUSH_FREQUENCY_MS = "flush-frequency-ms";
  public static final String CONFIG_KEY_SINK_RESTART_ATTEMPTS = "sink-restart-attempts";
  public static final int DEFAULT_SINK_RESTART_ATTEMPTS = 0;

  private final Map<String, Map<String, Object>> sinksConfigs = new HashMap<String, Map<String, Object>>();

  public MetricsSinksConfig(String filename) throws FileNotFoundException {
    FileInputStream fin = new FileInputStream(new File(filename));
    Yaml yaml = new Yaml();
    Map ret = (Map) yaml.load(fin);

    if (ret == null) {
      throw new RuntimeException("Could not parse metrics-sinks config file");
    } else {
      for (String sinkId : (List<String>) ret.get(CONFIG_KEY_METRICS_SINKS)) {
        sinksConfigs.put(sinkId, (Map<String, Object>) ret.get(sinkId));
      }
    }
  }

  public void setConfig(String key, Object value) {
    for (Map<String, Object> config : sinksConfigs.values()) {
      config.put(key, value);
    }
  }

  @Override
  public String toString() {
    return sinksConfigs.toString();
  }

  public int getNumberOfSinks() {
    return sinksConfigs.keySet().size();
  }

  public Map<String, Object> getConfigForSink(String sinkId) {
    return sinksConfigs.get(sinkId);
  }

  public List<String> getSinkIds() {
    return new ArrayList<String>(sinksConfigs.keySet());
  }
}
