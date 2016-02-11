package com.twitter.heron.spi.scheduler.context;

import java.util.Map;

import com.twitter.heron.spi.scheduler.IConfigLoader;
import com.twitter.heron.spi.scheduler.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.util.Factory;
import com.twitter.heron.spi.statemgr.IStateManager;

public class Context {
  private final IConfigLoader configLoader;
  private final IStateManager stateManager;
  private final SchedulerStateManagerAdaptor adaptor;
  private final String topologyName;
  private final Map<Object, Object> config;

  public Context(IConfigLoader configLoader, String topologyName)
      throws InstantiationException, IllegalAccessException, ClassNotFoundException {
    this.configLoader = configLoader;
    this.topologyName = topologyName;
    this.config = configLoader.getConfig();

    String stateManagerClass = getStateManagerClass();

    stateManager = getContextStateManager(stateManagerClass);

    adaptor = new SchedulerStateManagerAdaptor(stateManager, topologyName);
  }

  protected IStateManager getContextStateManager(String stateManagerClass)
      throws InstantiationException, IllegalAccessException, ClassNotFoundException {
    return Factory.makeStateManager(stateManagerClass);
  }

  public boolean start() {
    stateManager.initialize(config);

    return true;
  }

  public boolean close() {
    stateManager.close();

    return true;
  }

  public SchedulerStateManagerAdaptor getStateManagerAdaptor() {
    if (adaptor == null) {
      throw new IllegalStateException("StateManagerAdaptor not yet initialized.");
    }

    return adaptor;
  }

  public Map<Object, Object> getConfig() {
    return config;
  }

  public String getTopologyName() {
    return topologyName;
  }

  public String getUploaderClass() {
    return configLoader.getUploaderClass();
  }

  public String getLauncherClass() {
    return configLoader.getLauncherClass();
  }

  public String getSchedulerClass() {
    return configLoader.getSchedulerClass();
  }

  public String getRuntimeManagerClass() {
    return configLoader.getRuntimeManagerClass();
  }

  public String getPackingAlgorithmClass() {
    return configLoader.getPackingAlgorithmClass();
  }

  public String getStateManagerClass() {
    return configLoader.getStateManagerClass();
  }

  public boolean isVerbose() {
    return configLoader.isVerbose();
  }

  public void addProperty(String key, String value) {
    config.put(key, value);
  }

  public boolean containProperty(String key) {
    return config.containsKey(key);
  }

  public String getProperty(String key, String defaultValue) {
    if (config.containsKey(key)) {
      return (String) config.get(key);
    }

    return defaultValue;
  }

  public String getProperty(String key) {
    if (config.containsKey(key)) {
      return (String) config.get(key);
    }

    return null;
  }

  public String getPropertyWithException(String key) {
    if (config.containsKey(key)) {
      return (String) config.get(key);
    }

    throw new RuntimeException("Missing required key: " + key + " in config");
  }
}
