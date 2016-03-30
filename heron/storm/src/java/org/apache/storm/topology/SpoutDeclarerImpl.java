package org.apache.storm.topology;

import java.util.Map;

public class SpoutDeclarerImpl implements SpoutDeclarer {
  private com.twitter.heron.api.topology.SpoutDeclarer delegate;

  public SpoutDeclarerImpl(com.twitter.heron.api.topology.SpoutDeclarer delegate) {
    this.delegate = delegate;
  }

  @Override
  public SpoutDeclarer addConfigurations(Map conf) {
    delegate.addConfigurations(conf);
    return this;
  }

  @Override
  public SpoutDeclarer addConfiguration(String config, Object value) {
    delegate.addConfiguration(config, value);
    return this;
  }

  @Override
  public SpoutDeclarer setDebug(boolean debug) {
    delegate.setDebug(debug);
    return this;
  }

  @Override
  public SpoutDeclarer setMaxTaskParallelism(Number val) {
    // Heron does not support this
    return this;
  }

  @Override
  public SpoutDeclarer setMaxSpoutPending(Number val) {
    delegate.setMaxSpoutPending(val);
    return this;
  }

  @Override
  public SpoutDeclarer setNumTasks(Number val) {
    // Heron does not support this
    return this;
  }
}
