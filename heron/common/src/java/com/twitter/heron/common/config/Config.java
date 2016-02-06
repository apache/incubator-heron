package com.twitter.heron.common.config;

public class Config {
  private final String cluster;
  private final String role;
  private final String env;
  private final String topologyName;
  private final String configPath;

  private Config(Builder builder) {
    this.cluster = builder.mCluster;
    this.role = builder.mRole;
    this.env = builder.mEnv;
    this.topologyName = builder.mTopologyName;
    this.configPath = builder.mConfigPath;
  }

  public static class Builder {
    String mCluster;
    String mRole;
    String mEnv;
    String mTopologyName;
    String mConfigPath;

    public Builder setCluster(String cluster) {
      this.mCluster = cluster;
      return this;
    }

    public Builder setRole(String role) {
      this.mRole = role;
      return this;
    }

    public Builder setEnv(String env) {
      this.mEnv = env;
      return this;
    }

    public Builder setTopologyName(String topologyName) {
      this.mTopologyName = topologyName;
      return this;
    }

    public Builder setConfigPath(String configPath) {
      this.mConfigPath = configPath;
      return this;
    }

    public Config build() {
      return new Config(this);
    }
  }

  public String getCluster() {
    return this.cluster;
  }

  public String getRole() {
    return this.role;
  }

  public String getEnv() {
    return this.role;
  }

  public String getTopologyName() {
    return this.topologyName;
  }

  public String getConfigPath() {
    return this.getConfigPath();
  }
}
