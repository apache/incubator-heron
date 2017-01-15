// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.twitter.heron.common.config;

final class Config {
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

  public String getCluster() {
    return this.cluster;
  }

  public String getRole() {
    return this.role;
  }

  public String getEnv() {
    return this.env;
  }

  public String getTopologyName() {
    return this.topologyName;
  }

  public String getConfigPath() {
    return this.configPath;
  }

  public static class Builder {
    private String mCluster;
    private String mRole;
    private String mEnv;
    private String mTopologyName;
    private String mConfigPath;

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
}
