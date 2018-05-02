/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.heron.spi.common;

import java.time.Duration;
import java.time.temporal.TemporalUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Logger;

import org.apache.heron.common.basics.ByteAmount;
import org.apache.heron.common.basics.DryRunFormatType;
import org.apache.heron.common.basics.PackageType;
import org.apache.heron.common.basics.TypeUtils;

/**
 * Config is an Immutable Map of &lt;String, Object&gt; The get/set API that uses Key objects
 * should be favored over Strings. Usage of the String API should be refactored out.
 *
 * A newly created Config object holds configs that might include wildcard tokens, like
 * ${HERON_HOME}/bin, ${HERON_LIB}/packing/*. Token substitution can be done by converting that
 * config to a local or cluster config by using the {@code Config.toLocalMode} or
 * {@code Config.toClusterMode} methods.
 *
 * Local mode is for a config to be used to run Heron locally, where HERON_HOME might be an install
 * dir on the local host (e.g. HERON_HOME=/usr/bin/heron). Cluster mode is to be used when building
 * configs for a remote process run on a service, where all directories might be relative to the
 * current dir by default (e.g. HERON_HOME=~/heron-core).
 */
public class Config {
  private static final Logger LOG = Logger.getLogger(Config.class.getName());

  private final Map<String, Object> cfgMap;

  private enum Mode {
    RAW,    // the initially provided configs without pattern substitution
    LOCAL,  // the provided configs with pattern substitution for the local (i.e., client) env
    CLUSTER // the provided configs with pattern substitution for the cluster (i.e., remote) env
  }

  // Used to initialize a raw config. Should be used by consumers of Config via the builder
  protected Config(Builder build) {
    this.mode = Mode.RAW;
    this.rawConfig = this;
    this.cfgMap = new HashMap<>(build.keyValues);
  }

  // Used internally to create a Config that is actually a facade over a raw, local and
  // cluster config
  private Config(Mode mode, Config rawConfig, Config localConfig, Config clusterConfig) {
    this.mode = mode;
    this.rawConfig = rawConfig;
    this.localConfig = localConfig;
    this.clusterConfig = clusterConfig;
    switch (mode) {
      case RAW:
        this.cfgMap = rawConfig.cfgMap;
        break;
      case LOCAL:
        this.cfgMap = localConfig.cfgMap;
        break;
      case CLUSTER:
        this.cfgMap = clusterConfig.cfgMap;
        break;
      default:
        throw new IllegalArgumentException("Unrecognized mode passed to constructor: " + mode);
    }
  }

  public static Builder newBuilder() {
    return newBuilder(false);
  }

  public static Builder newBuilder(boolean loadDefaults) {
    return Builder.create(loadDefaults);
  }

  public static Config toLocalMode(Config config) {
    return config.lazyCreateConfig(Mode.LOCAL);
  }

  public static Config toClusterMode(Config config) {
    return config.lazyCreateConfig(Mode.CLUSTER);
  }

  private static Config expand(Config config) {
    return expand(config, 0);
  }

  /**
   * Recursively expand each config value until token substitution is exhausted. We must recurse
   * to handle the case where field expansion requires multiple iterations, due to new tokens being
   * introduced as we replace. For example:
   *
   *   ${HERON_BIN}/heron-executor        gets expanded to
   *   ${HERON_HOME}/bin/heron-executor   gets expanded to
   *   /usr/local/heron/bin/heron-executor
   *
   * If break logic is when another round does not reduce the number of tokens, since it means we
   * couldn't find a valid replacement.
   */
  private static Config expand(Config config, int previousTokensCount) {
    Config.Builder cb = Config.newBuilder().putAll(config);
    int tokensCount = 0;
    for (String key : config.getKeySet()) {
      Object value = config.get(key);
      if (value instanceof String) {
        String expandedValue = TokenSub.substitute(config, (String) value);
        if (expandedValue.contains("${")) {
          tokensCount++;
        }
        cb.put(key, expandedValue);
      } else {
        cb.put(key, value);
      }
    }
    if (previousTokensCount != tokensCount) {
      return expand(cb.build(), tokensCount);
    } else {
      return cb.build();
    }
  }

  private final Mode mode;
  private final Config rawConfig;     // what the user first creates
  private Config localConfig = null;  // what gets generated during toLocalMode
  private Config clusterConfig = null; // what gets generated during toClusterMode

  private Config lazyCreateConfig(Mode newMode) {
    if (newMode == this.mode) {
      return this;
    }

    // this is here so that we don't keep cascading deeper into object creation so:
    // localConfig == toLocalMode(toClusterMode(localConfig))
    Config newRawConfig = this.rawConfig;
    Config newLocalConfig = this.localConfig;
    Config newClusterConfig = this.clusterConfig;
    switch (this.mode) {
      case RAW:
        newRawConfig = this;
        break;
      case LOCAL:
        newLocalConfig = this;
        break;
      case CLUSTER:
        newClusterConfig = this;
        break;
      default:
        throw new IllegalArgumentException(
            "Unrecognized mode found in config: " + this.mode);
    }

    switch (newMode) {
      case LOCAL:
        if (this.localConfig == null) {
          Config tempConfig = Config.expand(Config.newBuilder().putAll(rawConfig.cfgMap).build());
          this.localConfig = new Config(Mode.LOCAL, newRawConfig, tempConfig, newClusterConfig);
        }
        return this.localConfig;
      case CLUSTER:
        if (this.clusterConfig == null) {
          Config.Builder bc = Config.newBuilder()
              .putAll(rawConfig.cfgMap)
              .put(Key.HERON_HOME, get(Key.HERON_CLUSTER_HOME))
              .put(Key.HERON_CONF, get(Key.HERON_CLUSTER_CONF));
          Config tempConfig = Config.expand(bc.build());
          this.clusterConfig = new Config(Mode.CLUSTER, newRawConfig, newLocalConfig, tempConfig);
        }
        return this.clusterConfig;
      case RAW:
      default:
        throw new IllegalArgumentException(
            "Unrecognized mode passed to lazyCreateConfig: " + newMode);
    }
  }

  public int size() {
    return cfgMap.size();
  }

  public Object get(Key key) {
    return get(key.value());
  }

  private Object get(String key) {
    switch (mode) {
      case LOCAL:
        return localConfig.cfgMap.get(key);
      case CLUSTER:
        return clusterConfig.cfgMap.get(key);
      case RAW:
        return rawConfig.cfgMap.get(key);
      default:
        throw new IllegalArgumentException(String.format(
            "Unrecognized mode passed to get for key=%s: %s", key, mode));
    }
  }

  public String getStringValue(String key) {
    return (String) get(key);
  }

  public String getStringValue(Key key) {
    return (String) get(key);
  }

  public String getStringValue(String key, String defaultValue) {
    String value = getStringValue(key);
    return value != null ? value : defaultValue;
  }

  public Boolean getBooleanValue(Key key) {
    return (Boolean) get(key);
  }

  private Boolean getBooleanValue(String key) {
    return (Boolean) get(key);
  }

  public Boolean getBooleanValue(String key, boolean defaultValue) {
    Boolean value = getBooleanValue(key);
    return value != null ? value : defaultValue;
  }

  public ByteAmount getByteAmountValue(Key key) {
    Object value = get(key);
    return TypeUtils.getByteAmount(value);
  }

  DryRunFormatType getDryRunFormatType(Key key) {
    return (DryRunFormatType) get(key);
  }

  public PackageType getPackageType(Key key) {
    return (PackageType) get(key);
  }

  public Long getLongValue(Key key) {
    Object value = get(key);
    return TypeUtils.getLong(value);
  }

  public Long getLongValue(String key, long defaultValue) {
    Object value = get(key);
    if (value != null) {
      return TypeUtils.getLong(value);
    }
    return defaultValue;
  }

  public Integer getIntegerValue(Key key) {
    Object value = get(key);
    return TypeUtils.getInteger(value);
  }

  public Integer getIntegerValue(String key, int defaultValue) {
    Object value = get(key);
    if (value != null) {
      return TypeUtils.getInteger(value);
    }
    return defaultValue;
  }

  public Double getDoubleValue(Key key) {
    Object value = get(key);
    return TypeUtils.getDouble(value);
  }

  public Duration getDurationValue(String key, TemporalUnit unit, Duration defaultValue) {
    Object value = get(key);
    if (value != null) {
      return TypeUtils.getDuration(value, unit);
    }
    return defaultValue;
  }

  public boolean containsKey(Key key) {
    return cfgMap.containsKey(key.value());
  }

  public Set<String> getKeySet() {
    return cfgMap.keySet();
  }

  public Set<Map.Entry<String, Object>> getEntrySet() {
    return cfgMap.entrySet();
  }

  @Override
  public String toString() {
    Map<String, Object> treeMap = new TreeMap<>(cfgMap);
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, Object> entry : treeMap.entrySet()) {
      sb.append("(\"").append(entry.getKey()).append("\"");
      sb.append(", ").append(entry.getValue()).append(")\n");
    }
    return sb.toString();
  }

  public static class Builder {
    private final Map<String, Object> keyValues = new HashMap<>();

    private static Config.Builder create(boolean loadDefaults) {
      Config.Builder cb = new Builder();

      if (loadDefaults) {
        loadDefaults(cb, Key.values());
      }

      return cb;
    }

    private static void loadDefaults(Config.Builder cb, Key... keys) {
      for (Key key : keys) {
        if (key.getDefault() != null) {
          cb.put(key, key.getDefault());
        }
      }
    }

    public Builder put(String key, Object value) {
      this.keyValues.put(key, value);
      return this;
    }

    public Builder put(Key key, Object value) {
      put(key.value(), value);
      return this;
    }

    public Builder putAll(Config ctx) {
      keyValues.putAll(ctx.cfgMap);
      return this;
    }

    public Builder putAll(Map<String, Object> map) {
      keyValues.putAll(map);
      return this;
    }

    public Config build() {
      return new Config(this);
    }
  }
}
