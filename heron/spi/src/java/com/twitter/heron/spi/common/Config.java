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

package com.twitter.heron.spi.common;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Config is an Immutable Map of <String, Object>
 */
// SUPPRESS CHECKSTYLE
public class Config {
  private final Map<String, Object> cfgMap = new HashMap();

  private Config(Builder build) {
    cfgMap.putAll(build.keyValues);
  }

  public static Builder newBuilder() {
    return Builder.create();
  }

  public static Config expand(Config config) {
    Config.Builder cb = Config.newBuilder();
    for (String key : config.getKeySet()) {
      Object value = config.get(key);
      if (value instanceof String) {
        String expandedValue = Misc.substitute(config, (String) value);
        cb.put(key, expandedValue);
      } else {
        cb.put(key, value);
      }
    }
    return cb.build();
  }

  public int size() {
    return cfgMap.size();
  }

  public Object get(String key) {
    return cfgMap.get(key);
  }

  public String getStringValue(String key) {
    return (String) get(key);
  }

  public String getStringValue(String key, String defaultValue) {
    String value = getStringValue(key);
    return value != null ? value : defaultValue;
  }

  public Boolean getBooleanValue(String key) {
    return (Boolean) get(key);
  }

  public Boolean getBooleanValue(String key, boolean defaultValue) {
    Boolean value = getBooleanValue(key);
    return value != null ? value : defaultValue;
  }

  public Long getLongValue(String key) {
    Object value = cfgMap.get(key);
    return Convert.getLong(value);
  }

  public Long getLongValue(String key, long defaultValue) {
    Object value = get(key);
    if (value != null) {
      return Convert.getLong(value);
    }
    return defaultValue;
  }

  public Integer getIntegerValue(String key) {
    Object value = cfgMap.get(key);
    return Convert.getInteger(value);
  }

  public Double getDoubleValue(String key) {
    Object value = cfgMap.get(key);
    return Convert.getDouble(value);
  }

  public Double getDoubleValue(String key, double defaultValue) {
    Object value = get(key);
    if (value != null) {
      return Convert.getDouble(value);
    }
    return defaultValue;
  }

  public boolean containsKey(String key) {
    return cfgMap.containsKey(key);
  }

  public Set<String> getKeySet() {
    return cfgMap.keySet();
  }

  public String asString() {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, Object> kv : cfgMap.entrySet()) {
      if (kv.getValue() instanceof String) {
        sb.append(String.format(" %s=\"%s\" ", kv.getKey(), kv.getValue().toString()));
      }
    }
    return sb.toString();
  }

  @Override
  public String toString() {
    Map<String, Object> treeMap = new TreeMap<String, Object>(cfgMap);
    StringBuilder sb = new StringBuilder();
    for (Object obj : treeMap.entrySet()) {
      Map.Entry<String, Object> entry = (Map.Entry) obj;
      sb.append("(\"" + entry.getKey() + "\"");
      sb.append(", " + entry.getValue() + ")\n");
    }
    return sb.toString();
  }

  public static class Builder {
    private final Map<String, Object> keyValues = new HashMap();

    private static Config.Builder create() {
      return new Builder();
    }

    public Builder put(String key, Object value) {
      this.keyValues.put(key, value);
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
