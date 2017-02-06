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

import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.common.basics.DryRunFormatType;
import com.twitter.heron.common.basics.PackageType;
import com.twitter.heron.common.basics.TypeUtils;

/**
 * Config is an Immutable Map of &lt;String, Object&gt; The get/set API that uses Key objects
 * should be favored over Strings. Usage of the String API should be refactored out.
 */
public class Config {
  private final Map<String, Object> cfgMap = new HashMap<>();

  protected Config(Builder build) {
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

  public Object get(Key key) {
    return get(key.value());
  }

  private Object get(String key) {
    return cfgMap.get(key);
  }

  public String getStringValue(String key) {
    return (String) get(key);
  }

  public String getStringValue(Key key) {
    return (String) get(key);
  }

  public String getStringValue(Key key, String defaultValue) {
    String value = getStringValue(key);
    return value != null ? value : defaultValue;
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

  public Boolean getBooleanValue(Key key, boolean defaultValue) {
    return getBooleanValue(key.value(), defaultValue);
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

  PackageType getPackageType(Key key) {
    return (PackageType) get(key);
  }

  public Long getLongValue(Key key) {
    Object value = get(key);
    return TypeUtils.getLong(value);
  }

  public Long getLongValue(Key key, long defaultValue) {
    return getLongValue(key.value(), defaultValue);
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

  public Integer getIntegerValue(Key key, int defaultValue) {
    return getIntegerValue(key.value(), defaultValue);
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

  public Double getDoubleValue(Key key, double defaultValue) {
    Object value = get(key);
    if (value != null) {
      return TypeUtils.getDouble(value);
    }
    return defaultValue;
  }

  public boolean containsKey(Key key) {
    return cfgMap.containsKey(key);
  }

  public Set<String> getKeySet() {
    return cfgMap.keySet();
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

    private static Config.Builder create() {
      return new Builder();
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
