package com.twitter.heron.spi.common;

import java.util.Map;
import java.util.HashMap;
import java.util.Set;

/**
 * Context is an Immutable Map of <String, Object>
 */
public class Context {
  private final Map<String, Object> cxtMap = new HashMap();

  public static class Builder {
    private final Map<String, Object> keyValues = new HashMap();

    private static Context.Builder create() {
      return new Builder();
    }

    public Builder put(String key, Object value) {
      this.keyValues.put(key, value);
      return this;
    }

    public Builder putAll(Context cxt) {
      keyValues.putAll(cxt.cxtMap);
      return this;
    }

    public Context build() {
      return new Context(this);
    }
  }

  public static Builder newBuilder() { 
    return Builder.create(); 
  }

  private Context(Builder build) {
    this.cxtMap.putAll(build.keyValues);
  }

  public Object get(String key) {
    return cxtMap.get(key);
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
    return (Long) get(key);
  }

  public Double getDoubleValue(String key) {
    return (Double) get(key);
  }

  public Long getLongValue(String key, long defaultValue) {
    Long value = getLongValue(key);
    return value != null ? value : defaultValue;
  }

  public Double getDoubleValue(String key, double defaultValue) {
    Double value = getDoubleValue(key);
    return value != null ? value : defaultValue;
  }

  public boolean containsKey(String key) {
    return cxtMap.containsKey(key);
  }

  public Set<String> getKeySet() {
    return cxtMap.keySet();
  }
}
