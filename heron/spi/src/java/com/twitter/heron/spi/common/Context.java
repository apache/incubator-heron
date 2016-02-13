package com.twitter.heron.spi.common;

import java.util.Map;
import java.util.HashMap;

/**
 * Context is an Immutable Map of <String, Object>
 */
public class Context {
  private final Map<String, Object> keyValues = new HashMap();

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
      keyValues.putAll(cxt.keyValues);
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
    this.keyValues.putAll(build.keyValues);
  }

  public Object get(String key) {
    return keyValues.get(key);
  }

  public String getStringValue(String key) {
    return (String) keyValues.get(key);
  }

  public String getStringValue(String key, String defaultValue) {
    String value = getStringValue(key);
    return value != null ? value : defaultValue;
  }

  public Boolean getBooleanValue(String key) {
    return (Boolean) keyValues.get(key);
  }

  public boolean getBooleanValue(String key, boolean defaultValue) {
    Boolean value = getBooleanValue(key);
    return value != null ? value : defaultValue;
  }

  public long getLongValue(String key, long defaultValue) {
    Object value = get(key);
    if (value != null) {
      Long lvalue = (Long) value;
      return lvalue.longValue();
    }
    return defaultValue;
  }

  public double getDoubleValue(String key, double defaultValue) {
    Object value = get(key);
    if (value != null) {
      Double dvalue = (Double) value;
      return dvalue.doubleValue();
    }
    return defaultValue;
  }
}
