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

    public static Context.Builder builder() {
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
}