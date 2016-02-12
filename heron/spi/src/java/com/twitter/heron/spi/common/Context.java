package com.twitter.heron.spi.common;

import java.util.Map;
import java.util.HashMap;

public class Context {
  private final Map<String, Object> keyValues = new HashMap<String, Object>();

  public static class Builder {
    private final Map<String, Object> keyValues = new HashMap<String, Object>();

    public Builder set(String key, Object value) {
      this.keyValues.put(key, value);
      return this;
    }

    public Builder setStringValue(String key, String value) {
      this.keyValues.put(key, value);
      return this;
    }

    public Builder setBooleanValue(String key, boolean value) {
      if (value == true)
        this.keyValues.put(key, "yes");
      else
        this.keyValues.put(key, "no");

      return this;
    }

    public Context build() {
      return new Context(this);
    }
  }

  public Context(Builder build) {
    this.keyValues.putAll(build.keyValues);
  }

  public void append(Builder build) {
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

  public boolean getBooleanValue(String key) {
    String value = getStringValue(key);
    return value == "yes" ? true : false;
  }

  public boolean getBooleanValue(String key, boolean defaultValue) {
    String value = (String) get(key);
    if (value != null)
      return value == "yes" ? true : false;

    return defaultValue;
  }
}