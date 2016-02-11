package com.twitter.heron.spi.common.context;

import java.util.Map;
import java.util.HashMap;

public class Context {
  private final Map<String, Object> keyValues = new HashMap<String, Object>();

  public static class Builder {
    private final Map<String, Object> keyValues = new HashMap<String, Object>();

    public Builder setKeyValue(String key, Object value) {
      this.keyValues.put(key, value);
      return this;
    }

    public Context build() {
      return new Context(this);
    }
  }

  public Context(Builder) {
    this.keyValues.putAll(Builder.keyValues);
  }

  public append(Builder) {
    this.keyValues.putAll(Builder.keyValues);
  }

  public Object getProperty(String key) {
    return config.get(key);
  }

}