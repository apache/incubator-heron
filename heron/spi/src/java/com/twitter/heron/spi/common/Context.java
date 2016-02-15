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

    public Builder putAll(Map<String, Object> map) {
      keyValues.putAll(map);
      return this;
    }

    public Context build() {
      return new Context(this);
    }
  }

  private Context(Builder build) {
    this.keyValues.putAll(build.keyValues);
  }

  public static Builder newBuilder() { 
    return Builder.create(); 
  }

  public int size() {
    return keyValues.size();
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

  public long getLongValue(String key) {
    Object value = keyValues.get(key);
    return getLong(value);
  }

  public long getLongValue(String key, long defaultValue) {
    Object value = get(key);
    if (value != null) {
      return getLong(value);
    }
    return defaultValue;
  }

  public double getDoubleValue(String key) {
    Object value = keyValues.get(key);
    return getDouble(value);
  }

  public double getDoubleValue(String key, double defaultValue) {
    Object value = get(key);
    if (value != null) {
      return getDouble(value);
    }
    return defaultValue;
  }

  private static long getLong(Object o) {
    if (o instanceof Long) {
      return ((Long) o).longValue();
    } else if (o instanceof Integer) {
      return ((Integer) o).longValue();
    } else if (o instanceof Short) {
      return ((Short) o).longValue();
    } else {
      try {
        return Long.parseLong(o.toString());
      } catch (NumberFormatException nfe) {
        throw new IllegalArgumentException("Don't know how to convert " + o + " + to long");
      }
    }
  }

  private static double getDouble(Object o) { 
    if (o instanceof Double) {
      return ((Double) o).doubleValue();
    } else if (o instanceof Float) {
      return ((Float) o).doubleValue();
    } else if (o instanceof Long) {
      return ((Long) o).doubleValue();
    } else if (o instanceof Integer) {
      return ((Integer) o).doubleValue();
    } else if (o instanceof Short) {
      return ((Short) o).doubleValue();
    } else {
      try {
        return Double.parseDouble(o.toString());
      } catch (NumberFormatException nfe) {
        throw new IllegalArgumentException("Don't know how to convert " + o + " + to double");
      }
    }
  }
}
