package com.twitter.heron.spi.common;

import java.util.Map;
import java.util.HashMap;
import java.util.Set;

/**
 * Config is an Immutable Map of <String, Object>
 */
public class Config {
  private final Map<String, Object> cfgMap = new HashMap();

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

  private Config(Builder build) {
    cfgMap.putAll(build.keyValues);
  }

  public static Builder newBuilder() { 
    return Builder.create(); 
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
    return getLong(value);
  }

  public Long getLongValue(String key, long defaultValue) {
    Object value = get(key);
    if (value != null) {
      return getLong(value);
    }
    return defaultValue;
  }

  public Double getDoubleValue(String key) {
    Object value = cfgMap.get(key);
    return getDouble(value);
  }

  public Double getDoubleValue(String key, double defaultValue) {
    Object value = get(key);
    if (value != null) {
      return getDouble(value);
    }
    return defaultValue;
  }

  public boolean containsKey(String key) {
    return cfgMap.containsKey(key);
  }

  public Set<String> getKeySet() {
    return cfgMap.keySet();
  }

  private static Long getLong(Object o) {
    if (o instanceof Long) {
      return ((Long) o);
    } else if (o instanceof Integer) {
      return new Long(((Integer) o).longValue());
    } else if (o instanceof Short) {
      return new Long(((Short) o).longValue());
    } else {
      try {
        return Long.parseLong(o.toString());
      } catch (NumberFormatException nfe) {
        throw new IllegalArgumentException("Don't know how to convert " + o + " + to long");
      }
    }
  }

  private static Double getDouble(Object o) { 
    if (o instanceof Double) {
      return ((Double) o);
    } else if (o instanceof Float) {
      return new Double(((Float) o).doubleValue());
    } else if (o instanceof Long) {
      return new Double(((Long) o).doubleValue());
    } else if (o instanceof Integer) {
      return new Double(((Integer) o).doubleValue());
    } else if (o instanceof Short) {
      return new Double(((Short) o).doubleValue());
    } else {
      try {
        return Double.parseDouble(o.toString());
      } catch (NumberFormatException nfe) {
        throw new IllegalArgumentException("Don't know how to convert " + o + " + to double");
      }
    }
  }

  public String asString() {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, Object> kv : cfgMap.entrySet()) {
      if (kv.getValue() instanceof String) {
        sb.append(String.format(" %s=\"%s\" ", kv.getKey(), kv.getValue()));
      }
    }
    return sb.toString();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (Object obj : cfgMap.entrySet()) {
      Map.Entry<String, Object> entry = (Map.Entry) obj;
      sb.append("(\"" + entry.getKey() + "\"");
      sb.append(", " + entry.getValue() + ")\n");
    }
    return sb.toString();
  }
}
