package com.twitter.heron.api.metric;

import java.util.HashMap;
import java.util.Map;

public class MultiAssignableMetric implements IMetric {
  private Map<String, AssignableMetric> _value = new HashMap();

  public MultiAssignableMetric() {

  }

  public AssignableMetric scope(String key) {
    AssignableMetric val = _value.get(key);
    if (val == null) {
      _value.put(key, val = new AssignableMetric(0));
    }
    return val;
  }

  public AssignableMetric safeScope(String key) {
    AssignableMetric val;
    synchronized (_value) {
      val = _value.get(key);
      if (val == null) {
        _value.put(key, val = new AssignableMetric(0));
      }
    }
    return val;
  }

  @Override
  public Object getValueAndReset() {
    Map ret = new HashMap();
    synchronized (_value) {
      for (Map.Entry<String, AssignableMetric> e : _value.entrySet()) {
        ret.put(e.getKey(), e.getValue().getValueAndReset());
      }
    }
    return ret;
  }
}
