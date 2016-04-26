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

package com.twitter.heron.api.metric;

import java.util.HashMap;
import java.util.Map;

public class MultiAssignableMetric implements IMetric {
  private Map<String, AssignableMetric> mValue = new HashMap();

  public MultiAssignableMetric() {

  }

  public AssignableMetric scope(String key) {
    AssignableMetric val = mValue.get(key);
    if (val == null) {
      mValue.put(key, val = new AssignableMetric(0));
    }
    return val;
  }

  public AssignableMetric safeScope(String key) {
    AssignableMetric val;
    synchronized (mValue) {
      val = mValue.get(key);
      if (val == null) {
        mValue.put(key, val = new AssignableMetric(0));
      }
    }
    return val;
  }

  @Override
  public Object getValueAndReset() {
    Map ret = new HashMap();
    synchronized (mValue) {
      for (Map.Entry<String, AssignableMetric> e : mValue.entrySet()) {
        ret.put(e.getKey(), e.getValue().getValueAndReset());
      }
    }
    return ret;
  }
}
