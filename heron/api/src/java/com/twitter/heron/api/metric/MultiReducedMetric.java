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

public class MultiReducedMetric implements IMetric {
  private Map<String, ReducedMetric> mValue = new HashMap();
  private IReducer mReducer;

  public MultiReducedMetric(IReducer reducer) {
    mReducer = reducer;
  }

  public ReducedMetric scope(String key) {
    ReducedMetric val = mValue.get(key);
    if (val == null) {
      mValue.put(key, val = new ReducedMetric(mReducer));
    }
    return val;
  }

  public Object getValueAndReset() {
    Map ret = new HashMap();
    for (Map.Entry<String, ReducedMetric> e : mValue.entrySet()) {
      Object val = e.getValue().getValueAndReset();
      if (val != null) {
        ret.put(e.getKey(), val);
      }
    }
    return ret;
  }
}
