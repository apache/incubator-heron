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

public class CountMetric implements IMetric<Long> {
  private long value = 0;

  public CountMetric() {
  }

  public void incr() {
    value++;
  }

  public void incrBy(long incrementBy) {
    value += incrementBy;
  }

  @Override
  public Long getValueAndReset() {
    long ret = value;
    value = 0;
    return ret;
  }
}
