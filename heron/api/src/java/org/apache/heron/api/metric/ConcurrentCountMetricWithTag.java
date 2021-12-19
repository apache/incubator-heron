/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.heron.api.metric;

import java.util.List;
import java.util.Map;

// A thread safe count metric
public class ConcurrentCountMetricWithTag implements IMetric<CountMetric> {
  private CountMetricWithTag counter;

  public ConcurrentCountMetricWithTag() {
    counter = new CountMetricWithTag();
  }

  public synchronized void incr(String... tags) {
    counter.incr(tags);
  }

  public synchronized void incrBy(long incrementBy, String... tags) {
    counter.incrBy(incrementBy, tags);
  }

  @Override
  public CountMetric getValueAndReset() {
    return null; // Not needed. `getTaggedMetricsAndReset` should be used instead.
  }

  @Override
  public Map<List<String>, CountMetric> getTaggedMetricsAndReset() {
    return counter.getTaggedMetricsAndReset();
  }
}
