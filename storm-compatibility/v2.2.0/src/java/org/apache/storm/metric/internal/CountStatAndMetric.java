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

package org.apache.storm.metric.internal;

import java.util.Map;

import org.apache.storm.metric.api.IMetric;

public class CountStatAndMetric implements IMetric {

  private final org.apache.heron.api.metric.CountStatAndMetric delegate;

  public CountStatAndMetric(int numBuckets) {
    delegate = new org.apache.heron.api.metric.CountStatAndMetric(numBuckets);
  }

  @Override
  public Object getValueAndReset() {
    return delegate.getValueAndReset();
  }

  public void incBy(long count) {
    delegate.incBy(count);
  }

  public Map<String, Long> getTimeCounts() {
    return delegate.getTimeCounts();
  }

  public void close() {
    delegate.close();
  }
}
