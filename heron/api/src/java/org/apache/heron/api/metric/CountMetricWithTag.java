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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CountMetricWithTag implements IMetric<CountMetric> {
  /**
   * List<String> is the list of tags. The counters are aggregated by tag lists.
   * For example, ["device:ios", "endpoint:v2"] is a entirely different entry from ["device:ios"]
   */
  private Map<List<String>, CountMetric> taggedCounts = new HashMap<>();

  public CountMetricWithTag() {
  }

  public void incr(String... tags) {
    incrBy(1, tags);
  }

  /**
   * Increment the metrics with optional tags.
   * Tags are comma separated strings: "tagName:tagValue".
   * For example: "device:ios", and "endpoint:v2".
   * Normally tag names and values should have limited values,
   * otherwise, like "id:192.168.0.123", the memory utililization
   * and the cost of tracking metrics could increase dramatically.
   * @param tags optional comma separated tags, like "device:ios", "endpoint:v2".
   *   Normally tag names and values should have limited values,
   */
  public void incrBy(long incrementBy, String... tags) {
    if (tags.length > 0) {
      Arrays.sort(tags); // Sort the tags to make the key deterministic.
      List<String> tagList = Arrays.asList(tags);
      incrTaggedCountBy(tagList, incrementBy);
    } else {
      incrTaggedCountBy(Collections.emptyList(), incrementBy);
    }
  }

  private void incrTaggedCountBy(List<String> tagList, long incrementBy) {
    if (!taggedCounts.containsKey(tagList)) {
        taggedCounts.put(tagList, new CountMetric());
    }
    taggedCounts.get(tagList).incrBy(incrementBy);
  }

  @Override
  public CountMetric getValueAndReset() {
    return null; // Not needed. `getTaggedMetricsAndReset` should be used instead.
  }

  @Override
  public Map<List<String>, CountMetric> getTaggedMetricsAndReset() {
    Map<List<String>, CountMetric> ret = taggedCounts;
    taggedCounts = new HashMap<>();
    return ret;
  }
}
