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

/**
 * Interface for a metric that can be tracked
 * @param <T> the type of the metric value being tracked
 */
public interface IMetric<T> {
  T getValueAndReset();

  /**
   * Get the <tag list, value> pairs of the metric and reset it to the identity value.
   * @return a map of <tag list, value> pairs. Return null if this function is not supported.
   */
  default Map<List<String>, T> getTaggedMetricsAndReset() {
    return null;
  }
}
