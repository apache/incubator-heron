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

package org.apache.heron.spi.metricsmgr.metrics;

import java.util.List;

/**
 * An immutable class providing a view of MetricsInfo
 * The value is in type String, and IMetricsSink would determine how to parse it.
 */
public class MetricsInfo {
  private final String name;
  private final String value;
  private final List<String> tags;

  public MetricsInfo(String name, String value) {
    this(name, value, null);
  }

  public MetricsInfo(String name, String value, List<String> tags) {
    this.name = name;
    this.value = value;
    this.tags = tags;
  }

  /**
   * Get the name of the metric
   *
   * @return the name of the metric
   */
  public String getName() {
    return name;
  }

  /**
   * Get the value of the metric
   *
   * @return the value of the metric
   */
  public String getValue() {
    return value;
  }

  /**
   * Get the tags of the metric
   *
   * @return the tags of the metric
   */
  public List<String> getTags() {
    return tags;
  }

  @Override
  public String toString() {
    if (tags == null) {
      return name + "=" + value;
    } else {
      return name + "=" + value + " tags=" + tags;
    }
  }
}
