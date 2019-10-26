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

package org.apache.heron.spi.metricsmgr.sink;

/**
 * Context needed for an IMetricsSink to init.
 * <p>
 * We distinguish config and context carefully:
 * Config is populated from yaml file and would not be changed anymore,
 * while context is populated in run-time.
 */

public interface SinkContext {
  String getTopologyName();

  String getCluster();

  String getRole();

  String getEnvironment();

  String getMetricsMgrId();

  String getSinkId();

  void exportCountMetric(String metricName, long delta);
}
