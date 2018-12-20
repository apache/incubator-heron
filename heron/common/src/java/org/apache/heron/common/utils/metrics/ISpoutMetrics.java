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

package org.apache.heron.common.utils.metrics;

import org.apache.heron.common.utils.misc.PhysicalPlanHelper;
import org.apache.heron.common.utils.topology.TopologyContextImpl;


/**
 * Spout's metrics to be collect
 * We need to:
 * 1. Define the metrics to be collected
 * 2. New them in the constructor
 * 3. Register them in registerMetrics(...) by using MetricsCollector's registerMetric(...)
 * 4. Expose methods which could be called externally to change the value of metrics
 */

public interface ISpoutMetrics extends ComponentMetrics {

  void registerMetrics(TopologyContextImpl topologyContext);

  // For MultiCountMetrics, we need to set the default value for all streams.
  // Otherwise, it is possible one metric for a particular stream is null.
  // For instance, the fail-count on a particular stream could be undefined
  // causing metrics not be exported.
  // However, it will not set the Multi Reduced/Assignable Metrics,
  // since we could not have default values for them
  void initMultiCountMetrics(PhysicalPlanHelper helper);

  void ackedTuple(String streamId, long latency);

  void failedTuple(String streamId, long latency);

  void timeoutTuple(String streamId);

  void nextTuple(long latency);

  void updateOutQueueFullCount();

  void updatePendingTuplesCount(long count);

  void updateTaskRunCount();

  void updateProduceTupleCount();

  void updateContinueWorkCount();
}
