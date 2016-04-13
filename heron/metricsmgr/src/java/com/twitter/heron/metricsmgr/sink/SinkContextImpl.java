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

package com.twitter.heron.metricsmgr.sink;

import com.twitter.heron.api.metric.MultiCountMetric;
import com.twitter.heron.spi.metricsmgr.sink.SinkContext;

/**
 * Context needed for an IMetricsSink to init.
 * <p/>
 * We distinguish config and context carefully:
 * Config is populated from yaml file and would not be changed anymore,
 * while context is populated in run-time.
 */

public class SinkContextImpl implements SinkContext {

  private final MultiCountMetric internalMultiCountMetrics;

  private final String sinkId;

  private final String metricsmgrId;

  private final String topologyName;

  public SinkContextImpl(String topologyName,
                         String metricsmgrId,
                         String sinkId,
                         MultiCountMetric internalMultiCountMetrics) {
    this.topologyName = topologyName;
    this.metricsmgrId = metricsmgrId;
    this.sinkId = sinkId;
    this.internalMultiCountMetrics = internalMultiCountMetrics;
  }

  @Override
  public String getTopologyName() {
    return topologyName;
  }

  @Override
  public String getMetricsMgrId() {
    return metricsmgrId;
  }

  @Override
  public String getSinkId() {
    return sinkId;
  }

  @Override
  public void exportCountMetric(String metricName, long delta) {
    internalMultiCountMetrics.scope(metricName).incrBy(delta);
  }
}
