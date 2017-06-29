//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package com.twitter.heron.common.utils.metrics;

/**
 * Abstract Class for common metric actions that both spouts and bolts support
 */
public abstract class ComponentMetrics {
  // Metric-name suffix reserved for value aggregating on all different streams
  public static final String ALL_STREAMS_AGGREGATED = "__all-streams-aggregated";

  public abstract void serializeDataTuple(String streamId, long latency);

  public abstract void emittedTuple(String streamId);
}
