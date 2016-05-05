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

package backtype.storm.task;

import backtype.storm.metric.api.CombinedMetric;
import backtype.storm.metric.api.ICombiner;
import backtype.storm.metric.api.IMetric;
import backtype.storm.metric.api.IReducer;
import backtype.storm.metric.api.ReducedMetric;


public interface IMetricsContext {
  <T extends IMetric<U>, U> T registerMetric(String name, T metric, int timeBucketSizeInSecs);

  <T, U, V> ReducedMetric<T, U, V> registerMetric(String name,
                                                  IReducer<T, U, V> reducer,
                                                  int timeBucketSizeInSecs);

  <T> CombinedMetric<T> registerMetric(String name,
                                       ICombiner<T> combiner,
                                       int timeBucketSizeInSecs);
}
