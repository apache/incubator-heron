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

package com.twitter.heron.api.metric;


public interface IMetricsRegister {

  /**
   * Register an IMetric instance.
   * Heron will then call getValueAndReset on the metric every timeBucketSizeInSecs
   * and the returned value is sent to all metrics consumers.
   * You must call this during IBolt::prepare or ISpout::open.
   * @return The IMetric argument unchanged.
   */
  <T extends IMetric<U>, U> T registerMetric(String name, T metric, int timeBucketSizeInSecs);
}
