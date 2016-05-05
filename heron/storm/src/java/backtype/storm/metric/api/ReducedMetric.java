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

package backtype.storm.metric.api;

public class ReducedMetric<T, U, V> implements IMetric<V> {
  private final IReducer<T, U, V> reducer;
  private T accumulator;

  public ReducedMetric(IReducer<T, U, V> reducer) {
    this.reducer = reducer;
    this.accumulator = this.reducer.init();
  }

  public void update(U value) {
    accumulator = reducer.reduce(accumulator, value);
  }

  @Override
  public V getValueAndReset() {
    V ret = reducer.extractResult(accumulator);
    accumulator = reducer.init();
    return ret;
  }
}
