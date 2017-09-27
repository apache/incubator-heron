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

package com.twitter.heron.dsl.impl.groupings;

import java.util.ArrayList;
import java.util.List;

import com.twitter.heron.api.grouping.CustomStreamGrouping;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.dsl.SerializableBiFunction;

/**
 * RemapCustomGrouping is the class that routes the incoming tuples
 * into the ReMapBolt. It takes in a user defined remapFn as input.
 * The remapFn defines how tuples should be redirected.
 */
public class RemapCustomGrouping<R> implements CustomStreamGrouping {
  private static final long serialVersionUID = 8118844912340601079L;
  private List<Integer> taskIds;
  private SerializableBiFunction<? super R, Integer, List<Integer>> remapFn;

  public RemapCustomGrouping(SerializableBiFunction<? super R, Integer, List<Integer>> remapFn) {
    this.remapFn = remapFn;
  }

  @Override
  public void prepare(TopologyContext context, String component,
                      String streamId, List<Integer> targetTasks) {
    this.taskIds = targetTasks;
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<Integer> chooseTasks(List<Object> values) {
    List<Integer> ret = new ArrayList<>();
    R obj = (R) values.get(0);
    List<Integer> targets = remapFn.apply(obj, ret.size());
    for (Integer target : targets) {
      ret.add(taskIds.get(target % taskIds.size()));
    }
    return ret;
  }
}
