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

/**
 * ReduceByWindowCustomGrouping is the class that routes the incoming tuples
 * into the ReduceByWindowOperator. It essentially does hash grouping.
 */
public class ReduceByWindowCustomGrouping<I> implements CustomStreamGrouping {
  private static final long serialVersionUID = -2533339197867000330L;
  private List<Integer> taskIds;

  public ReduceByWindowCustomGrouping() {
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
    I obj = (I) values.get(0);
    int index = obj.hashCode() % taskIds.size();
    ret.add(taskIds.get(index));
    return ret;
  }
}
