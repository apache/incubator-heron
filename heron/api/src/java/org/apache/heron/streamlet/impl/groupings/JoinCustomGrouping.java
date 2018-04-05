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

package org.apache.heron.streamlet.impl.groupings;

import java.util.ArrayList;
import java.util.List;

import org.apache.heron.api.grouping.CustomStreamGrouping;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.streamlet.SerializableFunction;

/**
 * JoinCustomGrouping is the class that routes the incoming tuples into the
 * JoinOperator. It essentially ensures that the values being routed are of type
 * KeyValue uses the key to route the tuple to the destination.
 */
public class JoinCustomGrouping<K, V> implements CustomStreamGrouping {
  private static final long serialVersionUID = 2007892247960031525L;
  private SerializableFunction<V, K> keyExtractor;
  private List<Integer> taskIds;

  public JoinCustomGrouping(SerializableFunction<V, K> keyExtractor) {
    this.keyExtractor = keyExtractor;
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
    V obj = (V) values.get(0);
    int index = keyExtractor.apply(obj).hashCode() % taskIds.size();
    ret.add(taskIds.get(index));
    return ret;
  }
}
