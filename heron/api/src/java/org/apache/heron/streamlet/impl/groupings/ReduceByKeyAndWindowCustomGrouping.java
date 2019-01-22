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


package org.apache.heron.streamlet.impl.groupings;

import java.util.ArrayList;
import java.util.List;

import org.apache.heron.api.grouping.CustomStreamGrouping;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.utils.Utils;
import org.apache.heron.streamlet.SerializableFunction;


/**
 * ReduceByKeyAndWindowCustomGrouping is the class that routes the incoming tuples
 * into the ReduceByKeyAndWindowOperator. It essentially ensures that the values being
 * routed are of type KeyValue uses the key to route the tuple to the destination.
 * The current implementation is identical to JoinCustomGrouping but it might
 * evolve in the future.
 */
public class ReduceByKeyAndWindowCustomGrouping<R, K> implements CustomStreamGrouping {
  private static final long serialVersionUID = -7630948017550637716L;
  private SerializableFunction<R, K> keyExtractor;
  private List<Integer> taskIds;

  public ReduceByKeyAndWindowCustomGrouping(SerializableFunction<R, K> keyExtractor) {
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
    R obj = (R) values.get(0);
    int key = keyExtractor.apply(obj).hashCode();
    ret.add(Utils.assignKeyToTask(key, taskIds));
    return ret;
  }
}
