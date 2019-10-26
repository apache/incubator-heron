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

package org.apache.heron.simulator.grouping;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.heron.proto.system.HeronTuples;

public class ShuffleGrouping extends Grouping {
  private final int taskIdsSize;
  private int nextTaskIndex;

  public ShuffleGrouping(List<Integer> taskIds) {
    super(taskIds);

    taskIdsSize = taskIds.size();

    nextTaskIndex = new Random().nextInt(taskIds.size());
  }

  @Override
  public List<Integer> getListToSend(HeronTuples.HeronDataTuple tuple) {
    List<Integer> res = new ArrayList<>(1);
    res.add(taskIds.get(nextTaskIndex));
    nextTaskIndex = getNextTaskIndex(nextTaskIndex);

    return res;
  }

  private int getNextTaskIndex(int index) {
    int dupIndex = index + 1;

    if (dupIndex == taskIdsSize) {
      dupIndex = 0;
    }

    return dupIndex;
  }
}
