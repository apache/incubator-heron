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

package com.twitter.heron.simulator.grouping;

import java.util.LinkedList;
import java.util.List;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.proto.system.HeronTuples;

public class FieldsGrouping extends Grouping {
  private final List<Integer> fieldsGroupingIndices = new LinkedList<>();

  public FieldsGrouping(TopologyAPI.InputStream inputStream,
                        TopologyAPI.StreamSchema schema,
                        List<Integer> taskIds) {
    super(taskIds);

    for (int i = 0; i < schema.getKeysCount(); i++) {
      for (int j = 0; j < inputStream.getGroupingFields().getKeysCount(); j++) {
        Boolean keysEqual = schema.getKeys(i).getKey().equals(
            inputStream.getGroupingFields().getKeys(j).getKey());
        if (keysEqual) {
          fieldsGroupingIndices.add(i);
          break;
        }
      }
    }
  }

  @Override
  public List<Integer> getListToSend(HeronTuples.HeronDataTuple tuple) {
    List<Integer> res = new LinkedList<>();

    int taskIndex = 0;
    int primeNumber = 633910111;
    for (Integer indices : fieldsGroupingIndices) {
      taskIndex += getHashCode(tuple.getValues(indices)) % primeNumber;
    }

    taskIndex = taskIndex % taskIds.size();
    // Make sure taskIndex is greater than 0
    taskIndex = taskIndex >= 0 ? taskIndex : taskIndex + taskIds.size();
    res.add(taskIds.get(taskIndex));

    return res;
  }

  /**
   * Returns a hash code value for the given Object,
   * basing on customized hash method.
   *
   * @param o the given
   * @return the hash code of the Object
   */
  protected int getHashCode(Object o) {
    return o.hashCode();
  }
}
