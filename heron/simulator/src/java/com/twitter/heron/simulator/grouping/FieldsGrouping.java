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

import com.google.protobuf.ByteString;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.proto.system.HeronTuples;

public class FieldsGrouping extends Grouping {
  private static final int DEFAULT_PRIME_NUMBER = 633910111;

  private final List<Integer> fieldsGroupingIndices = new LinkedList<>();
  private final int primeNumber;

  public FieldsGrouping(TopologyAPI.InputStream inputStream,
                        TopologyAPI.StreamSchema schema,
                        List<Integer> taskIds) {
    this(inputStream, schema, taskIds, DEFAULT_PRIME_NUMBER);
  }

  public FieldsGrouping(TopologyAPI.InputStream inputStream,
                        TopologyAPI.StreamSchema schema,
                        List<Integer> taskIds,
                        int primeNumber) {
    super(taskIds);

    this.primeNumber = primeNumber;

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
    for (Integer indices : fieldsGroupingIndices) {
      int hash = getByteStringHashCode(tuple.getValues(indices)) % primeNumber;
      taskIndex += hash >= 0 ? hash : hash + primeNumber;
    }

    taskIndex = taskIndex % taskIds.size();
    res.add(taskIds.get(taskIndex));

    return res;
  }

  /**
   * Returns a hash code value for the given ByteString,
   * basing on customized hash method.
   *
   * @param bs the given
   * @return the hash code of the ByteString
   */
  protected int getByteStringHashCode(ByteString bs) {
    return bs.hashCode();
  }
}
