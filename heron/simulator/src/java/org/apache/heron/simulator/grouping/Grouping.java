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

import java.util.List;

import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.proto.system.HeronTuples;

public abstract class Grouping {
  protected List<Integer> taskIds;

  public Grouping(List<Integer> taskIds) {
    this.taskIds = taskIds;
  }

  public static Grouping create(TopologyAPI.Grouping grouping,
                                TopologyAPI.InputStream inputStream,
                                TopologyAPI.StreamSchema schema,
                                List<Integer> taskIds) {
    switch (grouping) {
      case SHUFFLE:
        return new ShuffleGrouping(taskIds);

      case FIELDS:
        return new FieldsGrouping(inputStream, schema, taskIds);

      case ALL:
        return new AllGrouping(taskIds);

      case LOWEST:
        return new LowestGrouping(taskIds);

      case NONE:
        // This is what we are doing in production right now
        return new ShuffleGrouping(taskIds);

      case CUSTOM:
        return new CustomGrouping(taskIds);

      case DIRECT:
        throw new IllegalArgumentException("Direct Grouping not supported");

      default:
        throw new IllegalArgumentException("Unknown Grouping");
    }
  }

  public abstract List<Integer> getListToSend(HeronTuples.HeronDataTuple tuple);
}
