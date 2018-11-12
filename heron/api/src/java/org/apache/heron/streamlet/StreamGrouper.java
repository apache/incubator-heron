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

package org.apache.heron.streamlet;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.heron.api.grouping.StreamGrouping;
import org.apache.heron.api.utils.Utils;
import org.apache.heron.streamlet.impl.utils.StreamletUtils;

/**
 * StreamGrouper is the class to config grouping strategy in Streamlet API. Note that
 * One Operator could have more than one grouping strategies (when there are more than one
 * incoming streams) although it has one in most cases.
 */
public class StreamGrouper {
  /*
   * Map of stream id to grouping strategies. One operator can have more than one incoming streams
   * and each stream needs a grouping strategies. Hence it is possible for an operator to have
   * more than one entries.
   */
  private Map<String, StreamGrouping> groupingMap;

  /**
   * Construct a Grouper object with specific grouping strategy and default stream id
   * @param grouping The grouping strategy to be applied
   */
  public StreamGrouper(StreamGrouping grouping) {
    StreamletUtils.require(grouping != null, "grouping strategy can't be null");
    groupingMap = new ConcurrentHashMap<>();
    append(Utils.DEFAULT_STREAM_ID, grouping);
  }

  /**
   * Construct a Grouper object with specific grouping strategy and streamid
   * @param streamId The stream id that the strategy to be applied to
   * @param grouping The grouping strategy to be applied
   */
  public StreamGrouper(String streamId, StreamGrouping grouping) {
    StreamletUtils.require(streamId != null && !streamId.isEmpty(), "streamId can't be empty");
    StreamletUtils.require(grouping != null, "grouping strategy can't be null");
    groupingMap = new ConcurrentHashMap<>();
    append(streamId, grouping);

  }

  /**
   * Append more grouping strategies into an existing StreamGrouper object, when there are
   * multiple streams
   * @param streamId The stream id that the strategy to be applied to
   * @param grouping The grouping strategy to be applied
   */
  public void append(String streamId, StreamGrouping grouping) {
    StreamletUtils.require(streamId != null && !streamId.isEmpty(), "streamId can't be empty");
    StreamletUtils.require(grouping != null, "grouping strategy can't be null");
    groupingMap.put(streamId, grouping);
  }

  /**
   * Get the map of stream id to grouping strategy.
   * @return The map of stream id to grouping strategy
   */
  public Map<String, StreamGrouping> getGroupings() {
    return groupingMap;
  }
}
