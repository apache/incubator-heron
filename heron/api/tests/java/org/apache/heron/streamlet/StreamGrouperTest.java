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

import org.junit.Test;

import org.apache.heron.api.grouping.ShuffleStreamGrouping;
import org.apache.heron.api.grouping.StreamGrouping;
import org.apache.heron.api.utils.Utils;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link StreamGrouper}
 */
public class StreamGrouperTest {

  @Test
  public void testBuildGrouper() {
    StreamGrouping shuffle = new ShuffleStreamGrouping();
    StreamGrouper grouper = new StreamGrouper(shuffle);

    Map<String, StreamGrouping> groupingMap = grouper.getGroupings();
    assertEquals(groupingMap.size(), 1);
    assertEquals(groupingMap.get(Utils.DEFAULT_STREAM_ID), shuffle);
  }

  @Test
  public void testBuildGrouperWithStreamId() {
    StreamGrouping shuffle = new ShuffleStreamGrouping();
    StreamGrouper grouper = new StreamGrouper("test_stream", shuffle);

    Map<String, StreamGrouping> groupingMap = grouper.getGroupings();
    assertEquals(groupingMap.size(), 1);
    assertEquals(groupingMap.get("test_stream"), shuffle);
  }

  @Test
  public void testAppendGrouper() {
    StreamGrouping shuffle = new ShuffleStreamGrouping();
    StreamGrouper grouper = new StreamGrouper(shuffle);
    grouper.append("test_stream", shuffle);

    Map<String, StreamGrouping> groupingMap = grouper.getGroupings();
    assertEquals(groupingMap.size(), 2);
    assertEquals(groupingMap.get(Utils.DEFAULT_STREAM_ID), shuffle);
    assertEquals(groupingMap.get("test_stream"), shuffle);
  }
}
