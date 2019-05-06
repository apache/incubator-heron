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
package org.apache.heron.streamlet.impl.sources;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.heron.api.Config;
import org.apache.heron.api.spout.SpoutOutputCollector;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.streamlet.Source;

import static org.powermock.api.mockito.PowerMockito.mock;

public class ComplexSourceTest {

  private ComplexSource source;
  private Map<String, Object> confMap = new HashMap<>();
  private TopologyContext mockContext = mock(TopologyContext.class);

  private Source<Integer> generator =
      new org.apache.heron.streamlet.impl.sources.ComplexIntegerSource();

  private String msgId;
  private int limit = 10000;

  public ComplexSourceTest() {
    confMap.put("topology.reliability.mode", Config.TopologyReliabilityMode.ATMOST_ONCE);
    SpoutOutputCollector mySpout =
        new SpoutOutputCollector(new org.apache.heron.streamlet.impl.sources.TestCollector());
    source = new ComplexSource(generator);
    source.open(confMap, mockContext, mySpout);
  }

  @Before
  public void preTestSetup() {
    source.msgIdCache.invalidateAll();
    source.taskIds = null;
  }

  /**
   * Verify that acking removes entry from cache.
   */
  @Test
  public void testAckWithAckingEnabled() {
    source.ackingEnabled = true;
    // verify cache is empty
    Assert.assertEquals(0, source.msgIdCache.size());
    // Add a 'message id' entry to cache
    source.msgIdCache.put("msgId", "1");
    Assert.assertEquals(1, source.msgIdCache.size());
    Assert.assertEquals("1", source.msgIdCache.getIfPresent("msgId"));
    // ack this entry
    source.ack("msgId");
    // verify the message id entry is no longer in the cache
    Assert.assertEquals(0, source.msgIdCache.size());
    Assert.assertNull(source.msgIdCache.getIfPresent("msgId"));
  }

  /**
   * Ack many ids and verify cache is emptied out.
   */
  @Test
  public void testMultipleAcksWithAckingEnabled() {
    source.ackingEnabled = true;
    // verify cache is empty
    Assert.assertEquals(0, source.msgIdCache.size());
    // fill cache with many entries
    for (int i = 0; i < limit; i++) {
      msgId = "mid-" + String.valueOf(i);
      source.msgIdCache.put(msgId, String.valueOf(i));
      Assert.assertEquals(String.valueOf(i), source.msgIdCache.getIfPresent(msgId));
    }
    Assert.assertEquals(limit, source.msgIdCache.size());
    // ack all of the entries
    for (int i = 0; i < limit; i++) {
      msgId = "mid-" + String.valueOf(i);
      source.ack(msgId);
    }
    // verify cache is now empty, i.e., all ids were 'acked' successfully
    Assert.assertEquals(0, source.msgIdCache.size());
  }

  /**
   * With acking disabled the cache is not involved. Use this fact to
   * verify acking with ackingEnabled set to false.
   */
  @Test
  public void testAckWithAckingDisabled1() {
    source.ackingEnabled = false;
    // clear all cache entries
    Assert.assertEquals(0, source.msgIdCache.size());
    // Add an 'msgId' entry to cache. This entry is being placed into the
    // cache solely to be used in verifying that the ack call has no effect
    // on the cache.
    source.msgIdCache.put("msgId", "1");
    Assert.assertEquals(1, source.msgIdCache.size());
    Assert.assertEquals("1", source.msgIdCache.getIfPresent("msgId"));

    source.ack("msgId");
    // with ackingEnabled set to false, the ack call is basically a noop so the
    // cache is not involved so size should still be 1.
    Assert.assertEquals(1, source.msgIdCache.size());
    Assert.assertNotNull(source.msgIdCache.getIfPresent("msgId"));
  }

  /**
   * As above, the cache is not involved when acking is disabled, so verify
   * that nothing was added to cache by sending a ack.
   */
  @Test
  public void testAckWithAckingDisabled2() {
    source.ackingEnabled = false;
    Assert.assertEquals(0, source.msgIdCache.size());
    source.ack("id1");
    // with no acking, the msgIdCache is not involved so size should still be 0
    Assert.assertEquals(0, source.msgIdCache.size());
    Assert.assertNull(source.msgIdCache.getIfPresent("id1"));
  }

  /**
   * If a message if failed when ackingEnabled is set, the cache will
   * not remove the entry. Use this fact to test the fail method.
   * Use the value of taskId's to verify the emit method was called
   * as expected.
   */
  @Test
  public void testFailWithAckingEnabled() {
    source.ackingEnabled = true;
    // Add an entry to the cache to be used in failure test
    source.msgIdCache.put("msgId", 1234);
    Assert.assertEquals(1, source.msgIdCache.size());
    source.fail("msgId");
    Assert.assertNotNull(source.taskIds);
    Assert.assertNotEquals(0, source.taskIds.size());
    // cache should still retain value
    Assert.assertEquals(1, source.msgIdCache.size());
  }

  /**
   * Fail many ids and verify cache is not emptied out.
   */
  @Test
  public void testMultipleFailsWithAckingEnabled() {
    source.ackingEnabled = true;
    // verify cache is empty
    Assert.assertEquals(0, source.msgIdCache.size());
    // fill cache with many entries
    for (int i = 0; i < limit; i++) {
      msgId = "mid-" + String.valueOf(i);
      source.msgIdCache.put(msgId, String.valueOf(i));
      Assert.assertEquals(String.valueOf(i), source.msgIdCache.getIfPresent(msgId));
    }
    Assert.assertEquals(limit, source.msgIdCache.size());
    // fail all of the entries
    for (int i = 0; i < limit; i++) {
      msgId = "mid-" + String.valueOf(i);
      source.fail(msgId);
    }
    // verify cache is not empty, i.e., all ids should still be in cache
    Assert.assertEquals(limit, source.msgIdCache.size());
  }

  /**
   * Failing with acking disabled should do nothing. Verify that no
   * taskIds are returned via an emit call.
   */
  @Test
  public void testFailWithAckingDisabled() {
    source.ackingEnabled = false;
    String mid = "msgId";
    source.fail(mid);
    Assert.assertNull(source.taskIds);
  }

  /**
   * Verify that nextTuple adds an entry to the cache and that
   * a taskId value is returned.
   */
  @Test
  public void testNextTupleWithAckingEnabled() {
    source.ackingEnabled = true;
    Assert.assertEquals(0, source.msgIdCache.size());
    source.nextTuple();
    // This complexSource returns three entries per nextTuple call.
    Assert.assertEquals(3, source.msgIdCache.size());
    Assert.assertNotNull(source.taskIds);
  }

  /**
   * Verify that values are emitted and that no cache is used.
   */
  @Test
  public void testNextTupleWithAckingDisabled() {
    source.ackingEnabled = false;
    int expectedTaskId = 1234;
    // cache should not be utilized
    Assert.assertEquals(0, source.msgIdCache.size());
    Assert.assertNull(source.taskIds);
    for (int i = 0; i < limit; i++) {
      source.nextTuple();
      Assert.assertEquals(expectedTaskId, source.taskIds.get(0));
    }
    Assert.assertEquals(0, source.msgIdCache.size());
  }

}
