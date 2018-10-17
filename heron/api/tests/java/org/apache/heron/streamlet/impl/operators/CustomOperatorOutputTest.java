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

package org.apache.heron.streamlet.impl.operators;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.junit.Test;

import org.apache.heron.api.utils.Utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CustomOperatorOutputTest {
  @Test
  public void testOutputSucceed() {
    CustomOperatorOutput<Integer> output = CustomOperatorOutput.succeed();
    assertTrue(output.isSuccessful());
    assertTrue(output.getData().isEmpty());
    assertTrue(output.isAnchored());
  }

  @Test
  public void testOutputSucceedWithObject() {
    CustomOperatorOutput<Integer> output = CustomOperatorOutput.succeed(100);
    assertTrue(output.isSuccessful());
    assertEquals(output.getData().size(), 1);
    assertTrue(output.getData().containsKey(Utils.DEFAULT_STREAM_ID));

    List<Integer> data = output.getData().get(Utils.DEFAULT_STREAM_ID);
    assertEquals(data.size(), 1);
    assertEquals(data.toArray()[0], 100);
    assertTrue(output.isAnchored());
  }

  @Test
  public void testOutputSucceedWithList() {
    CustomOperatorOutput<Integer> output =
        CustomOperatorOutput.succeed(Arrays.asList(100));
    assertTrue(output.isSuccessful());
    assertEquals(output.getData().size(), 1);
    assertTrue(output.getData().containsKey(Utils.DEFAULT_STREAM_ID));

    List<Integer> data = output.getData().get(Utils.DEFAULT_STREAM_ID);
    assertEquals(data.size(), 1);
    assertEquals(data.toArray()[0], 100);
    assertTrue(output.isAnchored());
  }

  @Test
  public void testOutputSucceedWithMap() {
    HashMap<String, List<Integer>> map = new HashMap<String, List<Integer>>();
    List<Integer> list1 = Arrays.asList(100);
    List<Integer> list2 = Arrays.asList(200);
    map.put("stream1", list1);
    map.put("stream2", list2);

    CustomOperatorOutput<Integer> output = CustomOperatorOutput.succeed(map);
    assertTrue(output.isSuccessful());
    assertEquals(output.getData().size(), 2);
    assertTrue(output.getData().containsKey("stream1"));
    assertTrue(output.getData().containsKey("stream2"));

    List<Integer> data = output.getData().get("stream1");
    assertEquals(data.size(), 1);
    assertEquals(data.toArray()[0], 100);
    assertTrue(output.isAnchored());

    data = output.getData().get("stream2");
    assertEquals(data.size(), 1);
    assertEquals(data.toArray()[0], 200);
    assertTrue(output.isAnchored());
  }

  @Test
  public void testOutputFail() {
    CustomOperatorOutput<Integer> output = CustomOperatorOutput.fail();
    assertFalse(output.isSuccessful());
  }

  @Test
  public void testOutputWithAnchor() {
    CustomOperatorOutput<Integer> anchored =
        CustomOperatorOutput.<Integer>create().withAnchor(true);
    assertTrue(anchored.isAnchored());

    CustomOperatorOutput<Integer> unanchored =
        CustomOperatorOutput.<Integer>create().withAnchor(false);
    assertFalse(unanchored.isAnchored());
  }
}
