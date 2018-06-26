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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.google.protobuf.ByteString;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.proto.system.HeronTuples;

public class FieldsGroupingTest {

  @Before
  public void before() throws Exception {
  }

  @After
  public void after() throws Exception {
  }

  /**
   * Test to make sure that getListToSend
   * will not throw exceptions in corner cases
   */
  @Test
  public void testGetListToSend() throws Exception {
    List<Integer> taskIds = new LinkedList<>();
    for (int i = 0; i < 100; i++) {
      taskIds.add(i);
    }

    TopologyAPI.StreamSchema.KeyType kt =
        TopologyAPI.StreamSchema.KeyType.newBuilder().
            setType(TopologyAPI.Type.OBJECT).
            setKey("field1").
            build();

    TopologyAPI.StreamSchema schema = TopologyAPI.StreamSchema.newBuilder().addKeys(kt).build();

    TopologyAPI.InputStream is = TopologyAPI.InputStream.newBuilder().
        setGroupingFields(schema).
        setGtype(TopologyAPI.Grouping.FIELDS).
        setStream(TopologyAPI.StreamId.newBuilder().
            setComponentName("componentName").
            setId("id"))
        .build();

    HeronTuples.HeronDataTuple tuple =
        HeronTuples.HeronDataTuple.newBuilder().
            setKey(-1).
            addValues(ByteString.copyFromUtf8("")).
            build();

    // It will not throw exceptions though the hashCode of ByteString is Integer.MIN_VALUE
    FieldsGrouping g = Mockito.spy(new FieldsGrouping(is, schema, taskIds));

    Mockito.doReturn(Integer.MIN_VALUE).
        when(g).getHashCode(Mockito.any(ByteString.class));
    g.getListToSend(tuple);
    // Assert True here to make Test Tool take this test case into account
    Assert.assertTrue(true);
  }

  /**
   * Test to make sure that a particular tuple maps
   * to the same task id
   */
  @Test
  public void testSameTupleToSameTask() throws Exception {
    List<Integer> taskIds = new LinkedList<>();
    taskIds.add(2);
    taskIds.add(4);
    taskIds.add(6);
    taskIds.add(8);

    TopologyAPI.StreamSchema.KeyType kt =
        TopologyAPI.StreamSchema.KeyType.newBuilder().
            setType(TopologyAPI.Type.OBJECT).
            setKey("field1").
            build();

    TopologyAPI.StreamSchema schema = TopologyAPI.StreamSchema.newBuilder().addKeys(kt).build();

    TopologyAPI.InputStream is = TopologyAPI.InputStream.newBuilder().
        setGroupingFields(schema).
        setGtype(TopologyAPI.Grouping.FIELDS).
        setStream(TopologyAPI.StreamId.newBuilder().
            setComponentName("componentName").
            setId("id"))
        .build();

    FieldsGrouping g = new FieldsGrouping(is, schema, taskIds);
    HeronTuples.HeronDataTuple tuple =
        HeronTuples.HeronDataTuple.newBuilder().
            setKey(-1).
            addValues(ByteString.copyFromUtf8("")).
            build();

    Set<Integer> allDests = new HashSet<>();
    for (int i = 0; i < 1000; i++) {
      List<Integer> dests = g.getListToSend(tuple);

      Assert.assertEquals(1, dests.size());
      allDests.add(dests.get(0));
    }

    Assert.assertEquals(1, allDests.size());
    Assert.assertTrue(taskIds.contains(new ArrayList<>(allDests).get(0)));
  }

  /**
   * Test that only the relevant fields are hashed
   */
  @Test
  public void testHashOnlyRelevantFields() throws Exception {
    List<Integer> taskIds = new LinkedList<>();
    for (int i = 0; i < 100; i++) {
      taskIds.add(i);
    }

    TopologyAPI.StreamSchema.KeyType kt =
        TopologyAPI.StreamSchema.KeyType.newBuilder().
            setType(TopologyAPI.Type.OBJECT).
            setKey("field1").
            build();

    TopologyAPI.StreamSchema s = TopologyAPI.StreamSchema.newBuilder().addKeys(kt).build();

    TopologyAPI.InputStream is = TopologyAPI.InputStream.newBuilder().
        setGroupingFields(s).
        setGtype(TopologyAPI.Grouping.FIELDS).
        setStream(TopologyAPI.StreamId.newBuilder().
            setComponentName("componentName").
            setId("id"))
        .build();


    TopologyAPI.StreamSchema.KeyType kt1 =
        TopologyAPI.StreamSchema.KeyType.newBuilder().
            setType(TopologyAPI.Type.OBJECT).
            setKey("field1").
            build();
    TopologyAPI.StreamSchema.KeyType kt2 =
        TopologyAPI.StreamSchema.KeyType.newBuilder().
            setType(TopologyAPI.Type.OBJECT).
            setKey("field2").
            build();
    TopologyAPI.StreamSchema schema =
        TopologyAPI.StreamSchema.newBuilder().
            addKeys(kt1).
            addKeys(kt2).
            build();

    FieldsGrouping g = new FieldsGrouping(is, schema, taskIds);
    Set<Integer> allDests = new HashSet<>();
    for (int i = 0; i < 1000; i++) {
      HeronTuples.HeronDataTuple tuple =
          HeronTuples.HeronDataTuple.newBuilder().
              setKey(-1).
              addValues(ByteString.copyFromUtf8("this matters")).
              addValues(ByteString.copyFromUtf8("this doesnt " + i)).
              build();

      List<Integer> dests = g.getListToSend(tuple);
      Assert.assertEquals(1, dests.size());
      allDests.add(dests.get(0));
    }

    Assert.assertEquals(1, allDests.size());

    allDests.clear();
    for (int i = 0; i < 1000 * 1000; i++) {
      HeronTuples.HeronDataTuple tuple =
          HeronTuples.HeronDataTuple.newBuilder().
              setKey(-1).
              addValues(ByteString.copyFromUtf8("this changes " + i)).
              addValues(ByteString.copyFromUtf8("this doesnt ")).
              build();

      List<Integer> dests = g.getListToSend(tuple);
      Assert.assertEquals(1, dests.size());
      allDests.add(dests.get(0));
    }
    Assert.assertEquals(taskIds.size(), allDests.size());

  }
}
