package com.twitter.heron.localmode.grouping;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.google.protobuf.ByteString;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.proto.system.HeronTuples;

import junit.framework.Assert;

public class FieldsGroupingTest {

  @Before
  public void before() throws Exception {
  }

  @After
  public void after() throws Exception {
  }

  /**
   * Test to make sure that a particular tuple maps
   * to the same task id
   */
  @Test
  public void testSameTupleToSameTask() throws Exception {
    List<Integer> task_ids = new LinkedList<>();
    task_ids.add(2);
    task_ids.add(4);
    task_ids.add(6);
    task_ids.add(8);

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

    FieldsGrouping g = new FieldsGrouping(is, schema, task_ids);
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
    Assert.assertTrue(task_ids.contains(new ArrayList<>(allDests).get(0)));
  }

  /**
   * Test that only the relevant fields are hashed
   */
  @Test
  public void testHashOnlyRelevantFields() throws Exception {
    List<Integer> task_ids = new LinkedList<>();
    for (int i = 0; i < 100; i++) {
      task_ids.add(i);
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

    FieldsGrouping g = new FieldsGrouping(is, schema, task_ids);
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
    Assert.assertEquals(task_ids.size(), allDests.size());

  }
}
