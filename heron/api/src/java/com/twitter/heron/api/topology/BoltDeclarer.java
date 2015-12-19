package com.twitter.heron.api.topology;

import java.util.List;
import java.util.LinkedList;
import java.util.Map;

import com.google.protobuf.ByteString;

import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.utils.Utils;
import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.api.grouping.CustomStreamGrouping;
import com.twitter.heron.api.bolt.IRichBolt;

public class BoltDeclarer extends BaseComponentDeclarer<BoltDeclarer> {
  private OutputFieldsGetter output;
  private List<TopologyAPI.InputStream.Builder> inputs;

  public BoltDeclarer(String name, IRichBolt bolt, Number taskParallelism) {
    super(name, bolt, taskParallelism);
    inputs = new LinkedList<TopologyAPI.InputStream.Builder>();
    output = new OutputFieldsGetter();
    bolt.declareOutputFields(output);
  }

  @Override
  public BoltDeclarer returnThis() {
    return this;
  }

  public void dump(TopologyAPI.Topology.Builder bldr) {
    TopologyAPI.Bolt.Builder boltBldr = TopologyAPI.Bolt.newBuilder();

    TopologyAPI.Component.Builder cbldr = TopologyAPI.Component.newBuilder();
    super.dump(cbldr);
    boltBldr.setComp(cbldr);

    for (TopologyAPI.InputStream.Builder iter : inputs) {
      boltBldr.addInputs(iter);
    }

    Map<String, TopologyAPI.StreamSchema.Builder> outs = output.getFieldsDeclaration();
    for (Map.Entry<String, TopologyAPI.StreamSchema.Builder> entry : outs.entrySet()) {
      TopologyAPI.OutputStream.Builder obldr = TopologyAPI.OutputStream.newBuilder();
      TopologyAPI.StreamId.Builder sbldr = TopologyAPI.StreamId.newBuilder();
      sbldr.setId(entry.getKey());
      sbldr.setComponentName(getName());
      obldr.setStream(sbldr);
      obldr.setSchema(entry.getValue());
      boltBldr.addOutputs(obldr);
    }

    bldr.addBolts(boltBldr);
  }

  public BoltDeclarer fieldsGrouping(String componentName, Fields fields) {
    return fieldsGrouping(componentName, Utils.DEFAULT_STREAM_ID, fields);
  }
  public BoltDeclarer fieldsGrouping(String componentName, String streamId, Fields fields) {
    TopologyAPI.InputStream.Builder bldr = TopologyAPI.InputStream.newBuilder();
    bldr.setStream(TopologyAPI.StreamId.newBuilder().setId(streamId).setComponentName(componentName));
    bldr.setGtype(TopologyAPI.Grouping.FIELDS);
    TopologyAPI.StreamSchema.Builder gfbldr = TopologyAPI.StreamSchema.newBuilder();
    for (int i = 0; i < fields.size(); ++i) {
      TopologyAPI.StreamSchema.KeyType.Builder ktBldr = TopologyAPI.StreamSchema.KeyType.newBuilder();
      ktBldr.setKey(fields.get(i));
      ktBldr.setType(TopologyAPI.Type.OBJECT);
      gfbldr.addKeys(ktBldr);
    }
    bldr.setGroupingFields(gfbldr);
    return grouping(bldr);
  }

  public BoltDeclarer globalGrouping(String componentName) {
    return globalGrouping(componentName, Utils.DEFAULT_STREAM_ID);
  }
  public BoltDeclarer globalGrouping(String componentName, String streamId) {
    TopologyAPI.InputStream.Builder bldr = TopologyAPI.InputStream.newBuilder();
    bldr.setStream(TopologyAPI.StreamId.newBuilder().setId(streamId).setComponentName(componentName));
    bldr.setGtype(TopologyAPI.Grouping.LOWEST);
    return grouping(bldr);
  }

  public BoltDeclarer shuffleGrouping(String componentName) {
    return shuffleGrouping(componentName, Utils.DEFAULT_STREAM_ID);
  }
  public BoltDeclarer shuffleGrouping(String componentName, String streamId) {
    TopologyAPI.InputStream.Builder bldr = TopologyAPI.InputStream.newBuilder();
    bldr.setStream(TopologyAPI.StreamId.newBuilder().setId(streamId).setComponentName(componentName));
    bldr.setGtype(TopologyAPI.Grouping.SHUFFLE);
    return grouping(bldr);
  }

  public BoltDeclarer localOrShuffleGrouping(String componentName) {
    return localOrShuffleGrouping(componentName, Utils.DEFAULT_STREAM_ID);
  }
  public BoltDeclarer localOrShuffleGrouping(String componentName, String streamId) {
    // TODO:- revisit this
    return shuffleGrouping(componentName, streamId);
  }

  public BoltDeclarer noneGrouping(String componentName) {
    return noneGrouping(componentName, Utils.DEFAULT_STREAM_ID);
  }
  public BoltDeclarer noneGrouping(String componentName, String streamId) {
    TopologyAPI.InputStream.Builder bldr = TopologyAPI.InputStream.newBuilder();
    bldr.setStream(TopologyAPI.StreamId.newBuilder().setId(streamId).setComponentName(componentName));
    bldr.setGtype(TopologyAPI.Grouping.NONE);
    return grouping(bldr);
  }

  public BoltDeclarer allGrouping(String componentName) {
    return allGrouping(componentName, Utils.DEFAULT_STREAM_ID);
  }
  public BoltDeclarer allGrouping(String componentName, String streamId) {
    TopologyAPI.InputStream.Builder bldr = TopologyAPI.InputStream.newBuilder();
    bldr.setStream(TopologyAPI.StreamId.newBuilder().setId(streamId).setComponentName(componentName));
    bldr.setGtype(TopologyAPI.Grouping.ALL);
    return grouping(bldr);
  }

  public BoltDeclarer directGrouping(String componentName) {
    return directGrouping(componentName, Utils.DEFAULT_STREAM_ID);
  }
  public BoltDeclarer directGrouping(String componentName, String streamId) {
    // TODO:- revisit this
    throw new RuntimeException("direct Grouping not implemented");
  }

  public BoltDeclarer customGrouping(String componentName, CustomStreamGrouping grouping) {
    return customGrouping(componentName, Utils.DEFAULT_STREAM_ID, grouping);
  }

  public BoltDeclarer customGrouping(String componentName, String streamId, CustomStreamGrouping grouping) {
    TopologyAPI.InputStream.Builder bldr = TopologyAPI.InputStream.newBuilder();
    bldr.setStream(TopologyAPI.StreamId.newBuilder().setId(streamId).setComponentName(componentName));
    bldr.setGtype(TopologyAPI.Grouping.CUSTOM);
    bldr.setCustomGroupingJavaObject(ByteString.copyFrom(Utils.serialize(grouping)));
    return grouping(bldr);
  }

  private BoltDeclarer grouping(TopologyAPI.InputStream.Builder stream) {
    inputs.add(stream);
    return this;
  }
}
