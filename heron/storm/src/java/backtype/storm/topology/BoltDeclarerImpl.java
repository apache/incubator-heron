package backtype.storm.topology;

import java.util.Map;

import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.grouping.CustomStreamGroupingDelegate;

public class BoltDeclarerImpl implements BoltDeclarer {
  private com.twitter.heron.api.topology.BoltDeclarer delegate;

  public BoltDeclarerImpl(com.twitter.heron.api.topology.BoltDeclarer delegate) {
    this.delegate = delegate;
  }

  @Override
  public BoltDeclarer addConfigurations(Map conf) {
    delegate.addConfigurations(conf);
    return this;
  }

  @Override
  public BoltDeclarer addConfiguration(String config, Object value) {
    delegate.addConfiguration(config, value);
    return this;
  }

  @Override
  public BoltDeclarer setDebug(boolean debug) {
    delegate.setDebug(debug);
    return this;
  }

  @Override
  public BoltDeclarer setMaxTaskParallelism(Number val) {
    // Heron does not support this
    return this;
  }

  @Override
  public BoltDeclarer setMaxSpoutPending(Number val) {
    delegate.setMaxSpoutPending(val);
    return this;
  }

  @Override
  public BoltDeclarer setNumTasks(Number val) {
    // Heron does not support this
    return this;
  }

  @Override
  public BoltDeclarer fieldsGrouping(String componentId, Fields fields) {
    return fieldsGrouping(componentId, Utils.DEFAULT_STREAM_ID, fields);
  }

  @Override
  public BoltDeclarer fieldsGrouping(String componentId, String streamId, Fields fields) {
    delegate.fieldsGrouping(componentId, streamId, fields.getDelegate());
    return this;
  }

  @Override
  public BoltDeclarer globalGrouping(String componentId) {
    return globalGrouping(componentId, Utils.DEFAULT_STREAM_ID);
  }

  @Override
  public BoltDeclarer globalGrouping(String componentId, String streamId) {
    delegate.globalGrouping(componentId, streamId);
    return this;
  }

  @Override
  public BoltDeclarer shuffleGrouping(String componentId) {
    return shuffleGrouping(componentId, Utils.DEFAULT_STREAM_ID);
  }

  @Override
  public BoltDeclarer shuffleGrouping(String componentId, String streamId) {
    delegate.shuffleGrouping(componentId, streamId);
    return this;
  }

  @Override
  public BoltDeclarer localOrShuffleGrouping(String componentId) {
    return localOrShuffleGrouping(componentId, Utils.DEFAULT_STREAM_ID);
  }

  @Override
  public BoltDeclarer localOrShuffleGrouping(String componentId, String streamId) {
    delegate.localOrShuffleGrouping(componentId, streamId);
    return this;
  }

  @Override
  public BoltDeclarer noneGrouping(String componentId) {
    return noneGrouping(componentId, Utils.DEFAULT_STREAM_ID);
  }

  @Override
  public BoltDeclarer noneGrouping(String componentId, String streamId) {
    delegate.noneGrouping(componentId, streamId);
    return this;
  }

  @Override
  public BoltDeclarer allGrouping(String componentId) {
    return allGrouping(componentId, Utils.DEFAULT_STREAM_ID);
  }

  @Override
  public BoltDeclarer allGrouping(String componentId, String streamId) {
    delegate.allGrouping(componentId, streamId);
    return this;
  }

  @Override
  public BoltDeclarer directGrouping(String componentId) {
    return directGrouping(componentId, Utils.DEFAULT_STREAM_ID);
  }

  @Override
  public BoltDeclarer directGrouping(String componentId, String streamId) {
    delegate.directGrouping(componentId, streamId);
    return this;
  }

  @Override
  public BoltDeclarer customGrouping(String componentId, CustomStreamGrouping grouping) {
    return customGrouping(componentId, Utils.DEFAULT_STREAM_ID, grouping);
  }

  @Override
  public BoltDeclarer customGrouping(String componentId, String streamId, CustomStreamGrouping grouping) {
    delegate.customGrouping(componentId, streamId, new CustomStreamGroupingDelegate(grouping));
    return this;
  }
}
