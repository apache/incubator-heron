package org.apache.storm.grouping;

import java.util.List;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.task.TopologyContext;

public class CustomStreamGroupingDelegate implements com.twitter.heron.api.grouping.CustomStreamGrouping {
  private CustomStreamGrouping delegate;

  public CustomStreamGroupingDelegate(CustomStreamGrouping delegate) {
    this.delegate = delegate;
  }

  @Override
  public void prepare(com.twitter.heron.api.topology.TopologyContext context,
                      String component, String streamId,
                      List<Integer> targetTasks) {
    TopologyContext c = new TopologyContext(context);
    GlobalStreamId g = new GlobalStreamId(component, streamId);
    delegate.prepare(c, g, targetTasks);
  }
    
  @Override
  public List<Integer> chooseTasks(List<Object> values) {
    return delegate.chooseTasks(-1, values);
  }
}
