package com.twitter.heron.api.topology;

import java.util.Map;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.api.spout.IRichSpout;

public class SpoutDeclarer extends BaseComponentDeclarer<SpoutDeclarer> {
  private OutputFieldsGetter output;

  public SpoutDeclarer(String name, IRichSpout spout, Number taskParallelism) {
    super(name, spout, taskParallelism);
    output = new OutputFieldsGetter();
    spout.declareOutputFields(output);
  }

  @Override
  public SpoutDeclarer returnThis() {
    return this;
  }

  public void dump(TopologyAPI.Topology.Builder bldr) {
    TopologyAPI.Spout.Builder spoutBldr = TopologyAPI.Spout.newBuilder();

    TopologyAPI.Component.Builder compBldr = TopologyAPI.Component.newBuilder();
    super.dump(compBldr);
    spoutBldr.setComp(compBldr);

    Map<String, TopologyAPI.StreamSchema.Builder> outs = output.getFieldsDeclaration();
    for (Map.Entry<String, TopologyAPI.StreamSchema.Builder> entry : outs.entrySet()) {
      TopologyAPI.OutputStream.Builder obldr = TopologyAPI.OutputStream.newBuilder();
      TopologyAPI.StreamId.Builder sbldr = TopologyAPI.StreamId.newBuilder();
      sbldr.setId(entry.getKey());
      sbldr.setComponentName(getName());
      obldr.setStream(sbldr);
      obldr.setSchema(entry.getValue());
      spoutBldr.addOutputs(obldr);
    }

    bldr.addSpouts(spoutBldr);
  }
}
