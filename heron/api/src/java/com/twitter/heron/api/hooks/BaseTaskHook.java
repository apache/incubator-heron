package com.twitter.heron.api.hooks;

import java.util.Map;

import com.twitter.heron.api.hooks.info.BoltAckInfo;
import com.twitter.heron.api.hooks.info.BoltExecuteInfo;
import com.twitter.heron.api.hooks.info.BoltFailInfo;
import com.twitter.heron.api.hooks.info.EmitInfo;
import com.twitter.heron.api.hooks.info.SpoutAckInfo;
import com.twitter.heron.api.hooks.info.SpoutFailInfo;
import com.twitter.heron.api.topology.TopologyContext;

public class BaseTaskHook implements ITaskHook {
  @Override
  public void prepare(Map conf, TopologyContext context) {
  }

  @Override
  public void cleanup() {
  }    

  @Override
  public void emit(EmitInfo info) {
  }

  @Override
  public void spoutAck(SpoutAckInfo info) {
  }

  @Override
  public void spoutFail(SpoutFailInfo info) {
  }

  @Override
  public void boltAck(BoltAckInfo info) {
  }

  @Override
  public void boltFail(BoltFailInfo info) {
  }

  @Override
  public void boltExecute(BoltExecuteInfo info) {
  }
}
