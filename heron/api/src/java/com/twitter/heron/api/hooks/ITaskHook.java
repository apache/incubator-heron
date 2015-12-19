package com.twitter.heron.api.hooks;

import java.util.Map;

import com.twitter.heron.api.hooks.info.BoltAckInfo;
import com.twitter.heron.api.hooks.info.BoltExecuteInfo;
import com.twitter.heron.api.hooks.info.BoltFailInfo;
import com.twitter.heron.api.hooks.info.EmitInfo;
import com.twitter.heron.api.hooks.info.SpoutAckInfo;
import com.twitter.heron.api.hooks.info.SpoutFailInfo;
import com.twitter.heron.api.topology.TopologyContext;

public interface ITaskHook {
  /**
   * Called after the spout/bolt's open/prepare method is called
   * conf is the Config thats passed to the topology
   */ 
  void prepare(Map conf, TopologyContext context);

  /**
   * Called just before the spout/bolt's cleanup method is called.
   */ 
  void cleanup();

  /**
   * Called everytime a tuple is emitted in spout/bolt
   */ 
  void emit(EmitInfo info);

  /**
   * Called in spout everytime a tuple gets acked
   */ 
  void spoutAck(SpoutAckInfo info);

  /**
   * Called in spout everytime a tuple gets failed
   */ 
  void spoutFail(SpoutFailInfo info);

  /**
   * Called in bolt everytime a tuple gets executed
   */ 
  void boltExecute(BoltExecuteInfo info);

  /**
   * Called in bolt everytime a tuple gets acked
   */ 
  void boltAck(BoltAckInfo info);

  /**
   * Called in bolt everytime a tuple gets failed
   */ 
  void boltFail(BoltFailInfo info);
}
