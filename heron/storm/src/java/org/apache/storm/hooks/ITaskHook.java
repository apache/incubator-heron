package org.apache.storm.hooks;

import java.util.Map;

import org.apache.storm.hooks.info.BoltAckInfo;
import org.apache.storm.hooks.info.BoltExecuteInfo;
import org.apache.storm.hooks.info.BoltFailInfo;
import org.apache.storm.hooks.info.EmitInfo;
import org.apache.storm.hooks.info.SpoutAckInfo;
import org.apache.storm.hooks.info.SpoutFailInfo;
import org.apache.storm.task.TopologyContext;

public interface ITaskHook {
    void prepare(Map conf, TopologyContext context);
    void cleanup();
    void emit(EmitInfo info);
    void spoutAck(SpoutAckInfo info);
    void spoutFail(SpoutFailInfo info);
    void boltExecute(BoltExecuteInfo info);
    void boltAck(BoltAckInfo info);
    void boltFail(BoltFailInfo info);
}
