package org.apache.storm.hooks;

import java.util.Map;

import org.apache.storm.hooks.info.BoltAckInfo;
import org.apache.storm.hooks.info.BoltExecuteInfo;
import org.apache.storm.hooks.info.BoltFailInfo;
import org.apache.storm.hooks.info.EmitInfo;
import org.apache.storm.hooks.info.SpoutAckInfo;
import org.apache.storm.hooks.info.SpoutFailInfo;
import org.apache.storm.task.TopologyContext;

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
