package backtype.storm.hooks;

import java.util.Map;

import backtype.storm.hooks.info.BoltAckInfo;
import backtype.storm.hooks.info.BoltExecuteInfo;
import backtype.storm.hooks.info.BoltFailInfo;
import backtype.storm.hooks.info.EmitInfo;
import backtype.storm.hooks.info.SpoutAckInfo;
import backtype.storm.hooks.info.SpoutFailInfo;
import backtype.storm.task.TopologyContext;

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
