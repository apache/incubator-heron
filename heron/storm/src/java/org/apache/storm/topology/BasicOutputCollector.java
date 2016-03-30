package org.apache.storm.topology;

import java.util.List;

import org.apache.storm.task.IOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;

public class BasicOutputCollector implements IBasicOutputCollector {
    private OutputCollector out;
    private Tuple inputTuple;

    public BasicOutputCollector(OutputCollector out) {
        this.out = out;
    }

    public List<Integer> emit(String streamId, List<Object> tuple) {
        return out.emit(streamId, inputTuple, tuple);
    }

    public List<Integer> emit(List<Object> tuple) {
        return emit(Utils.DEFAULT_STREAM_ID, tuple);
    }

    public void setContext(Tuple inputTuple) {
        this.inputTuple = inputTuple;
    }

    public void emitDirect(int taskId, String streamId, List<Object> tuple) {
        out.emitDirect(taskId, streamId, inputTuple, tuple);
    }

    public void emitDirect(int taskId, List<Object> tuple) {
        emitDirect(taskId, Utils.DEFAULT_STREAM_ID, tuple);
    }

    protected IOutputCollector getOutputter() {
        return out;
    }

    public void reportError(Throwable t) {
        out.reportError(t);
    }
}
