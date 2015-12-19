package com.twitter.heron.api.bolt;

import java.util.List;

public interface IBasicOutputCollector {
    List<Integer> emit(String streamId, List<Object> tuple);
    void emitDirect(int taskId, String streamId, List<Object> tuple);
    void reportError(Throwable t);
}
