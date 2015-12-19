package com.twitter.heron.api.topology;

import com.twitter.heron.api.tuple.Fields;

public interface OutputFieldsDeclarer {
    /**
     * Uses default stream id.
     */
    public void declare(Fields fields);
    public void declare(boolean direct, Fields fields);
    
    public void declareStream(String streamId, Fields fields);
    public void declareStream(String streamId, boolean direct, Fields fields);
}
