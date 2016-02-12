package org.apache.storm.topology;

import org.apache.storm.tuple.Fields;

public interface OutputFieldsDeclarer {
    /**
     * Uses default stream id.
     */
    public void declare(Fields fields);
    public void declare(boolean direct, Fields fields);
    
    public void declareStream(String streamId, Fields fields);
    public void declareStream(String streamId, boolean direct, Fields fields);
}
