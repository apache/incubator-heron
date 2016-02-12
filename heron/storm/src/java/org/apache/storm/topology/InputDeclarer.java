package org.apache.storm.topology;

import org.apache.storm.generated.GlobalStreamId;
//import org.apache.storm.generated.Grouping;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.tuple.Fields;

public interface InputDeclarer<T extends InputDeclarer> {
    public T fieldsGrouping(String componentId, Fields fields);
    public T fieldsGrouping(String componentId, String streamId, Fields fields);

    public T globalGrouping(String componentId);
    public T globalGrouping(String componentId, String streamId);

    public T shuffleGrouping(String componentId);
    public T shuffleGrouping(String componentId, String streamId);

    public T localOrShuffleGrouping(String componentId);
    public T localOrShuffleGrouping(String componentId, String streamId);

    public T noneGrouping(String componentId);
    public T noneGrouping(String componentId, String streamId);

    public T allGrouping(String componentId);
    public T allGrouping(String componentId, String streamId);

    public T directGrouping(String componentId);
    public T directGrouping(String componentId, String streamId);

    public T customGrouping(String componentId, CustomStreamGrouping grouping);
    public T customGrouping(String componentId, String streamId, CustomStreamGrouping grouping);
    
    // public T grouping(GlobalStreamId id, Grouping grouping);
}
