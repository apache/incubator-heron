package com.twitter.heron.api.grouping;

import java.io.Serializable;
import java.util.List;

import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Values;

public interface CustomStreamGrouping extends Serializable {
    
   /**
     * Tells the stream grouping at runtime the tasks in the target bolt.
     * This information should be used in chooseTasks to determine the target tasks.
     * 
     * It also tells the grouping the metadata on the stream this grouping will be used on.
     */
   void prepare(TopologyContext context, String component, String streamId, List<Integer> targetTasks);
    
   /**
     * This function implements a custom stream grouping. It takes in as input
     * the number of tasks in the target bolt in prepare and returns the
     * tasks to send the tuples to.
     * 
     * @param values the values to group on
     */
   List<Integer> chooseTasks(List<Object> values); 
}
