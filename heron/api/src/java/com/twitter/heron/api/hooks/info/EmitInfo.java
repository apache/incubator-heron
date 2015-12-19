package com.twitter.heron.api.hooks.info;

import java.util.Collection;
import java.util.List;

import com.twitter.heron.api.tuple.Tuple;

public class EmitInfo {
  private List<Object> values;
  private String stream;
  private int taskId;
  private Collection<Integer> outTasks;
    
  public EmitInfo(List<Object> values, String stream, int taskId, Collection<Integer> outTasks) {
    this.values = values;
    this.stream = stream;
    this.taskId = taskId;
    this.outTasks = outTasks;
  }

  public List<Object> getValues() {
    return values;
  }

  public String getStream() {
    return stream;
  }

  public int getTaskId() {
    return taskId;
  }

  public Collection<Integer> getOutTasks() {
    return outTasks;
  }
}
