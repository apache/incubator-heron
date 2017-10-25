// Copyright 2017 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.twitter.heron.api.hooks.info;

import java.util.Collection;
import java.util.List;

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
