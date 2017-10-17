//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package com.twitter.heron.dsl.impl;

import java.io.Serializable;
import java.util.Map;

import com.twitter.heron.api.state.State;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.dsl.Context;

/**
 * Context is the information available at runtime for operators like transform.
 * It contains basic things like config, runtime information like task,
 * the stream that it is operating on, ProcessState, etc.
 */
public class ContextImpl implements Context {
  private TopologyContext topologyContext;
  private Map<String, Object> topologyConfig;
  private State<Serializable, Serializable> state;

  public ContextImpl(TopologyContext topologyContext, Map<String, Object> topologyConfig,
                     State<Serializable, Serializable> state) {
    this.topologyContext = topologyContext;
    this.topologyConfig = topologyConfig;
    this.state = state;
  }

  @Override
  public int getTaskId() {
    return topologyContext.getThisTaskId();
  }

  @Override
  public Map<String, Object> getConfig() {
    return topologyConfig;
  }

  @Override
  public String getStreamName() {
    return topologyContext.getThisStreams().iterator().next();
  }

  @Override
  public int getStreamPartition() {
    return topologyContext.getThisTaskIndex();
  }

  @Override
  public State<Serializable, Serializable> getState() {
    return state;
  }
}
