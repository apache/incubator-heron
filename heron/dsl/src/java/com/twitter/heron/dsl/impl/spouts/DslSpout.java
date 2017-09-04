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

package com.twitter.heron.dsl.impl.spouts;

import java.io.Serializable;

import com.twitter.heron.api.spout.BaseRichSpout;
import com.twitter.heron.api.state.State;
import com.twitter.heron.api.topology.IStatefulComponent;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.tuple.Fields;

public abstract class DslSpout extends BaseRichSpout
    implements IStatefulComponent<Serializable, Serializable> {

  private static final long serialVersionUID = 8583965332619565343L;

  @Override
  public void initState(State<Serializable, Serializable> state) { }

  @Override
  public void preSave(String checkpointId) { }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("output"));
  }
}
