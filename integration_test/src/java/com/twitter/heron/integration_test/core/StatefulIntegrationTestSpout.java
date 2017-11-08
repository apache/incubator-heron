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
package com.twitter.heron.integration_test.core;

import java.io.Serializable;

import com.twitter.heron.api.spout.IRichSpout;
import com.twitter.heron.api.state.State;
import com.twitter.heron.api.topology.IStatefulComponent;

public class StatefulIntegrationTestSpout<K extends Serializable, V extends Serializable>
    extends IntegrationTestSpout implements IStatefulComponent<K, V> {
  private static final long serialVersionUID = -3651385732407518328L;
  private IStatefulComponent<K, V> delegate;

  public StatefulIntegrationTestSpout(IRichSpout delegateSpout,
                                      int maxExecutions, String topologyStartedStateUrl) {
    super(delegateSpout, maxExecutions, topologyStartedStateUrl);
    this.delegate = (IStatefulComponent<K, V>) delegateSpout;
  }

  @Override
  public void initState(State<K, V> state) {
    this.delegate.initState(state);
  }

  @Override
  public void preSave(String checkpointId) {
    this.delegate.preSave(checkpointId);
  }
}
