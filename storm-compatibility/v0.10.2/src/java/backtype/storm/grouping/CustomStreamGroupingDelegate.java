/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package backtype.storm.grouping;

import java.util.List;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.task.TopologyContext;

public class CustomStreamGroupingDelegate implements
    org.apache.heron.api.grouping.CustomStreamGrouping {
  private static final long serialVersionUID = 2286979042095596191L;
  private CustomStreamGrouping delegate;

  public CustomStreamGroupingDelegate(CustomStreamGrouping delegate) {
    this.delegate = delegate;
  }

  @Override
  public void prepare(org.apache.heron.api.topology.TopologyContext context,
                      String component, String streamId,
                      List<Integer> targetTasks) {
    TopologyContext c = new TopologyContext(context);
    GlobalStreamId g = new GlobalStreamId(component, streamId);
    delegate.prepare(c, g, targetTasks);
  }

  @Override
  public List<Integer> chooseTasks(List<Object> values) {
    return delegate.chooseTasks(-1, values);
  }
}
