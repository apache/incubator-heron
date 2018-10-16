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


package org.apache.heron.streamlet.impl.operators;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import org.apache.heron.api.bolt.OutputCollector;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Tuple;
import org.apache.heron.api.tuple.Values;

/**
 * CustomOperator is the base class for all user defined operators.
 * Usage:
 * 1. Create user defined operator
 *   class MyOperator extends CustomOperator<String, String> {
 *     public MyOperator() {
 *       ...
 *     }
 *
 *     @override
 *     public Optional<CustomOperatorOutput<String>> CustomOperatorOutput<T> process(String data) {
 *       ...
 *     }
 *   }
 * Note that users can override low level bolt functions like getComponentConfiguration() in order
 * to implement more advanced features.
 * 2. Use it in Streamlet
 *   ....
 *   .perform(new MyOperator())
 *   ....
 */
public abstract class CustomOperator<R, T> extends StreamletOperator<R, T> {

  private OutputCollector outputCollector;

  /**
   * Process function to be implemented by all custom operators.
   * @param data The data object to be processed
   * @return a CustomOperatorOutput that contains process results and other flags. The output is wrapped
   *     in Optional. When the process failed, return none
   */
  public abstract Optional<CustomOperatorOutput<T>> process(R data);

  /**
   * Called when a task for this component is initialized within a worker on the cluster.
   * It provides the bolt with the environment in which the bolt executes.
   * @param heronConf The Heron configuration for this bolt. This is the configuration provided to the topology merged in with cluster configuration on this machine.
   * @param context This object can be used to get information about this task's place within the topology, including the task id and component id of this task, input and output information, etc.
   * @param collector The collector is used to emit tuples from this bolt. Tuples can be emitted at any time, including the prepare and cleanup methods. The collector is thread-safe and should be saved as an instance variable of this bolt object.
   */
  public void prepare(Map<String, Object> heronConf,
                      TopologyContext context,
                      OutputCollector collector) {

    this.outputCollector = collector;
  }

  /**
   * Implementation of execute() function to support type safty in Streamlet API
   * The function is responsible for:
   * - convert incoming tuple to the specified type
   * - call the new process() function which has type safty support
   * - emit/ack/fail the results
   */
  @SuppressWarnings("unchecked")
  @Override
  public void execute(Tuple tuple) {
    R data = (R) tuple.getValue(0);
    Optional<CustomOperatorOutput<T>> results = process(data);

    if (results.isPresent()) {
      Collection<Tuple> anchors = results.get().isAnchored() ? Arrays.asList(tuple) : null;
      emitResults(results.get().getData(), anchors);
      outputCollector.ack(tuple);
    } else {
      outputCollector.fail(tuple);
    }
  }

  /**
   * Convert process results to tuples and emit out to the downstream
   * @param data results collections with corresponding stream id
   * @param anchors anchors to be used when emitting tuples
   */
  private void emitResults(Map<String, Collection<T>> data, Collection<Tuple> anchors) {
    for (Map.Entry<String, Collection<T>> entry : data.entrySet()) {
      String streamId = entry.getKey();
      for (T value: entry.getValue()) {
        outputCollector.emit(streamId, anchors, new Values(value));
      }
    }
  }
}
