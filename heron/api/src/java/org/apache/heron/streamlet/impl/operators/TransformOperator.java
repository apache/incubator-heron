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

import java.io.Serializable;
import java.util.Map;

import org.apache.heron.api.bolt.OutputCollector;
import org.apache.heron.api.state.State;
import org.apache.heron.api.topology.IStatefulComponent;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Tuple;
import org.apache.heron.api.tuple.Values;
import org.apache.heron.streamlet.Context;
import org.apache.heron.streamlet.SerializableTransformer;
import org.apache.heron.streamlet.impl.ContextImpl;

/**
 * TransformOperator is the class that implements the transform functionality.
 * It takes in the transformFunction Function as the input.
 * It calls the transformFunction setup/cleanup at the beginning/end of the
 * processing. And for every tuple, it applies the transformFunction, and emits the resulting value
 */
public class TransformOperator<R, T> extends StreamletOperator<R, T>
    implements IStatefulComponent<Serializable, Serializable> {

  private static final long serialVersionUID = 429297144878185182L;
  private SerializableTransformer<? super R, ? extends T> serializableTransformer;

  private State<Serializable, Serializable> state;

  public TransformOperator(
      SerializableTransformer<? super R, ? extends T> serializableTransformer) {
    this.serializableTransformer = serializableTransformer;
  }

  @Override
  public void initState(State<Serializable, Serializable> startupState) {
    this.state = startupState;
  }

  @Override
  public void preSave(String checkpointId) {
  }

  @Override
  public void cleanup() {
    serializableTransformer.cleanup();
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void prepare(Map<String, Object> map,
                      TopologyContext topologyContext,
                      OutputCollector outputCollector) {
    super.prepare(map, topologyContext, outputCollector);
    Context context = new ContextImpl(topologyContext, map, state);
    serializableTransformer.setup(context);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void execute(Tuple tuple) {
    R obj = (R) tuple.getValue(0);
    serializableTransformer.transform(obj, x -> collector.emit(new Values(x)));
    collector.ack(tuple);
  }
}
