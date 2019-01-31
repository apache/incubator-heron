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

import org.apache.heron.api.tuple.Tuple;
import org.apache.heron.api.tuple.Values;
import org.apache.heron.streamlet.SerializableFunction;

/**
 * MapOperator is the class that implements the map functionality.
 * It takes in the mapFunction Function as the input.
 * For every tuple, it applies the mapFunction, and emits the resulting value
 */
public class MapOperator<R, T> extends StreamletOperator<R, T> {
  private static final long serialVersionUID = -1303096133107278700L;
  private SerializableFunction<? super R, ? extends T> mapFn;

  public MapOperator(SerializableFunction<? super R, ? extends T> mapFn) {
    this.mapFn = mapFn;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void execute(Tuple tuple) {
    R obj = (R) tuple.getValue(0);
    T result = mapFn.apply(obj);
    collector.emit(new Values(result));
    collector.ack(tuple);
  }
}
