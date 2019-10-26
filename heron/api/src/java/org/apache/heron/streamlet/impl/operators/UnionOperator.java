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

/**
 * UnionOperator is the class that implements the union functionality.
 * Its a very simple bolt that re-emits every tuple that it sees.
 */
public class UnionOperator<I> extends StreamletOperator<I, I> {
  private static final long serialVersionUID = -7326832064961413315L;

  public UnionOperator() {
  }

  @SuppressWarnings("unchecked")
  @Override
  public void execute(Tuple tuple) {
    I obj = (I) tuple.getValue(0);
    collector.emit(new Values(obj));
    collector.ack(tuple);
  }
}
