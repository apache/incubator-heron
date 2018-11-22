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

import java.util.Map;

import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.api.tuple.Tuple;
import org.apache.heron.api.tuple.Values;
import org.apache.heron.streamlet.SerializablePredicate;

/**
 * SplitOperator is the class that implements the split functionality.
 * It takes in the split function as the input and use it to process tuples and
 * get the output stream id and emit to the specific streams.
 * Note that one tuple can be emitted to multiple or zero streams.
 */
public class SplitOperator<R> extends StreamletOperator<R, R> {
  private Map<String, SerializablePredicate<R>> splitFns;

  public SplitOperator(Map<String, SerializablePredicate<R>> splitFns) {
    this.splitFns = splitFns;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void execute(Tuple tuple) {
    R obj = (R) tuple.getValue(0);
    for (Map.Entry<String, SerializablePredicate<R>> entry: splitFns.entrySet()) {
      if (entry.getValue().test(obj)) {
        collector.emit(entry.getKey(), new Values(obj));
      }
    }
    collector.ack(tuple);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    //super.declareOutputFields(declarer);
    for (String stream: splitFns.keySet()) {
      declarer.declareStream(stream, new Fields(OUTPUT_FIELD_NAME));
    }
  }
}
