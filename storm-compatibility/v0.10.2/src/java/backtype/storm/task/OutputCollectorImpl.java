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

package backtype.storm.task;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;

/**
 * This output collector exposes the API for emitting tuples from an IRichBolt.
 * This is the core API for emitting tuples. For a simpler API, and a more restricted
 * form of stream processing, see IBasicBolt and BasicOutputCollector.
 */
public class OutputCollectorImpl extends OutputCollector {
  private org.apache.heron.api.bolt.OutputCollector delegate;

  public OutputCollectorImpl(org.apache.heron.api.bolt.OutputCollector delegate) {
    super(null);
    this.delegate = delegate;
  }

  @Override
  public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
    if (anchors != null) {
      ArrayList<org.apache.heron.api.tuple.Tuple> l =
          new ArrayList<org.apache.heron.api.tuple.Tuple>();
      for (Tuple t : anchors) {
        TupleImpl i = (TupleImpl) t;
        l.add(i.getDelegate());
      }
      return delegate.emit(streamId, l, tuple);
    } else {
      return delegate.emit(streamId, (Collection<org.apache.heron.api.tuple.Tuple>) null, tuple);
    }
  }

  @Override
  public void emitDirect(
      int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {
    if (anchors != null) {
      ArrayList<org.apache.heron.api.tuple.Tuple> l =
          new ArrayList<org.apache.heron.api.tuple.Tuple>();
      for (Tuple t : anchors) {
        TupleImpl i = (TupleImpl) t;
        l.add(i.getDelegate());
      }
      delegate.emitDirect(taskId, streamId, l, tuple);
    } else {
      delegate.emitDirect(
          taskId, streamId, (Collection<org.apache.heron.api.tuple.Tuple>) null, tuple);
    }
  }

  @Override
  public void ack(Tuple input) {
    TupleImpl i = (TupleImpl) input;
    delegate.ack(i.getDelegate());
  }

  @Override
  public void fail(Tuple input) {
    TupleImpl i = (TupleImpl) input;
    delegate.fail(i.getDelegate());
  }

  @Override
  public void reportError(Throwable error) {
    delegate.reportError(error);
  }
}
