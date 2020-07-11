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

package org.apache.storm.windowing;

import java.util.LinkedList;
import java.util.List;

import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;

public class TupleWindowImpl implements TupleWindow {

  private final org.apache.heron.api.windowing.TupleWindow delegate;

  public TupleWindowImpl(org.apache.heron.api.windowing.TupleWindow tupleWindow) {
    this.delegate = tupleWindow;
  }

  @Override
  public List<Tuple> get() {
    return convert(this.delegate.get());
  }

  @Override
  public List<Tuple> getNew() {
    return convert(this.delegate.getNew());
  }

  @Override
  public List<Tuple> getExpired() {
    return convert(this.delegate.getExpired());
  }

  @Override
  public Long getEndTimestamp() {
    return this.delegate.getEndTimestamp();
  }

  @Override
  public Long getStartTimestamp() {
    return this.delegate.getStartTimestamp();
  }

  private static List<Tuple> convert(List<org.apache.heron.api.tuple.Tuple> tuples) {
    List<Tuple> ret = new LinkedList<>();
    for (org.apache.heron.api.tuple.Tuple tuple : tuples) {
      ret.add(new TupleImpl(tuple));
    }
    return ret;
  }
}
