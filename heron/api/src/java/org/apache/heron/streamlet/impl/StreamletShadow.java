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

package org.apache.heron.streamlet.impl.streamlets;

import java.util.List;
import java.util.Set;

import org.apache.heron.api.topology.TopologyBuilder;
import org.apache.heron.streamlet.impl.StreamletBaseImpl;
import org.apache.heron.streamlet.impl.StreamletImpl;

/**
 * StreamletShadow is a special kind of StreamletImpl object:
 * - It is still an StreamletImpl therefore it supports all Streamlet functions like filter()
 * and map(), and can be the parent object of other StreamletImpl objects. Therefore,
 * from API point of view, it can be used in the same way as a normal StreamletImpl object.
 * - However it is just an shadow object of a real StreamletImpl object and it doesn't
 * represent a node in the topology DAG. Therefore it can not be a child of another StreamletImpl
 * object. As the result, the shadow object is clonable, and it is fine to create multiple
 * StreamletShadow objects pointing to the same StreamletImpl object and have different properties.
 *
 * A StreamletShadow object can be used to decorate the real Streamletimpl object. This is
 * important for children StreamletImpl objects to consume output data from the same parent in
 * different ways, such as selecting different stream.
 *
 * Usage:
 * To create a shadow object that selecting "test" stream from an existing StreamletImpl
 * object(stream):
 *
 * StreamletImpl shadow = new StreamletShadow(stream) {
 *   Override
 *   public String getStreamId() {
 *     return "test";
 *   }
 * }
 *
 */
public class StreamletShadow<R> extends StreamletImpl<R> {
  private StreamletImpl<R> real;

  public StreamletShadow(StreamletImpl<R> real) {
    this.real = real;
  }

  public StreamletImpl<R> getReal() {
    return real;
  }

  /**
   * Gets the stream id of this Streamlet.
   * @return the stream id of this Streamlet
   */
  @Override
  public String getStreamId() {
    return real.getStreamId();
  }

  /*
   * Functions accessible by child objects need to be overriden (forwarding the call to
   * the real object since shadow object shouldn't have them)
   */
  @Override
  public StreamletShadow<R> setName(String sName) {
    real.setName(sName);
    return this;
  }

  @Override
  public String getName() {
    return real.getName();
  }

  @Override
  public StreamletShadow<R> setNumPartitions(int numPartitions) {
    real.setNumPartitions(numPartitions);
    return this;
  }

  @Override
  public int getNumPartitions() {
    return real.getNumPartitions();
  }

  /*
   * Functions related to topology building need to be overriden.
   */
  public <T> void addChild(StreamletBaseImpl<T> child) {
    real.addChild(child);
  }

  /**
   * Gets all the children of this streamlet.
   * Children of a streamlet are streamlets that are resulting from transformations of elements of
   * this and potentially other streamlets.
   * @return The kid streamlets
   */
  @Override
  public List<StreamletBaseImpl<?>> getChildren() {
    return real.getChildren();
  }

  @Override
  public void build(TopologyBuilder bldr, Set<String> stageNames) {
    throw new UnsupportedOperationException("build() in StreamletShadow should NOT be invoked");
  }

  @Override
  public boolean doBuild(TopologyBuilder bldr, Set<String> stageNames) {
    throw new UnsupportedOperationException("build() in StreamletShadow should NOT be invoked");
  }
}
