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

import java.util.Set;

import org.apache.heron.api.spout.IRichSpout;
import org.apache.heron.api.topology.TopologyBuilder;
import org.apache.heron.streamlet.impl.StreamletImpl;

/**
 * SpoutStreamlet is a quick way of creating a Streamlet
 * from an user supplied Spout object. The spout is the
 * source of all tuples for this Streamlet.
 */
public class SpoutStreamlet<R> extends StreamletImpl<R> {
  private IRichSpout spout;

  public SpoutStreamlet(IRichSpout spout) {
    this.spout = spout;
    setNumPartitions(1);
  }

  @Override
  public boolean doBuild(TopologyBuilder bldr, Set<String> stageNames) {
    setDefaultNameIfNone(StreamletNamePrefix.SPOUT, stageNames);
    bldr.setSpout(getName(), spout, getNumPartitions());
    return true;
  }
}
