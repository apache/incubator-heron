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

package org.apache.storm.generated;

import java.util.Map;

import org.apache.heron.api.HeronTopology;

public class StormTopology {
  private HeronTopology topology;
  private Map<String, SpoutSpec> spouts; // required
  private Map<String, Bolt> bolts; // required

  public StormTopology() {
  }

  public StormTopology(HeronTopology topology) {
    this.topology = topology;
  }

  public HeronTopology getStormTopology() {
    return topology;
  }

  public int get_bolts_size() {
    return topology.getTopology().getBoltsCount();
  }

  public Map<String, Bolt> get_bolts() {
    return bolts;
  }

  @SuppressWarnings("HiddenField")
  public void set_bolts(Map<String, Bolt> bolts) {
    this.bolts = bolts;
  }

  public int get_spouts_size() {
    return this.spouts.size();
  }

  public Map<String, SpoutSpec> get_spouts() {
    return this.spouts;
  }

  @SuppressWarnings("HiddenField")
  public void set_spouts(Map<String, SpoutSpec> spouts) {
    this.spouts = spouts;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("StormTopology(");
    boolean first = true;
    sb.append("spouts:");
    if (get_bolts() == null) {
      sb.append("null");
    } else {
      sb.append(get_spouts());
    }
    first = false;
    if (!first) {
      sb.append(", ");
    }
    sb.append("bolts:");
    if (get_bolts() == null) {
      sb.append("null");
    } else {
      sb.append(get_bolts());
    }
    sb.append(")");
    return sb.toString();
  }
}
