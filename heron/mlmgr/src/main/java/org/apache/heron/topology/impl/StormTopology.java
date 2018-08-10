package org.apache.samoa.topology.impl;

/*
 * #%L
 * SAMOA
 * %%
 * Copyright (C) 2014 - 2015 Apache Software Foundation
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import org.apache.samoa.topology.AbstractTopology;
import org.apache.samoa.topology.IProcessingItem;

import backtype.storm.topology.TopologyBuilder;

/**
 * Adaptation of SAMOA topology in samoa-storm
 * 
 * @author Arinto Murdopo
 * 
 */
public class StormTopology extends AbstractTopology {

  private TopologyBuilder builder;

  public StormTopology(String topologyName) {
    super(topologyName);
    this.builder = new TopologyBuilder();
  }

  @Override
  public void addProcessingItem(IProcessingItem procItem, int parallelismHint) {
    StormTopologyNode stormNode = (StormTopologyNode) procItem;
    stormNode.addToTopology(this, parallelismHint);
    super.addProcessingItem(procItem, parallelismHint);
  }

  public TopologyBuilder getStormBuilder() {
    return builder;
  }
}
