package org.apache.heron.learners.classifiers.trees;

/*
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


import org.apache.samoa.instances.Instance;

/**
 * Class that represents inactive learning node. Inactive learning node is a node which only keeps track of the observed
 * class distribution. It does not store the statistic for splitting the node.
 * 
 * @author Arinto Murdopo
 * 
 */
final class InactiveLearningNode extends LearningNode {

  /**
	 * 
	 */
  private static final long serialVersionUID = -814552382883472302L;

  InactiveLearningNode(double[] initialClassObservation) {
    super(initialClassObservation);
  }

  @Override
  void learnFromInstance(Instance inst, ModelAggregatorProcessor proc) {
    this.observedClassDistribution.addToValue(
        (int) inst.classValue(), inst.weight());
  }

  @Override
  double[] getClassVotes(Instance inst, ModelAggregatorProcessor map) {
    return this.observedClassDistribution.getArrayCopy();
  }

}
