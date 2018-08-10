package org.apache.samoa.learners.classifiers.trees;

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

import org.apache.samoa.instances.Instance;

/**
 * Abstract class that represents a learning node
 * 
 * @author Arinto Murdopo
 * 
 */
abstract class LearningNode extends Node {

  private static final long serialVersionUID = 7157319356146764960L;

  protected LearningNode(double[] classObservation) {
    super(classObservation);
  }

  /**
   * Method to process the instance for learning
   * 
   * @param inst
   *          The processed instance
   * @param proc
   *          The model aggregator processor where this learning node exists
   */
  abstract void learnFromInstance(Instance inst, ModelAggregatorProcessor proc);

  @Override
  protected boolean isLeaf() {
    return true;
  }

  @Override
  protected FoundNode filterInstanceToLeaf(Instance inst, SplitNode parent,
      int parentBranch) {
    return new FoundNode(this, parent, parentBranch);
  }
}
