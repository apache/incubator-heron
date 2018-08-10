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
import org.apache.samoa.moa.classifiers.core.conditionaltests.InstanceConditionalTest;
import org.apache.samoa.moa.core.AutoExpandVector;

/**
 * SplitNode represents the node that contains one or more questions in the decision tree model, in order to route the
 * instances into the correct leaf.
 * 
 * @author Arinto Murdopo
 * 
 */
public class SplitNode extends Node {

  private static final long serialVersionUID = -7380795529928485792L;

  private final AutoExpandVector<Node> children;
  protected final InstanceConditionalTest splitTest;

  public SplitNode(InstanceConditionalTest splitTest,
      double[] classObservation) {
    super(classObservation);
    this.children = new AutoExpandVector<>();
    this.splitTest = splitTest;
  }

  @Override
  FoundNode filterInstanceToLeaf(Instance inst, SplitNode parent, int parentBranch) {
    int childIndex = instanceChildIndex(inst);
    if (childIndex >= 0) {
      Node child = getChild(childIndex);
      if (child != null) {
        return child.filterInstanceToLeaf(inst, this, childIndex);
      }
      return new FoundNode(null, this, childIndex);
    }
    return new FoundNode(this, parent, parentBranch);
  }

  @Override
  boolean isLeaf() {
    return false;
  }

  @Override
  double[] getClassVotes(Instance inst, ModelAggregatorProcessor vht) {
    return this.observedClassDistribution.getArrayCopy();
  }

  /**
   * Method to return the number of children of this split node
   * 
   * @return number of children
   */
  int numChildren() {
    return this.children.size();
  }

  /**
   * Method to set the children in a specific index of the SplitNode with the appropriate child
   * 
   * @param index
   *          Index of the child in the SplitNode
   * @param child
   *          The child node
   */
  void setChild(int index, Node child) {
    if ((this.splitTest.maxBranches() >= 0)
        && (index >= this.splitTest.maxBranches())) {
      throw new IndexOutOfBoundsException();
    }
    this.children.set(index, child);
  }

  /**
   * Method to get the child node given the index
   * 
   * @param index
   *          The child node index
   * @return The child node in the given index
   */
  Node getChild(int index) {
    return this.children.get(index);
  }

  /**
   * Method to route the instance using this split node
   * 
   * @param inst
   *          The routed instance
   * @return The index of the branch where the instance is routed
   */
  int instanceChildIndex(Instance inst) {
    return this.splitTest.branchForInstance(inst);
  }
}
