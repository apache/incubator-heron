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

import org.apache.samoa.core.DoubleVector;
import org.apache.samoa.instances.Instance;

/**
 * Abstract class that represents a node in the tree model.
 * 
 * @author Arinto Murdopo
 * 
 */
abstract class Node implements java.io.Serializable {

  private static final long serialVersionUID = 4008521239214180548L;

  protected final DoubleVector observedClassDistribution;

  /**
   * Method to route/filter an instance into its corresponding leaf. This method will be invoked recursively.
   * 
   * @param inst
   *          Instance to be routed
   * @param parent
   *          Parent of the current node
   * @param parentBranch
   *          The index of the current node in the parent
   * @return FoundNode which is the data structure to represent the resulting leaf.
   */
  abstract FoundNode filterInstanceToLeaf(Instance inst, SplitNode parent, int parentBranch);

  /**
   * Method to return the predicted class of the instance based on the statistic inside the node.
   * 
   * @param inst
   *          To-be-predicted instance
   * @param map
   *          ModelAggregatorProcessor
   * @return The prediction result in the form of class distribution
   */
  abstract double[] getClassVotes(Instance inst, ModelAggregatorProcessor map);

  /**
   * Method to check whether the node is a leaf node or not.
   * 
   * @return Boolean flag to indicate whether the node is a leaf or not
   */
  abstract boolean isLeaf();

  /**
   * Constructor of the tree node
   * 
   * @param classObservation
   *          distribution of the observed classes.
   */
  protected Node(double[] classObservation) {
    this.observedClassDistribution = new DoubleVector(classObservation);
  }

  /**
   * Getter method for the class distribution
   * 
   * @return Observed class distribution
   */
  protected double[] getObservedClassDistribution() {
    return this.observedClassDistribution.getArrayCopy();
  }

  /**
   * A method to check whether the class distribution only consists of one class or not.
   * 
   * @return Flag whether class distribution is pure or not.
   */
  protected boolean observedClassDistributionIsPure() {
    return (observedClassDistribution.numNonZeroEntries() < 2);
  }

  protected void describeSubtree(ModelAggregatorProcessor modelAggrProc, StringBuilder out, int indent) {
    // TODO: implement method to gracefully define the tree
  }

  // TODO: calculate promise for limiting the model based on the memory size
  // double calculatePromise();
}
