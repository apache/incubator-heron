package org.apache.samoa.learners.classifiers.rules.common;

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
import org.apache.samoa.moa.core.DoubleVector;

/**
 * LearningNode for regression rule that does not update statistics for expanding rule. It only updates statistics for
 * computing predictions.
 * 
 * @author Anh Thu Vu
 * 
 */
public class RulePassiveRegressionNode extends RuleRegressionNode implements RulePassiveLearningNode {

  /**
	 * 
	 */
  private static final long serialVersionUID = 3720878438856489690L;

  public RulePassiveRegressionNode(double[] statistics) {
    super(statistics);
  }

  public RulePassiveRegressionNode() {
    super();
  }

  public RulePassiveRegressionNode(RuleRegressionNode activeLearningNode) {
    this.predictionFunction = activeLearningNode.predictionFunction;
    this.ruleNumberID = activeLearningNode.ruleNumberID;
    this.nodeStatistics = new DoubleVector(activeLearningNode.nodeStatistics);
    this.learningRatio = activeLearningNode.learningRatio;
    this.perceptron = new Perceptron(activeLearningNode.perceptron, true);
    this.targetMean = new TargetMean(activeLearningNode.targetMean);
  }

  /*
   * Update with input instance
   */
  @Override
  public void updateStatistics(Instance inst) {
    // Update the statistics for this node
    // number of instances passing through the node
    nodeStatistics.addToValue(0, 1);
    // sum of y values
    nodeStatistics.addToValue(1, inst.classValue());
    // sum of squared y values
    nodeStatistics.addToValue(2, inst.classValue() * inst.classValue());

    this.perceptron.trainOnInstance(inst);
    if (this.predictionFunction != 1) { // Train target mean if prediction function is not Perceptron
      this.targetMean.trainOnInstance(inst);
    }
  }
}
