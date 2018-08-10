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
import org.apache.samoa.moa.core.StringUtils;

/**
 * Rule with LearningNode (statistical data).
 * 
 * @author Anh Thu Vu
 * 
 */
public abstract class LearningRule extends Rule {

  /**
	 * 
	 */
  private static final long serialVersionUID = 1L;

  /*
   * Constructor
   */
  public LearningRule() {
    super();
  }

  /*
   * LearningNode
   */
  public abstract RuleRegressionNode getLearningNode();

  public abstract void setLearningNode(RuleRegressionNode learningNode);

  /*
   * No. of instances seen
   */
  public long getInstancesSeen() {
    return this.getLearningNode().getInstancesSeen();
  }

  /*
   * Error and change detection
   */
  public double computeError(Instance instance) {
    return this.getLearningNode().computeError(instance);
  }

  /*
   * Prediction
   */
  public double[] getPrediction(Instance instance, int mode) {
    return this.getLearningNode().getPrediction(instance, mode);
  }

  public double[] getPrediction(Instance instance) {
    return this.getLearningNode().getPrediction(instance);
  }

  public double getCurrentError() {
    return this.getLearningNode().getCurrentError();
  }

  /*
   * Anomaly detection
   */
  public boolean isAnomaly(Instance instance,
      double uniVariateAnomalyProbabilityThreshold,
      double multiVariateAnomalyProbabilityThreshold,
      int numberOfInstanceesForAnomaly) {
    return this.getLearningNode().isAnomaly(instance, uniVariateAnomalyProbabilityThreshold,
        multiVariateAnomalyProbabilityThreshold,
        numberOfInstanceesForAnomaly);
  }

  /*
   * Update
   */
  public void updateStatistics(Instance instance) {
    this.getLearningNode().updateStatistics(instance);
  }

  public String printRule() {
    StringBuilder out = new StringBuilder();
    int indent = 1;
    StringUtils.appendIndented(out, indent, "Rule Nr." + this.ruleNumberID + " Instances seen:"
        + this.getLearningNode().getInstancesSeen() + "\n"); // AC
    for (RuleSplitNode node : nodeList) {
      StringUtils.appendIndented(out, indent, node.getSplitTest().toString());
      StringUtils.appendIndented(out, indent, " ");
      StringUtils.appendIndented(out, indent, node.toString());
    }
    DoubleVector pred = new DoubleVector(this.getLearningNode().getSimplePrediction());
    StringUtils.appendIndented(out, 0, " --> y: " + pred.toString());
    StringUtils.appendNewline(out);

    if (getLearningNode().perceptron != null) {
      ((RuleActiveRegressionNode) this.getLearningNode()).perceptron.getModelDescription(out, 0);
      StringUtils.appendNewline(out);
    }
    return (out.toString());
  }
}
