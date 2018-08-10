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

import java.io.Serializable;

import org.apache.samoa.moa.classifiers.core.conditionaltests.InstanceConditionalTest;
import org.apache.samoa.moa.classifiers.core.conditionaltests.NumericAttributeBinaryTest;
import org.apache.samoa.moa.classifiers.rules.core.attributeclassobservers.FIMTDDNumericAttributeClassLimitObserver;
import org.apache.samoa.moa.classifiers.rules.core.conditionaltests.NumericAttributeBinaryRulePredicate;

/**
 * ActiveRule is a LearningRule that actively update its LearningNode with incoming instances.
 * 
 * @author Anh Thu Vu
 * 
 */

public class ActiveRule extends LearningRule {

  private static final long serialVersionUID = 1L;

  private double[] statisticsOtherBranchSplit;

  private Builder builder;

  private RuleActiveRegressionNode learningNode;

  private RuleSplitNode lastUpdatedRuleSplitNode;

  /*
   * Constructor with Builder
   */
  public ActiveRule() {
    super();
    this.builder = null;
    this.learningNode = null;
    this.ruleNumberID = 0;
  }

  public ActiveRule(Builder builder) {
    super();
    this.setBuilder(builder);
    this.learningNode = newRuleActiveLearningNode(builder);
    // JD - use builder ID to set ruleNumberID
    this.ruleNumberID = builder.id;
  }

  private RuleActiveRegressionNode newRuleActiveLearningNode(Builder builder) {
    return new RuleActiveRegressionNode(builder);
  }

  /*
   * Setters & getters
   */
  public Builder getBuilder() {
    return builder;
  }

  public void setBuilder(Builder builder) {
    this.builder = builder;
  }

  @Override
  public RuleRegressionNode getLearningNode() {
    return this.learningNode;
  }

  @Override
  public void setLearningNode(RuleRegressionNode learningNode) {
    this.learningNode = (RuleActiveRegressionNode) learningNode;
  }

  public double[] statisticsOtherBranchSplit() {
    return this.statisticsOtherBranchSplit;
  }

  public RuleSplitNode getLastUpdatedRuleSplitNode() {
    return this.lastUpdatedRuleSplitNode;
  }

  /*
   * Builder
   */
  public static class Builder implements Serializable {

    private static final long serialVersionUID = 1712887264918475622L;
    protected boolean changeDetection;
    protected boolean usePerceptron;
    protected double threshold;
    protected double alpha;
    protected int predictionFunction;
    protected boolean constantLearningRatioDecay;
    protected double learningRatio;

    protected double[] statistics;

    protected FIMTDDNumericAttributeClassLimitObserver numericObserver;

    protected double lastTargetMean;

    public int id;

    public Builder() {
    }

    public Builder changeDetection(boolean changeDetection) {
      this.changeDetection = changeDetection;
      return this;
    }

    public Builder threshold(double threshold) {
      this.threshold = threshold;
      return this;
    }

    public Builder alpha(double alpha) {
      this.alpha = alpha;
      return this;
    }

    public Builder predictionFunction(int predictionFunction) {
      this.predictionFunction = predictionFunction;
      return this;
    }

    public Builder statistics(double[] statistics) {
      this.statistics = statistics;
      return this;
    }

    public Builder constantLearningRatioDecay(boolean constantLearningRatioDecay) {
      this.constantLearningRatioDecay = constantLearningRatioDecay;
      return this;
    }

    public Builder learningRatio(double learningRatio) {
      this.learningRatio = learningRatio;
      return this;
    }

    public Builder numericObserver(FIMTDDNumericAttributeClassLimitObserver numericObserver) {
      this.numericObserver = numericObserver;
      return this;
    }

    public Builder id(int id) {
      this.id = id;
      return this;
    }

    public ActiveRule build() {
      return new ActiveRule(this);
    }

  }

  /**
   * Try to Expand method.
   * 
   * @param splitConfidence
   * @param tieThreshold
   * @return
   */
  public boolean tryToExpand(double splitConfidence, double tieThreshold) {

    boolean shouldSplit = this.learningNode.tryToExpand(splitConfidence, tieThreshold);
    return shouldSplit;

  }

  // JD: Only call after tryToExpand returning true
  public void split()
  {
    // this.statisticsOtherBranchSplit =
    // this.learningNode.getStatisticsOtherBranchSplit();
    // create a split node,
    int splitIndex = this.learningNode.getSplitIndex();
    InstanceConditionalTest st = this.learningNode.getBestSuggestion().splitTest;
    if (st instanceof NumericAttributeBinaryTest) {
      NumericAttributeBinaryTest splitTest = (NumericAttributeBinaryTest) st;
      NumericAttributeBinaryRulePredicate predicate = new NumericAttributeBinaryRulePredicate(
          splitTest.getAttsTestDependsOn()[0], splitTest.getSplitValue(),
          splitIndex + 1);
      lastUpdatedRuleSplitNode = new RuleSplitNode(predicate, this.learningNode.getStatisticsBranchSplit());
      if (this.nodeListAdd(lastUpdatedRuleSplitNode)) {
        // create a new learning node
        RuleActiveRegressionNode newLearningNode = newRuleActiveLearningNode(this.getBuilder().statistics(
            this.learningNode.getStatisticsNewRuleActiveLearningNode()));
        newLearningNode.initialize(this.learningNode);
        this.learningNode = newLearningNode;
      }
    }
    else
      throw new UnsupportedOperationException("AMRules (currently) only supports numerical attributes.");
  }

  // protected void debug(String string, int level) {
  // if (this.amRules.VerbosityOption.getValue()>=level) {
  // System.out.println(string);
  // }
  // }

  /**
   * MOA GUI output
   */
  @Override
  public void getDescription(StringBuilder sb, int indent) {
  }
}
