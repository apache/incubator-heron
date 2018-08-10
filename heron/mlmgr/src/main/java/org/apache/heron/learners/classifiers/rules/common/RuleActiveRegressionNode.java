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

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.samoa.instances.Instance;
import org.apache.samoa.moa.classifiers.core.AttributeSplitSuggestion;
import org.apache.samoa.moa.classifiers.core.attributeclassobservers.AttributeClassObserver;
import org.apache.samoa.moa.classifiers.core.attributeclassobservers.FIMTDDNumericAttributeClassObserver;
import org.apache.samoa.moa.classifiers.core.splitcriteria.SplitCriterion;
import org.apache.samoa.moa.classifiers.rules.core.attributeclassobservers.FIMTDDNumericAttributeClassLimitObserver;
import org.apache.samoa.moa.classifiers.rules.core.splitcriteria.SDRSplitCriterionAMRules;
import org.apache.samoa.moa.classifiers.rules.driftdetection.PageHinkleyFading;
import org.apache.samoa.moa.classifiers.rules.driftdetection.PageHinkleyTest;
import org.apache.samoa.moa.core.AutoExpandVector;
import org.apache.samoa.moa.core.DoubleVector;

/**
 * LearningNode for regression rule that updates both statistics for expanding rule and computing predictions.
 * 
 * @author Anh Thu Vu
 * 
 */
public class RuleActiveRegressionNode extends RuleRegressionNode implements RuleActiveLearningNode {

  /**
	 * 
	 */
  private static final long serialVersionUID = 519854943188168546L;

  protected int splitIndex = 0;

  protected PageHinkleyTest pageHinckleyTest;
  protected boolean changeDetection;

  protected double[] statisticsNewRuleActiveLearningNode = null;
  protected double[] statisticsBranchSplit = null;
  protected double[] statisticsOtherBranchSplit;

  protected AttributeSplitSuggestion bestSuggestion = null;

  protected AutoExpandVector<AttributeClassObserver> attributeObservers = new AutoExpandVector<>();
  private FIMTDDNumericAttributeClassLimitObserver numericObserver;

  /*
   * Simple setters & getters
   */
  public int getSplitIndex() {
    return splitIndex;
  }

  public void setSplitIndex(int splitIndex) {
    this.splitIndex = splitIndex;
  }

  public double[] getStatisticsOtherBranchSplit() {
    return statisticsOtherBranchSplit;
  }

  public void setStatisticsOtherBranchSplit(double[] statisticsOtherBranchSplit) {
    this.statisticsOtherBranchSplit = statisticsOtherBranchSplit;
  }

  public double[] getStatisticsBranchSplit() {
    return statisticsBranchSplit;
  }

  public void setStatisticsBranchSplit(double[] statisticsBranchSplit) {
    this.statisticsBranchSplit = statisticsBranchSplit;
  }

  public double[] getStatisticsNewRuleActiveLearningNode() {
    return statisticsNewRuleActiveLearningNode;
  }

  public void setStatisticsNewRuleActiveLearningNode(
      double[] statisticsNewRuleActiveLearningNode) {
    this.statisticsNewRuleActiveLearningNode = statisticsNewRuleActiveLearningNode;
  }

  public AttributeSplitSuggestion getBestSuggestion() {
    return bestSuggestion;
  }

  public void setBestSuggestion(AttributeSplitSuggestion bestSuggestion) {
    this.bestSuggestion = bestSuggestion;
  }

  /*
   * Constructor with builder
   */
  public RuleActiveRegressionNode() {
    super();
  }

  public RuleActiveRegressionNode(ActiveRule.Builder builder) {
    super(builder.statistics);
    this.changeDetection = builder.changeDetection;
    if (!builder.changeDetection) {
      this.pageHinckleyTest = new PageHinkleyFading(builder.threshold, builder.alpha);
    }
    this.predictionFunction = builder.predictionFunction;
    this.learningRatio = builder.learningRatio;
    this.ruleNumberID = builder.id;
    this.numericObserver = builder.numericObserver;

    this.perceptron = new Perceptron();
    this.perceptron.prepareForUse();
    this.perceptron.originalLearningRatio = builder.learningRatio;
    this.perceptron.constantLearningRatioDecay = builder.constantLearningRatioDecay;

    if (this.predictionFunction != 1)
    {
      this.targetMean = new TargetMean();
      if (builder.statistics[0] > 0)
        this.targetMean.reset(builder.statistics[1] / builder.statistics[0], (long) builder.statistics[0]);
    }
    this.predictionFunction = builder.predictionFunction;
    if (builder.statistics != null)
      this.nodeStatistics = new DoubleVector(builder.statistics);
  }

  /*
   * Update with input instance
   */
  public boolean updatePageHinckleyTest(double error) {
    boolean changeDetected = false;
    if (!this.changeDetection) {
      changeDetected = pageHinckleyTest.update(error);
    }
    return changeDetected;
  }

  public boolean updateChangeDetection(double error) {
    return !changeDetection && pageHinckleyTest.update(error);
  }

  @Override
  public void updateStatistics(Instance inst) {
    // Update the statistics for this node
    // number of instances passing through the node
    nodeStatistics.addToValue(0, 1);
    // sum of y values
    nodeStatistics.addToValue(1, inst.classValue());
    // sum of squared y values
    nodeStatistics.addToValue(2, inst.classValue() * inst.classValue());

    for (int i = 0; i < inst.numAttributes() - 1; i++) {
      int instAttIndex = modelAttIndexToInstanceAttIndex(i, inst);

      AttributeClassObserver obs = this.attributeObservers.get(i);
      if (obs == null) {
        // At this stage all nominal attributes are ignored
        if (inst.attribute(instAttIndex).isNumeric()) // instAttIndex
        {
          obs = newNumericClassObserver();
          this.attributeObservers.set(i, obs);
        }
      }
      if (obs != null) {
        ((FIMTDDNumericAttributeClassObserver) obs).observeAttributeClass(inst.value(instAttIndex), inst.classValue(),
            inst.weight());
      }
    }

    this.perceptron.trainOnInstance(inst);
    if (this.predictionFunction != 1) { // Train target mean if prediction function is not Perceptron
      this.targetMean.trainOnInstance(inst);
    }
  }

  protected AttributeClassObserver newNumericClassObserver() {
    // return new FIMTDDNumericAttributeClassObserver();
    // return new FIMTDDNumericAttributeClassLimitObserver();
    // return
    // (AttributeClassObserver)((AttributeClassObserver)this.numericObserverOption.getPreMaterializedObject()).copy();
    FIMTDDNumericAttributeClassLimitObserver newObserver = new FIMTDDNumericAttributeClassLimitObserver();
    newObserver.setMaxNodes(numericObserver.getMaxNodes());
    return newObserver;
  }

  /*
   * Init after being split from oldLearningNode
   */
  public void initialize(RuleRegressionNode oldLearningNode) {
    if (oldLearningNode.perceptron != null)
    {
      this.perceptron = new Perceptron(oldLearningNode.perceptron);
      this.perceptron.resetError();
      this.perceptron.setLearningRatio(oldLearningNode.learningRatio);
    }

    if (oldLearningNode.targetMean != null)
    {
      this.targetMean = new TargetMean(oldLearningNode.targetMean);
      this.targetMean.resetError();
    }
    // reset statistics
    this.nodeStatistics.setValue(0, 0);
    this.nodeStatistics.setValue(1, 0);
    this.nodeStatistics.setValue(2, 0);
  }

  /*
   * Expand
   */
  @Override
  public boolean tryToExpand(double splitConfidence, double tieThreshold) {

    // splitConfidence. Hoeffding Bound test parameter.
    // tieThreshold. Hoeffding Bound test parameter.
    SplitCriterion splitCriterion = new SDRSplitCriterionAMRules();
    // SplitCriterion splitCriterion = new SDRSplitCriterionAMRulesNode();//JD
    // for assessing only best branch

    // Using this criterion, find the best split per attribute and rank the
    // results
    AttributeSplitSuggestion[] bestSplitSuggestions = this.getBestSplitSuggestions(splitCriterion);
    Arrays.sort(bestSplitSuggestions);
    // Declare a variable to determine if any of the splits should be performed
    boolean shouldSplit = false;

    // If only one split was returned, use it
    if (bestSplitSuggestions.length < 2) {
      shouldSplit = ((bestSplitSuggestions.length > 0) && (bestSplitSuggestions[0].merit > 0));
      bestSuggestion = bestSplitSuggestions[bestSplitSuggestions.length - 1];
    } // Otherwise, consider which of the splits proposed may be worth trying
    else {
      // Determine the hoeffding bound value, used to select how many instances
      // should be used to make a test decision
      // to feel reasonably confident that the test chosen by this sample is the
      // same as what would be chosen using infinite examples
      double hoeffdingBound = computeHoeffdingBound(1, splitConfidence, getInstancesSeen());
      // Determine the top two ranked splitting suggestions
      bestSuggestion = bestSplitSuggestions[bestSplitSuggestions.length - 1];
      AttributeSplitSuggestion secondBestSuggestion = bestSplitSuggestions[bestSplitSuggestions.length - 2];

      // If the upper bound of the sample mean for the ratio of SDR(best
      // suggestion) to SDR(second best suggestion),
      // as determined using the hoeffding bound, is less than 1, then the true
      // mean is also less than 1, and thus at this
      // particular moment of observation the bestSuggestion is indeed the best
      // split option with confidence 1-delta, and
      // splitting should occur.
      // Alternatively, if two or more splits are very similar or identical in
      // terms of their splits, then a threshold limit
      // (default 0.05) is applied to the hoeffding bound; if the hoeffding
      // bound is smaller than this limit then the two
      // competing attributes are equally good, and the split will be made on
      // the one with the higher SDR value.

      if (bestSuggestion.merit > 0) {
        if ((((secondBestSuggestion.merit / bestSuggestion.merit) + hoeffdingBound) < 1)
            || (hoeffdingBound < tieThreshold)) {
          shouldSplit = true;
        }
      }
    }

    if (shouldSplit) {
      AttributeSplitSuggestion splitDecision = bestSplitSuggestions[bestSplitSuggestions.length - 1];
      double minValue = Double.MAX_VALUE;
      double[] branchMerits = SDRSplitCriterionAMRules
          .computeBranchSplitMerits(bestSuggestion.resultingClassDistributions);

      for (int i = 0; i < bestSuggestion.numSplits(); i++) {
        double value = branchMerits[i];
        if (value < minValue) {
          minValue = value;
          splitIndex = i;
          statisticsNewRuleActiveLearningNode = bestSuggestion.resultingClassDistributionFromSplit(i);
        }
      }
      statisticsBranchSplit = splitDecision.resultingClassDistributionFromSplit(splitIndex);
      statisticsOtherBranchSplit = bestSuggestion.resultingClassDistributionFromSplit(splitIndex == 0 ? 1 : 0);

    }
    return shouldSplit;
  }

  public AutoExpandVector<AttributeClassObserver> getAttributeObservers() {
    return this.attributeObservers;
  }

  public AttributeSplitSuggestion[] getBestSplitSuggestions(SplitCriterion criterion) {

    List<AttributeSplitSuggestion> bestSuggestions = new LinkedList<AttributeSplitSuggestion>();

    // Set the nodeStatistics up as the preSplitDistribution, rather than the
    // observedClassDistribution
    double[] nodeSplitDist = this.nodeStatistics.getArrayCopy();
    for (int i = 0; i < this.attributeObservers.size(); i++) {
      AttributeClassObserver obs = this.attributeObservers.get(i);
      if (obs != null) {

        // AT THIS STAGE NON-NUMERIC ATTRIBUTES ARE IGNORED
        AttributeSplitSuggestion bestSuggestion = null;
        if (obs instanceof FIMTDDNumericAttributeClassObserver) {
          bestSuggestion = obs.getBestEvaluatedSplitSuggestion(criterion, nodeSplitDist, i, true);
        }

        if (bestSuggestion != null) {
          bestSuggestions.add(bestSuggestion);
        }
      }
    }
    return bestSuggestions.toArray(new AttributeSplitSuggestion[bestSuggestions.size()]);
  }

}
