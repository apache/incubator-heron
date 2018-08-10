package org.apache.samoa.learners.classifiers.rules.distributed;

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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.Processor;
import org.apache.samoa.instances.Instance;
import org.apache.samoa.instances.Instances;
import org.apache.samoa.learners.InstanceContentEvent;
import org.apache.samoa.learners.ResultContentEvent;
import org.apache.samoa.learners.classifiers.rules.common.ActiveRule;
import org.apache.samoa.learners.classifiers.rules.common.LearningRule;
import org.apache.samoa.learners.classifiers.rules.common.PassiveRule;
import org.apache.samoa.learners.classifiers.rules.common.Perceptron;
import org.apache.samoa.learners.classifiers.rules.common.RuleActiveRegressionNode;
import org.apache.samoa.moa.classifiers.rules.core.attributeclassobservers.FIMTDDNumericAttributeClassLimitObserver;
import org.apache.samoa.moa.classifiers.rules.core.voting.ErrorWeightedVote;
import org.apache.samoa.moa.classifiers.rules.core.voting.InverseErrorWeightedVote;
import org.apache.samoa.moa.classifiers.rules.core.voting.UniformWeightedVote;
import org.apache.samoa.topology.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Model Aggregator Processor (VAMR).
 * 
 * @author Anh Thu Vu
 * 
 */
public class AMRulesAggregatorProcessor implements Processor {

  /**
	 * 
	 */
  private static final long serialVersionUID = 6303385725332704251L;

  private static final Logger logger =
      LoggerFactory.getLogger(AMRulesAggregatorProcessor.class);

  private int processorId;

  // Rules & default rule
  protected transient List<PassiveRule> ruleSet;
  protected transient ActiveRule defaultRule;
  protected transient int ruleNumberID;
  protected transient double[] statistics;

  // SAMOA Stream
  private Stream statisticsStream;
  private Stream resultStream;

  // Options
  protected int pageHinckleyThreshold;
  protected double pageHinckleyAlpha;
  protected boolean driftDetection;
  protected int predictionFunction; // Adaptive=0 Perceptron=1 TargetMean=2
  protected boolean constantLearningRatioDecay;
  protected double learningRatio;

  protected double splitConfidence;
  protected double tieThreshold;
  protected int gracePeriod;

  protected boolean noAnomalyDetection;
  protected double multivariateAnomalyProbabilityThreshold;
  protected double univariateAnomalyprobabilityThreshold;
  protected int anomalyNumInstThreshold;

  protected boolean unorderedRules;

  protected FIMTDDNumericAttributeClassLimitObserver numericObserver;
  protected int voteType;

  /*
   * Constructor
   */
  public AMRulesAggregatorProcessor(Builder builder) {
    this.pageHinckleyThreshold = builder.pageHinckleyThreshold;
    this.pageHinckleyAlpha = builder.pageHinckleyAlpha;
    this.driftDetection = builder.driftDetection;
    this.predictionFunction = builder.predictionFunction;
    this.constantLearningRatioDecay = builder.constantLearningRatioDecay;
    this.learningRatio = builder.learningRatio;
    this.splitConfidence = builder.splitConfidence;
    this.tieThreshold = builder.tieThreshold;
    this.gracePeriod = builder.gracePeriod;

    this.noAnomalyDetection = builder.noAnomalyDetection;
    this.multivariateAnomalyProbabilityThreshold = builder.multivariateAnomalyProbabilityThreshold;
    this.univariateAnomalyprobabilityThreshold = builder.univariateAnomalyprobabilityThreshold;
    this.anomalyNumInstThreshold = builder.anomalyNumInstThreshold;
    this.unorderedRules = builder.unorderedRules;

    this.numericObserver = builder.numericObserver;
    this.voteType = builder.voteType;
  }

  /*
   * Process
   */
  @Override
  public boolean process(ContentEvent event) {
    if (event instanceof InstanceContentEvent) {
      InstanceContentEvent instanceEvent = (InstanceContentEvent) event;
      this.processInstanceEvent(instanceEvent);
    }
    else if (event instanceof PredicateContentEvent) {
      this.updateRuleSplitNode((PredicateContentEvent) event);
    }
    else if (event instanceof RuleContentEvent) {
      RuleContentEvent rce = (RuleContentEvent) event;
      if (rce.isRemoving()) {
        this.removeRule(rce.getRuleNumberID());
      }
    }

    return true;
  }

  // Merge predict and train so we only check for covering rules one time
  private void processInstanceEvent(InstanceContentEvent instanceEvent) {
    Instance instance = instanceEvent.getInstance();
    boolean predictionCovered = false;
    boolean trainingCovered = false;
    boolean continuePrediction = instanceEvent.isTesting();
    boolean continueTraining = instanceEvent.isTraining();

    ErrorWeightedVote errorWeightedVote = newErrorWeightedVote();
    Iterator<PassiveRule> ruleIterator = this.ruleSet.iterator();
    while (ruleIterator.hasNext()) {
      if (!continuePrediction && !continueTraining)
        break;

      PassiveRule rule = ruleIterator.next();

      if (rule.isCovering(instance) == true) {
        predictionCovered = true;

        if (continuePrediction) {
          double[] vote = rule.getPrediction(instance);
          double error = rule.getCurrentError();
          errorWeightedVote.addVote(vote, error);
          if (!this.unorderedRules)
            continuePrediction = false;
        }

        if (continueTraining) {
          if (!isAnomaly(instance, rule)) {
            trainingCovered = true;
            rule.updateStatistics(instance);
            // Send instance to statistics PIs
            sendInstanceToRule(instance, rule.getRuleNumberID());

            if (!this.unorderedRules)
              continueTraining = false;
          }
        }
      }
    }

    if (predictionCovered) {
      // Combined prediction
      ResultContentEvent rce = newResultContentEvent(errorWeightedVote.computeWeightedVote(), instanceEvent);
      resultStream.put(rce);
    }
    else if (instanceEvent.isTesting()) {
      // predict with default rule
      double[] vote = defaultRule.getPrediction(instance);
      ResultContentEvent rce = newResultContentEvent(vote, instanceEvent);
      resultStream.put(rce);
    }

    if (!trainingCovered && instanceEvent.isTraining()) {
      // train default rule with this instance
      defaultRule.updateStatistics(instance);
      if (defaultRule.getInstancesSeen() % this.gracePeriod == 0.0) {
        if (defaultRule.tryToExpand(this.splitConfidence, this.tieThreshold) == true) {
          ActiveRule newDefaultRule = newRule(defaultRule.getRuleNumberID(),
              (RuleActiveRegressionNode) defaultRule.getLearningNode(),
              ((RuleActiveRegressionNode) defaultRule.getLearningNode()).getStatisticsOtherBranchSplit()); // other branch
          defaultRule.split();
          defaultRule.setRuleNumberID(++ruleNumberID);
          this.ruleSet.add(new PassiveRule(this.defaultRule));
          // send to statistics PI
          sendAddRuleEvent(defaultRule.getRuleNumberID(), this.defaultRule);
          defaultRule = newDefaultRule;
        }
      }
    }
  }

  /**
   * Helper method to generate new ResultContentEvent based on an instance and its prediction result.
   * 
   * @param prediction
   *          The predicted class label from the decision tree model.
   * @param inEvent
   *          The associated instance content event
   * @return ResultContentEvent to be sent into Evaluator PI or other destination PI.
   */
  private ResultContentEvent newResultContentEvent(double[] prediction, InstanceContentEvent inEvent) {
    ResultContentEvent rce = new ResultContentEvent(inEvent.getInstanceIndex(), inEvent.getInstance(),
        inEvent.getClassId(), prediction, inEvent.isLastEvent());
    rce.setClassifierIndex(this.processorId);
    rce.setEvaluationIndex(inEvent.getEvaluationIndex());
    return rce;
  }

  public ErrorWeightedVote newErrorWeightedVote() {
    if (voteType == 1)
      return new UniformWeightedVote();
    return new InverseErrorWeightedVote();
  }

  /**
   * Method to verify if the instance is an anomaly.
   * 
   * @param instance
   * @param rule
   * @return
   */
  private boolean isAnomaly(Instance instance, LearningRule rule) {
    // AMRUles is equipped with anomaly detection. If on, compute the anomaly
    // value.
    boolean isAnomaly = false;
    if (this.noAnomalyDetection == false) {
      if (rule.getInstancesSeen() >= this.anomalyNumInstThreshold) {
        isAnomaly = rule.isAnomaly(instance,
            this.univariateAnomalyprobabilityThreshold,
            this.multivariateAnomalyProbabilityThreshold,
            this.anomalyNumInstThreshold);
      }
    }
    return isAnomaly;
  }

  /*
   * Create new rules
   */
  private ActiveRule newRule(int ID, RuleActiveRegressionNode node, double[] statistics) {
    ActiveRule r = newRule(ID);

    if (node != null)
    {
      if (node.getPerceptron() != null)
      {
        r.getLearningNode().setPerceptron(new Perceptron(node.getPerceptron()));
        r.getLearningNode().getPerceptron().setLearningRatio(this.learningRatio);
      }
      if (statistics == null)
      {
        double mean;
        if (node.getNodeStatistics().getValue(0) > 0) {
          mean = node.getNodeStatistics().getValue(1) / node.getNodeStatistics().getValue(0);
          r.getLearningNode().getTargetMean().reset(mean, 1);
        }
      }
    }
    if (statistics != null && ((RuleActiveRegressionNode) r.getLearningNode()).getTargetMean() != null)
    {
      double mean;
      if (statistics[0] > 0) {
        mean = statistics[1] / statistics[0];
        ((RuleActiveRegressionNode) r.getLearningNode()).getTargetMean().reset(mean, (long) statistics[0]);
      }
    }
    return r;
  }

  private ActiveRule newRule(int ID) {
    ActiveRule r = new ActiveRule.Builder().
        threshold(this.pageHinckleyThreshold).
        alpha(this.pageHinckleyAlpha).
        changeDetection(this.driftDetection).
        predictionFunction(this.predictionFunction).
        statistics(new double[3]).
        learningRatio(this.learningRatio).
        numericObserver(numericObserver).
        id(ID).build();
    return r;
  }

  /*
   * Add predicate/RuleSplitNode for a rule
   */
  private void updateRuleSplitNode(PredicateContentEvent pce) {
    int ruleID = pce.getRuleNumberID();
    for (PassiveRule rule : ruleSet) {
      if (rule.getRuleNumberID() == ruleID) {
        if (pce.getRuleSplitNode() != null)
          rule.nodeListAdd(pce.getRuleSplitNode());
        if (pce.getLearningNode() != null)
          rule.setLearningNode(pce.getLearningNode());
      }
    }
  }

  /*
   * Remove rule
   */
  private void removeRule(int ruleID) {
    for (PassiveRule rule : ruleSet) {
      if (rule.getRuleNumberID() == ruleID) {
        ruleSet.remove(rule);
        break;
      }
    }
  }

  @Override
  public void onCreate(int id) {
    this.processorId = id;
    this.statistics = new double[] { 0.0, 0, 0 };
    this.ruleNumberID = 0;
    this.defaultRule = newRule(++this.ruleNumberID);

    this.ruleSet = new LinkedList<PassiveRule>();
  }

  /*
   * Clone processor
   */
  @Override
  public Processor newProcessor(Processor p) {
    AMRulesAggregatorProcessor oldProcessor = (AMRulesAggregatorProcessor) p;
    Builder builder = new Builder(oldProcessor);
    AMRulesAggregatorProcessor newProcessor = builder.build();
    newProcessor.resultStream = oldProcessor.resultStream;
    newProcessor.statisticsStream = oldProcessor.statisticsStream;
    return newProcessor;
  }

  /*
   * Send events
   */
  private void sendInstanceToRule(Instance instance, int ruleID) {
    AssignmentContentEvent ace = new AssignmentContentEvent(ruleID, instance);
    this.statisticsStream.put(ace);
  }

  private void sendAddRuleEvent(int ruleID, ActiveRule rule) {
    RuleContentEvent rce = new RuleContentEvent(ruleID, rule, false);
    this.statisticsStream.put(rce);
  }

  /*
   * Output streams
   */
  public void setStatisticsStream(Stream statisticsStream) {
    this.statisticsStream = statisticsStream;
  }

  public Stream getStatisticsStream() {
    return this.statisticsStream;
  }

  public void setResultStream(Stream resultStream) {
    this.resultStream = resultStream;
  }

  public Stream getResultStream() {
    return this.resultStream;
  }

  /*
   * Others
   */
  public boolean isRandomizable() {
    return true;
  }

  /*
   * Builder
   */
  public static class Builder {
    private int pageHinckleyThreshold;
    private double pageHinckleyAlpha;
    private boolean driftDetection;
    private int predictionFunction; // Adaptive=0 Perceptron=1 TargetMean=2
    private boolean constantLearningRatioDecay;
    private double learningRatio;
    private double splitConfidence;
    private double tieThreshold;
    private int gracePeriod;

    private boolean noAnomalyDetection;
    private double multivariateAnomalyProbabilityThreshold;
    private double univariateAnomalyprobabilityThreshold;
    private int anomalyNumInstThreshold;

    private boolean unorderedRules;

    private FIMTDDNumericAttributeClassLimitObserver numericObserver;
    private int voteType;

    private Instances dataset;

    public Builder(Instances dataset) {
      this.dataset = dataset;
    }

    public Builder(AMRulesAggregatorProcessor processor) {
      this.pageHinckleyThreshold = processor.pageHinckleyThreshold;
      this.pageHinckleyAlpha = processor.pageHinckleyAlpha;
      this.driftDetection = processor.driftDetection;
      this.predictionFunction = processor.predictionFunction;
      this.constantLearningRatioDecay = processor.constantLearningRatioDecay;
      this.learningRatio = processor.learningRatio;
      this.splitConfidence = processor.splitConfidence;
      this.tieThreshold = processor.tieThreshold;
      this.gracePeriod = processor.gracePeriod;

      this.noAnomalyDetection = processor.noAnomalyDetection;
      this.multivariateAnomalyProbabilityThreshold = processor.multivariateAnomalyProbabilityThreshold;
      this.univariateAnomalyprobabilityThreshold = processor.univariateAnomalyprobabilityThreshold;
      this.anomalyNumInstThreshold = processor.anomalyNumInstThreshold;
      this.unorderedRules = processor.unorderedRules;

      this.numericObserver = processor.numericObserver;
      this.voteType = processor.voteType;
    }

    public Builder threshold(int threshold) {
      this.pageHinckleyThreshold = threshold;
      return this;
    }

    public Builder alpha(double alpha) {
      this.pageHinckleyAlpha = alpha;
      return this;
    }

    public Builder changeDetection(boolean changeDetection) {
      this.driftDetection = changeDetection;
      return this;
    }

    public Builder predictionFunction(int predictionFunction) {
      this.predictionFunction = predictionFunction;
      return this;
    }

    public Builder constantLearningRatioDecay(boolean constantDecay) {
      this.constantLearningRatioDecay = constantDecay;
      return this;
    }

    public Builder learningRatio(double learningRatio) {
      this.learningRatio = learningRatio;
      return this;
    }

    public Builder splitConfidence(double splitConfidence) {
      this.splitConfidence = splitConfidence;
      return this;
    }

    public Builder tieThreshold(double tieThreshold) {
      this.tieThreshold = tieThreshold;
      return this;
    }

    public Builder gracePeriod(int gracePeriod) {
      this.gracePeriod = gracePeriod;
      return this;
    }

    public Builder noAnomalyDetection(boolean noAnomalyDetection) {
      this.noAnomalyDetection = noAnomalyDetection;
      return this;
    }

    public Builder multivariateAnomalyProbabilityThreshold(double mAnomalyThreshold) {
      this.multivariateAnomalyProbabilityThreshold = mAnomalyThreshold;
      return this;
    }

    public Builder univariateAnomalyProbabilityThreshold(double uAnomalyThreshold) {
      this.univariateAnomalyprobabilityThreshold = uAnomalyThreshold;
      return this;
    }

    public Builder anomalyNumberOfInstancesThreshold(int anomalyNumInstThreshold) {
      this.anomalyNumInstThreshold = anomalyNumInstThreshold;
      return this;
    }

    public Builder unorderedRules(boolean unorderedRules) {
      this.unorderedRules = unorderedRules;
      return this;
    }

    public Builder numericObserver(FIMTDDNumericAttributeClassLimitObserver numericObserver) {
      this.numericObserver = numericObserver;
      return this;
    }

    public Builder voteType(int voteType) {
      this.voteType = voteType;
      return this;
    }

    public AMRulesAggregatorProcessor build() {
      return new AMRulesAggregatorProcessor(this);
    }
  }

}
