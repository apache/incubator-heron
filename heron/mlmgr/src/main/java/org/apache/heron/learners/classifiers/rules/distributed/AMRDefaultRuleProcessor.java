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

import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.Processor;
import org.apache.samoa.instances.Instance;
import org.apache.samoa.instances.Instances;
import org.apache.samoa.learners.InstanceContentEvent;
import org.apache.samoa.learners.ResultContentEvent;
import org.apache.samoa.learners.classifiers.rules.common.ActiveRule;
import org.apache.samoa.learners.classifiers.rules.common.Perceptron;
import org.apache.samoa.learners.classifiers.rules.common.RuleActiveRegressionNode;
import org.apache.samoa.moa.classifiers.rules.core.attributeclassobservers.FIMTDDNumericAttributeClassLimitObserver;
import org.apache.samoa.topology.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default Rule Learner Processor (HAMR).
 * 
 * @author Anh Thu Vu
 * 
 */
public class AMRDefaultRuleProcessor implements Processor {

  /**
	 * 
	 */
  private static final long serialVersionUID = 23702084591044447L;

  private static final Logger logger =
      LoggerFactory.getLogger(AMRDefaultRuleProcessor.class);

  private int processorId;

  // Default rule
  protected transient ActiveRule defaultRule;
  protected transient int ruleNumberID;
  protected transient double[] statistics;

  // SAMOA Stream
  private Stream ruleStream;
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

  protected FIMTDDNumericAttributeClassLimitObserver numericObserver;

  /*
   * Constructor
   */
  public AMRDefaultRuleProcessor(Builder builder) {
    this.pageHinckleyThreshold = builder.pageHinckleyThreshold;
    this.pageHinckleyAlpha = builder.pageHinckleyAlpha;
    this.driftDetection = builder.driftDetection;
    this.predictionFunction = builder.predictionFunction;
    this.constantLearningRatioDecay = builder.constantLearningRatioDecay;
    this.learningRatio = builder.learningRatio;
    this.splitConfidence = builder.splitConfidence;
    this.tieThreshold = builder.tieThreshold;
    this.gracePeriod = builder.gracePeriod;

    this.numericObserver = builder.numericObserver;
  }

  @Override
  public boolean process(ContentEvent event) {
    InstanceContentEvent instanceEvent = (InstanceContentEvent) event;
    // predict
    if (instanceEvent.isTesting()) {
      this.predictOnInstance(instanceEvent);
    }

    // train
    if (instanceEvent.isTraining()) {
      this.trainOnInstance(instanceEvent);
    }

    return false;
  }

  /*
   * Prediction
   */
  private void predictOnInstance(InstanceContentEvent instanceEvent) {
    double[] vote = defaultRule.getPrediction(instanceEvent.getInstance());
    ResultContentEvent rce = newResultContentEvent(vote, instanceEvent);
    resultStream.put(rce);
  }

  private ResultContentEvent newResultContentEvent(double[] prediction, InstanceContentEvent inEvent) {
    ResultContentEvent rce = new ResultContentEvent(inEvent.getInstanceIndex(), inEvent.getInstance(),
        inEvent.getClassId(), prediction, inEvent.isLastEvent());
    rce.setClassifierIndex(this.processorId);
    rce.setEvaluationIndex(inEvent.getEvaluationIndex());
    return rce;
  }

  /*
   * Training
   */
  private void trainOnInstance(InstanceContentEvent instanceEvent) {
    this.trainOnInstanceImpl(instanceEvent.getInstance());
  }

  public void trainOnInstanceImpl(Instance instance) {
    defaultRule.updateStatistics(instance);
    if (defaultRule.getInstancesSeen() % this.gracePeriod == 0.0) {
      if (defaultRule.tryToExpand(this.splitConfidence, this.tieThreshold) == true) {
        ActiveRule newDefaultRule = newRule(defaultRule.getRuleNumberID(),
            (RuleActiveRegressionNode) defaultRule.getLearningNode(),
            ((RuleActiveRegressionNode) defaultRule.getLearningNode()).getStatisticsOtherBranchSplit()); // other branch
        defaultRule.split();
        defaultRule.setRuleNumberID(++ruleNumberID);
        // send out the new rule
        sendAddRuleEvent(defaultRule.getRuleNumberID(), this.defaultRule);
        defaultRule = newDefaultRule;
      }
    }
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

  @Override
  public void onCreate(int id) {
    this.processorId = id;
    this.statistics = new double[] { 0.0, 0, 0 };
    this.ruleNumberID = 0;
    this.defaultRule = newRule(++this.ruleNumberID);
  }

  /*
   * Clone processor
   */
  @Override
  public Processor newProcessor(Processor p) {
    AMRDefaultRuleProcessor oldProcessor = (AMRDefaultRuleProcessor) p;
    Builder builder = new Builder(oldProcessor);
    AMRDefaultRuleProcessor newProcessor = builder.build();
    newProcessor.resultStream = oldProcessor.resultStream;
    newProcessor.ruleStream = oldProcessor.ruleStream;
    return newProcessor;
  }

  /*
   * Send events
   */
  private void sendAddRuleEvent(int ruleID, ActiveRule rule) {
    RuleContentEvent rce = new RuleContentEvent(ruleID, rule, false);
    this.ruleStream.put(rce);
  }

  /*
   * Output streams
   */
  public void setRuleStream(Stream ruleStream) {
    this.ruleStream = ruleStream;
  }

  public Stream getRuleStream() {
    return this.ruleStream;
  }

  public void setResultStream(Stream resultStream) {
    this.resultStream = resultStream;
  }

  public Stream getResultStream() {
    return this.resultStream;
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

    private FIMTDDNumericAttributeClassLimitObserver numericObserver;

    private Instances dataset;

    public Builder(Instances dataset) {
      this.dataset = dataset;
    }

    public Builder(AMRDefaultRuleProcessor processor) {
      this.pageHinckleyThreshold = processor.pageHinckleyThreshold;
      this.pageHinckleyAlpha = processor.pageHinckleyAlpha;
      this.driftDetection = processor.driftDetection;
      this.predictionFunction = processor.predictionFunction;
      this.constantLearningRatioDecay = processor.constantLearningRatioDecay;
      this.learningRatio = processor.learningRatio;
      this.splitConfidence = processor.splitConfidence;
      this.tieThreshold = processor.tieThreshold;
      this.gracePeriod = processor.gracePeriod;

      this.numericObserver = processor.numericObserver;
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

    public Builder numericObserver(FIMTDDNumericAttributeClassLimitObserver numericObserver) {
      this.numericObserver = numericObserver;
      return this;
    }

    public AMRDefaultRuleProcessor build() {
      return new AMRDefaultRuleProcessor(this);
    }
  }

}
