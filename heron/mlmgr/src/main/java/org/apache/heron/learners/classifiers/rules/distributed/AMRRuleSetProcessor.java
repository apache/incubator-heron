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

import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.Processor;
import org.apache.samoa.instances.Instance;
import org.apache.samoa.instances.Instances;
import org.apache.samoa.learners.InstanceContentEvent;
import org.apache.samoa.learners.ResultContentEvent;
import org.apache.samoa.learners.classifiers.rules.common.ActiveRule;
import org.apache.samoa.learners.classifiers.rules.common.LearningRule;
import org.apache.samoa.learners.classifiers.rules.common.PassiveRule;
import org.apache.samoa.moa.classifiers.rules.core.voting.ErrorWeightedVote;
import org.apache.samoa.moa.classifiers.rules.core.voting.InverseErrorWeightedVote;
import org.apache.samoa.moa.classifiers.rules.core.voting.UniformWeightedVote;
import org.apache.samoa.topology.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Model Aggregator Processor (HAMR).
 * 
 * @author Anh Thu Vu
 * 
 */
public class AMRRuleSetProcessor implements Processor {

  /**
	 * 
	 */
  private static final long serialVersionUID = -6544096255649379334L;
  private static final Logger logger = LoggerFactory.getLogger(AMRRuleSetProcessor.class);

  private int processorId;

  // Rules & default rule
  protected transient CopyOnWriteArrayList<PassiveRule> ruleSet;

  // SAMOA Stream
  private Stream statisticsStream;
  private Stream resultStream;
  private Stream defaultRuleStream;

  // Options
  protected boolean noAnomalyDetection;
  protected double multivariateAnomalyProbabilityThreshold;
  protected double univariateAnomalyprobabilityThreshold;
  protected int anomalyNumInstThreshold;

  protected boolean unorderedRules;

  protected int voteType;

  /*
   * Constructor
   */
  public AMRRuleSetProcessor(Builder builder) {

    this.noAnomalyDetection = builder.noAnomalyDetection;
    this.multivariateAnomalyProbabilityThreshold = builder.multivariateAnomalyProbabilityThreshold;
    this.univariateAnomalyprobabilityThreshold = builder.univariateAnomalyprobabilityThreshold;
    this.anomalyNumInstThreshold = builder.anomalyNumInstThreshold;
    this.unorderedRules = builder.unorderedRules;

    this.voteType = builder.voteType;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.samoa.core.Processor#process(org.apache.samoa.core.
   * ContentEvent)
   */
  @Override
  public boolean process(ContentEvent event) {
    if (event instanceof InstanceContentEvent) {
      this.processInstanceEvent((InstanceContentEvent) event);
    }
    else if (event instanceof PredicateContentEvent) {
      PredicateContentEvent pce = (PredicateContentEvent) event;
      if (pce.getRuleSplitNode() == null) {
        this.updateLearningNode(pce);
      }
      else {
        this.updateRuleSplitNode(pce);
      }
    }
    else if (event instanceof RuleContentEvent) {
      RuleContentEvent rce = (RuleContentEvent) event;
      if (rce.isRemoving()) {
        this.removeRule(rce.getRuleNumberID());
      }
      else {
        addRule(rce.getRule());
      }
    }
    return true;
  }

  private void processInstanceEvent(InstanceContentEvent instanceEvent) {
    Instance instance = instanceEvent.getInstance();
    boolean predictionCovered = false;
    boolean trainingCovered = false;
    boolean continuePrediction = instanceEvent.isTesting();
    boolean continueTraining = instanceEvent.isTraining();

    ErrorWeightedVote errorWeightedVote = newErrorWeightedVote();
    for (PassiveRule aRuleSet : this.ruleSet) {
      if (!continuePrediction && !continueTraining)
        break;

      if (aRuleSet.isCovering(instance)) {
        predictionCovered = true;

        if (continuePrediction) {
          double[] vote = aRuleSet.getPrediction(instance);
          double error = aRuleSet.getCurrentError();
          errorWeightedVote.addVote(vote, error);
          if (!this.unorderedRules)
            continuePrediction = false;
        }

        if (continueTraining) {
          if (!isAnomaly(instance, aRuleSet)) {
            trainingCovered = true;
            aRuleSet.updateStatistics(instance);

            // Send instance to statistics PIs
            sendInstanceToRule(instance, aRuleSet.getRuleNumberID());

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

    boolean defaultPrediction = instanceEvent.isTesting() && !predictionCovered;
    boolean defaultTraining = instanceEvent.isTraining() && !trainingCovered;
    if (defaultPrediction || defaultTraining) {
      instanceEvent.setTesting(defaultPrediction);
      instanceEvent.setTraining(defaultTraining);
      this.defaultRuleStream.put(instanceEvent);
    }
  }

  private ResultContentEvent newResultContentEvent(double[] prediction, InstanceContentEvent inEvent) {
    ResultContentEvent rce = new ResultContentEvent(inEvent.getInstanceIndex(), inEvent.getInstance(),
        inEvent.getClassId(), prediction, inEvent.isLastEvent());
    rce.setClassifierIndex(this.processorId);
    rce.setEvaluationIndex(inEvent.getEvaluationIndex());
    return rce;
  }

  public ErrorWeightedVote newErrorWeightedVote() {
    // TODO: do a reset instead of init a new object
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
    if (!this.noAnomalyDetection) {
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
   * Add predicate/RuleSplitNode for a rule
   */
  private void updateRuleSplitNode(PredicateContentEvent pce) {
    int ruleID = pce.getRuleNumberID();
    for (PassiveRule rule : ruleSet) {
      if (rule.getRuleNumberID() == ruleID) {
        rule.nodeListAdd(pce.getRuleSplitNode());
        rule.setLearningNode(pce.getLearningNode());
      }
    }
  }

  private void updateLearningNode(PredicateContentEvent pce) {
    int ruleID = pce.getRuleNumberID();
    for (PassiveRule rule : ruleSet) {
      if (rule.getRuleNumberID() == ruleID) {
        rule.setLearningNode(pce.getLearningNode());
      }
    }
  }

  /*
   * Add new rule/Remove rule
   */
  private boolean addRule(ActiveRule rule) {
    this.ruleSet.add(new PassiveRule(rule));
    return true;
  }

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
    this.ruleSet = new CopyOnWriteArrayList<PassiveRule>();

  }

  /*
   * Clone processor
   */
  @Override
  public Processor newProcessor(Processor p) {
    AMRRuleSetProcessor oldProcessor = (AMRRuleSetProcessor) p;
    Builder builder = new Builder(oldProcessor);
    AMRRuleSetProcessor newProcessor = builder.build();
    newProcessor.resultStream = oldProcessor.resultStream;
    newProcessor.statisticsStream = oldProcessor.statisticsStream;
    newProcessor.defaultRuleStream = oldProcessor.defaultRuleStream;
    return newProcessor;
  }

  /*
   * Send events
   */
  private void sendInstanceToRule(Instance instance, int ruleID) {
    AssignmentContentEvent ace = new AssignmentContentEvent(ruleID, instance);
    this.statisticsStream.put(ace);
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

  public Stream getDefaultRuleStream() {
    return this.defaultRuleStream;
  }

  public void setDefaultRuleStream(Stream defaultRuleStream) {
    this.defaultRuleStream = defaultRuleStream;
  }

  /*
   * Builder
   */
  public static class Builder {
    private boolean noAnomalyDetection;
    private double multivariateAnomalyProbabilityThreshold;
    private double univariateAnomalyprobabilityThreshold;
    private int anomalyNumInstThreshold;

    private boolean unorderedRules;

    // private FIMTDDNumericAttributeClassLimitObserver numericObserver;
    private int voteType;

    private Instances dataset;

    public Builder(Instances dataset) {
      this.dataset = dataset;
    }

    public Builder(AMRRuleSetProcessor processor) {

      this.noAnomalyDetection = processor.noAnomalyDetection;
      this.multivariateAnomalyProbabilityThreshold = processor.multivariateAnomalyProbabilityThreshold;
      this.univariateAnomalyprobabilityThreshold = processor.univariateAnomalyprobabilityThreshold;
      this.anomalyNumInstThreshold = processor.anomalyNumInstThreshold;
      this.unorderedRules = processor.unorderedRules;

      this.voteType = processor.voteType;
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

    public Builder voteType(int voteType) {
      this.voteType = voteType;
      return this;
    }

    public AMRRuleSetProcessor build() {
      return new AMRRuleSetProcessor(this);
    }
  }

}
