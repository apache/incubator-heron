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
import org.apache.samoa.learners.classifiers.rules.common.ActiveRule;
import org.apache.samoa.learners.classifiers.rules.common.RuleActiveRegressionNode;
import org.apache.samoa.learners.classifiers.rules.common.RulePassiveRegressionNode;
import org.apache.samoa.learners.classifiers.rules.common.RuleSplitNode;
import org.apache.samoa.topology.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Learner Processor (VAMR).
 * 
 * @author Anh Thu Vu
 * 
 */
public class AMRulesStatisticsProcessor implements Processor {

  /**
	 * 
	 */
  private static final long serialVersionUID = 5268933189695395573L;

  private static final Logger logger =
      LoggerFactory.getLogger(AMRulesStatisticsProcessor.class);

  private int processorId;

  private transient List<ActiveRule> ruleSet;

  private Stream outputStream;

  private double splitConfidence;
  private double tieThreshold;
  private int gracePeriod;

  private int frequency;

  public AMRulesStatisticsProcessor(Builder builder) {
    this.splitConfidence = builder.splitConfidence;
    this.tieThreshold = builder.tieThreshold;
    this.gracePeriod = builder.gracePeriod;
    this.frequency = builder.frequency;
  }

  @Override
  public boolean process(ContentEvent event) {
    if (event instanceof AssignmentContentEvent) {

      AssignmentContentEvent attrContentEvent = (AssignmentContentEvent) event;
      trainRuleOnInstance(attrContentEvent.getRuleNumberID(), attrContentEvent.getInstance());
    }
    else if (event instanceof RuleContentEvent) {
      RuleContentEvent ruleContentEvent = (RuleContentEvent) event;
      if (!ruleContentEvent.isRemoving()) {
        addRule(ruleContentEvent.getRule());
      }
    }

    return false;
  }

  /*
   * Process input instances
   */
  private void trainRuleOnInstance(int ruleID, Instance instance) {
    Iterator<ActiveRule> ruleIterator = this.ruleSet.iterator();
    while (ruleIterator.hasNext()) {
      ActiveRule rule = ruleIterator.next();
      if (rule.getRuleNumberID() == ruleID) {
        // Check (again) for coverage
        // Skip anomaly check as Aggregator's perceptron should be well-updated
        if (rule.isCovering(instance) == true) {
          double error = rule.computeError(instance); // Use adaptive mode error
          boolean changeDetected = ((RuleActiveRegressionNode) rule.getLearningNode()).updateChangeDetection(error);
          if (changeDetected == true) {
            ruleIterator.remove();

            this.sendRemoveRuleEvent(ruleID);
          } else {
            rule.updateStatistics(instance);
            if (rule.getInstancesSeen() % this.gracePeriod == 0.0) {
              if (rule.tryToExpand(this.splitConfidence, this.tieThreshold)) {
                rule.split();

                // expanded: update Aggregator with new/updated predicate
                this.sendPredicate(rule.getRuleNumberID(), rule.getLastUpdatedRuleSplitNode(),
                    (RuleActiveRegressionNode) rule.getLearningNode());
              }
            }
          }
        }

        return;
      }
    }
  }

  private void sendRemoveRuleEvent(int ruleID) {
    RuleContentEvent rce = new RuleContentEvent(ruleID, null, true);
    this.outputStream.put(rce);
  }

  private void sendPredicate(int ruleID, RuleSplitNode splitNode, RuleActiveRegressionNode learningNode) {
    this.outputStream.put(new PredicateContentEvent(ruleID, splitNode, new RulePassiveRegressionNode(learningNode)));
  }

  /*
   * Process control message (regarding adding or removing rules)
   */
  private boolean addRule(ActiveRule rule) {
    this.ruleSet.add(rule);
    return true;
  }

  @Override
  public void onCreate(int id) {
    this.processorId = id;
    this.ruleSet = new LinkedList<ActiveRule>();
  }

  @Override
  public Processor newProcessor(Processor p) {
    AMRulesStatisticsProcessor oldProcessor = (AMRulesStatisticsProcessor) p;
    AMRulesStatisticsProcessor newProcessor =
        new AMRulesStatisticsProcessor.Builder(oldProcessor).build();

    newProcessor.setOutputStream(oldProcessor.outputStream);
    return newProcessor;
  }

  /*
   * Builder
   */
  public static class Builder {
    private double splitConfidence;
    private double tieThreshold;
    private int gracePeriod;

    private int frequency;

    private Instances dataset;

    public Builder(Instances dataset) {
      this.dataset = dataset;
    }

    public Builder(AMRulesStatisticsProcessor processor) {
      this.splitConfidence = processor.splitConfidence;
      this.tieThreshold = processor.tieThreshold;
      this.gracePeriod = processor.gracePeriod;
      this.frequency = processor.frequency;
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

    public Builder frequency(int frequency) {
      this.frequency = frequency;
      return this;
    }

    public AMRulesStatisticsProcessor build() {
      return new AMRulesStatisticsProcessor(this);
    }
  }

  /*
   * Output stream
   */
  public void setOutputStream(Stream stream) {
    this.outputStream = stream;
  }

  public Stream getOutputStream() {
    return this.outputStream;
  }

}
