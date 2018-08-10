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
import org.apache.samoa.learners.classifiers.rules.common.RulePassiveRegressionNode;
import org.apache.samoa.learners.classifiers.rules.common.RuleSplitNode;

/**
 * New features (of newly expanded rules) from Learners to Model Aggregators.
 * 
 * @author Anh Thu Vu
 * 
 */
public class PredicateContentEvent implements ContentEvent {

  /**
	 * 
	 */
  private static final long serialVersionUID = 7909435830443732451L;

  private int ruleNumberID;
  private RuleSplitNode ruleSplitNode;
  private RulePassiveRegressionNode learningNode;

  /*
   * Constructor
   */
  public PredicateContentEvent() {
    this(0, null, null);
  }

  public PredicateContentEvent(int ruleID, RuleSplitNode ruleSplitNode, RulePassiveRegressionNode learningNode) {
    this.ruleNumberID = ruleID;
    this.ruleSplitNode = ruleSplitNode; // is this is null: this is for updating learningNode only
    this.learningNode = learningNode;
  }

  @Override
  public String getKey() {
    return Integer.toString(this.ruleNumberID);
  }

  @Override
  public void setKey(String key) {
    // do nothing
  }

  @Override
  public boolean isLastEvent() {
    return false; // N/A
  }

  public int getRuleNumberID() {
    return this.ruleNumberID;
  }

  public RuleSplitNode getRuleSplitNode() {
    return this.ruleSplitNode;
  }

  public RulePassiveRegressionNode getLearningNode() {
    return this.learningNode;
  }

}
