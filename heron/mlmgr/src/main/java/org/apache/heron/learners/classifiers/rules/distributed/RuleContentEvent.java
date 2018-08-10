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
import org.apache.samoa.learners.classifiers.rules.common.ActiveRule;

/**
 * New rule from Model Aggregator/Default Rule Learner to Learners or removed rule from Learner to Model Aggregators.
 * 
 * @author Anh Thu Vu
 * 
 */
public class RuleContentEvent implements ContentEvent {

  /**
	 * 
	 */
  private static final long serialVersionUID = -9046390274402894461L;

  private final int ruleNumberID;
  private final ActiveRule addingRule; // for removing rule, we only need the rule's ID
  private final boolean isRemoving;

  public RuleContentEvent() {
    this(0, null, false);
  }

  public RuleContentEvent(int ruleID, ActiveRule rule, boolean isRemoving) {
    this.ruleNumberID = ruleID;
    this.isRemoving = isRemoving;
    this.addingRule = rule;
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
    return false;
  }

  public int getRuleNumberID() {
    return this.ruleNumberID;
  }

  public ActiveRule getRule() {
    return this.addingRule;
  }

  public boolean isRemoving() {
    return this.isRemoving;
  }

}
