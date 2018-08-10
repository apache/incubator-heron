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

import java.util.LinkedList;

/**
 * PassiveRule is a LearningRule that update its LearningNode with the received new LearningNode.
 * 
 * @author Anh Thu Vu
 * 
 */
public class PassiveRule extends LearningRule {

  /**
	 * 
	 */
  private static final long serialVersionUID = -5551571895910530275L;

  private RulePassiveRegressionNode learningNode;

  /*
   * Constructor to turn an ActiveRule into a PassiveRule
   */
  public PassiveRule(ActiveRule rule) {
    this.nodeList = new LinkedList<>();
    for (RuleSplitNode node : rule.nodeList) {
      this.nodeList.add(node.getACopy());
    }

    this.learningNode = new RulePassiveRegressionNode(rule.getLearningNode());
    this.ruleNumberID = rule.ruleNumberID;
  }

  @Override
  public RuleRegressionNode getLearningNode() {
    return this.learningNode;
  }

  @Override
  public void setLearningNode(RuleRegressionNode learningNode) {
    this.learningNode = (RulePassiveRegressionNode) learningNode;
  }

  /*
   * MOA GUI
   */
  @Override
  public void getDescription(StringBuilder sb, int indent) {
    // TODO Auto-generated method stub
  }
}
