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

/**
 * The most basic rule: inherit from Rule the ID and list of features.
 * 
 * @author Anh Thu Vu
 * 
 */
/*
 * This branch (Non-learning rule) was created for an old implementation.
 * Probably should remove None-Learning and Learning Rule classes, merge Rule
 * with LearningRule.
 */
public class NonLearningRule extends Rule {

  /**
	 * 
	 */
  private static final long serialVersionUID = -1210907339230307784L;

  public NonLearningRule(ActiveRule rule) {
    this.nodeList = rule.nodeList;
    this.ruleNumberID = rule.ruleNumberID;
  }

  @Override
  public void getDescription(StringBuilder sb, int indent) {
    // do nothing
  }

}
