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

import org.apache.samoa.instances.Instance;
import org.apache.samoa.learners.classifiers.trees.SplitNode;
import org.apache.samoa.moa.classifiers.core.conditionaltests.InstanceConditionalTest;
import org.apache.samoa.moa.classifiers.rules.core.Predicate;
import org.apache.samoa.moa.classifiers.rules.core.conditionaltests.NumericAttributeBinaryRulePredicate;

/**
 * Represent a feature of rules (an element of ruleś nodeList).
 * 
 * @author Anh Thu Vu
 * 
 */
public class RuleSplitNode extends SplitNode {

  protected double lastTargetMean;
  protected int operatorObserver;

  private static final long serialVersionUID = 1L;

  public InstanceConditionalTest getSplitTest() {
    return this.splitTest;
  }

  /**
   * Create a new RuleSplitNode
   */
  public RuleSplitNode() {
    this(null, new double[0]);
  }

  public RuleSplitNode(InstanceConditionalTest splitTest, double[] classObservations) {
    super(splitTest, classObservations);
  }

  public RuleSplitNode getACopy() {
    InstanceConditionalTest splitTest = new NumericAttributeBinaryRulePredicate(
        (NumericAttributeBinaryRulePredicate) this.getSplitTest());
    return new RuleSplitNode(splitTest, this.getObservedClassDistribution());
  }

  public boolean evaluate(Instance instance) {
    Predicate predicate = (Predicate) this.splitTest;
    return predicate.evaluate(instance);
  }

}
