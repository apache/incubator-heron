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
import java.util.List;

import org.apache.samoa.instances.Instance;
import org.apache.samoa.moa.AbstractMOAObject;
import org.apache.samoa.moa.classifiers.rules.core.conditionaltests.NumericAttributeBinaryRulePredicate;

/**
 * The base class for "rule". Represents the most basic rule with and ID and a list of features (nodeList).
 * 
 * @author Anh Thu Vu
 * 
 */
public abstract class Rule extends AbstractMOAObject {
  private static final long serialVersionUID = 1L;

  protected int ruleNumberID;

  protected List<RuleSplitNode> nodeList;

  /*
   * Constructor
   */
  public Rule() {
    this.nodeList = new LinkedList<RuleSplitNode>();
  }

  /*
   * Rule ID
   */
  public int getRuleNumberID() {
    return ruleNumberID;
  }

  public void setRuleNumberID(int ruleNumberID) {
    this.ruleNumberID = ruleNumberID;
  }

  /*
   * RuleSplitNode list
   */
  public List<RuleSplitNode> getNodeList() {
    return nodeList;
  }

  public void setNodeList(List<RuleSplitNode> nodeList) {
    this.nodeList = nodeList;
  }

  /*
   * Covering
   */
  public boolean isCovering(Instance inst) {
    boolean isCovering = true;
    for (RuleSplitNode node : nodeList) {
      if (node.evaluate(inst) == false) {
        isCovering = false;
        break;
      }
    }
    return isCovering;
  }

  /*
   * Add RuleSplitNode
   */
  public boolean nodeListAdd(RuleSplitNode ruleSplitNode) {
    // Check that the node is not already in the list
    boolean isIncludedInNodeList = false;
    boolean isUpdated = false;
    for (RuleSplitNode node : nodeList) {
      NumericAttributeBinaryRulePredicate nodeTest = (NumericAttributeBinaryRulePredicate) node.getSplitTest();
      NumericAttributeBinaryRulePredicate ruleSplitNodeTest = (NumericAttributeBinaryRulePredicate) ruleSplitNode
          .getSplitTest();
      if (nodeTest.isUsingSameAttribute(ruleSplitNodeTest)) {
        isIncludedInNodeList = true;
        if (nodeTest.isIncludedInRuleNode(ruleSplitNodeTest) == true) { // remove this line to keep the most recent attribute value
          // replace the value
          nodeTest.setAttributeValue(ruleSplitNodeTest);
          isUpdated = true; // if is updated (i.e. an expansion happened) a new learning node should be created
        }
      }
    }
    if (isIncludedInNodeList == false) {
      this.nodeList.add(ruleSplitNode);
    }
    return (!isIncludedInNodeList || isUpdated);
  }
}
