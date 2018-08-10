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
import org.apache.samoa.instances.Instance;

/**
 * Forwarded instances from Model Agrregator to Learners/Default Rule Learner.
 * 
 * @author Anh Thu Vu
 * 
 */
public class AssignmentContentEvent implements ContentEvent {

  /**
	 * 
	 */
  private static final long serialVersionUID = 1031695762172836629L;

  private int ruleNumberID;
  private Instance instance;

  public AssignmentContentEvent() {
    this(0, null);
  }

  public AssignmentContentEvent(int ruleID, Instance instance) {
    this.ruleNumberID = ruleID;
    this.instance = instance;
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

  public Instance getInstance() {
    return this.instance;
  }

  public int getRuleNumberID() {
    return this.ruleNumberID;
  }

}
