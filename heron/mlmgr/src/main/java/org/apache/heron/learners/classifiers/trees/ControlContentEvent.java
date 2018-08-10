package org.apache.samoa.learners.classifiers.trees;

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

/**
 * Abstract class to represent ContentEvent to control Local Statistic Processor.
 * 
 * @author Arinto Murdopo
 * 
 */
abstract class ControlContentEvent implements ContentEvent {

  /**
	 * 
	 */
  private static final long serialVersionUID = 5837375639629708363L;

  protected final long learningNodeId;

  public ControlContentEvent() {
    this.learningNodeId = -1;
  }

  ControlContentEvent(long id) {
    this.learningNodeId = id;
  }

  @Override
  public final String getKey() {
    return null;
  }

  @Override
  public void setKey(String str) {
    // Do nothing
  }

  @Override
  public boolean isLastEvent() {
    return false;
  }

  final long getLearningNodeId() {
    return this.learningNodeId;
  }

  abstract LocStatControl getType();

  static enum LocStatControl {
    COMPUTE, DELETE
  }
}
