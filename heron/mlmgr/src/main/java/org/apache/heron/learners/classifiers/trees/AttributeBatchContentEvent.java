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

import java.util.LinkedList;
import java.util.List;

import org.apache.samoa.core.ContentEvent;

/**
 * Attribute Content Event represents the instances that split vertically based on their attribute
 * 
 * @author Arinto Murdopo
 * 
 */
final class AttributeBatchContentEvent implements ContentEvent {

  private static final long serialVersionUID = 6652815649846676832L;

  private final long learningNodeId;
  private final int obsIndex;
  private final List<ContentEvent> contentEventList;
  private final transient String key;
  private final boolean isNominal;

  public AttributeBatchContentEvent() {
    learningNodeId = -1;
    obsIndex = -1;
    contentEventList = new LinkedList<>();
    key = "";
    isNominal = true;
  }

  private AttributeBatchContentEvent(Builder builder) {
    this.learningNodeId = builder.learningNodeId;
    this.obsIndex = builder.obsIndex;
    this.contentEventList = new LinkedList<>();
    if (builder.contentEvent != null) {
      this.contentEventList.add(builder.contentEvent);
    }
    this.isNominal = builder.isNominal;
    this.key = builder.key;
  }

  public void add(ContentEvent contentEvent) {
    this.contentEventList.add(contentEvent);
  }

  @Override
  public String getKey() {
    return this.key;
  }

  @Override
  public void setKey(String str) {
    // do nothing, maybe useful when we want to reuse the object for
    // serialization/deserialization purpose
  }

  @Override
  public boolean isLastEvent() {
    return false;
  }

  long getLearningNodeId() {
    return this.learningNodeId;
  }

  int getObsIndex() {
    return this.obsIndex;
  }

  public List<ContentEvent> getContentEventList() {
    return this.contentEventList;
  }

  boolean isNominal() {
    return this.isNominal;
  }

  static final class Builder {

    // required parameters
    private final long learningNodeId;
    private final int obsIndex;
    private final String key;

    private ContentEvent contentEvent;
    private boolean isNominal = false;

    Builder(long id, int obsIndex, String key) {
      this.learningNodeId = id;
      this.obsIndex = obsIndex;
      this.key = key;
    }

    private Builder(long id, int obsIndex) {
      this.learningNodeId = id;
      this.obsIndex = obsIndex;
      this.key = "";
    }

    Builder contentEvent(ContentEvent contentEvent) {
      this.contentEvent = contentEvent;
      return this;
    }

    Builder isNominal(boolean val) {
      this.isNominal = val;
      return this;
    }

    AttributeBatchContentEvent build() {
      return new AttributeBatchContentEvent(this);
    }
  }

}
