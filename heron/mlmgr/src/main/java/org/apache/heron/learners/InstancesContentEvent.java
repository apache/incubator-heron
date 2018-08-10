package org.apache.samoa.learners;

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
 * License
 */

import net.jcip.annotations.Immutable;

import java.util.LinkedList;
import java.util.List;

import org.apache.samoa.core.ContentEvent;

/**
 * The Class InstanceEvent.
 */
@Immutable
final public class InstancesContentEvent implements ContentEvent {

  /**
	 * 
	 */
  private static final long serialVersionUID = -8620668863064613845L;

  protected List<InstanceContent> instanceList = new LinkedList<InstanceContent>();

  public InstancesContentEvent() {

  }

  /**
   * Instantiates a new event with a list of InstanceContent.
   *
   */

  public InstancesContentEvent(InstanceContentEvent event) {
    this.add(event.getInstanceContent());
  }


  public void add(InstanceContent instance) {
    instanceList.add(instance);
  }

  /**
   * Gets the single instance of InstanceEvent.
   * 
   * @return the instance.
   */
  public InstanceContent[] getInstances() {
    return instanceList.toArray(new InstanceContent[instanceList.size()]);
  }


  /**
   * Gets the classifier index.
   *
   * @return the classifier index
   */
  public int getClassifierIndex() {
    return this.instanceList.get(0).getClassifierIndex();
  }

  /**
   * Gets the evaluation index.
   *
   * @return the evaluation index
   */
  public int getEvaluationIndex() {
    return this.instanceList.get(0).getEvaluationIndex();
  }


  /*
   * (non-Javadoc)
   * 
   * @see samoa.core.ContentEvent#getKey(int)
   */
  public String getKey(int key) {
    if (key == 0)
      return Long.toString(this.getEvaluationIndex());
    else
      return Long.toString(10000
          * this.getEvaluationIndex()
          + this.getClassifierIndex());
  }

  @Override
  public String getKey() {
    // System.out.println("InstanceContentEvent "+Long.toString(this.instanceIndex));
    return Long.toString(this.getClassifierIndex());
  }

  @Override
  public void setKey(String key) {
    //No needed
  }

  @Override
  public boolean isLastEvent() {
    return this.instanceList.get(this.instanceList.size()-1).isLastEvent();
  }

  public List<InstanceContent> getList() {
    return this.instanceList;
  }
}
