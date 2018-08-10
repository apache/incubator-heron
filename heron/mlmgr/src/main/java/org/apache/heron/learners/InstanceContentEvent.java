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

import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.SerializableInstance;
import org.apache.samoa.instances.Instance;

import net.jcip.annotations.Immutable;

/**
 * The Class InstanceContentEvent.
 */
@Immutable
final public class InstanceContentEvent implements ContentEvent {

  /**
	 * 
	 */
  private static final long serialVersionUID = -8620668863064613845L;
  private InstanceContent instanceContent;

  public InstanceContentEvent() {

  }

  /**
   * Instantiates a new instance event.
   * 
   * @param index
   *          the index
   * @param instance
   *          the instance
   * @param isTraining
   *          the is training
   */
  public InstanceContentEvent(long index, Instance instance,
      boolean isTraining, boolean isTesting) {
    this.instanceContent = new InstanceContent(index, instance, isTraining, isTesting);
  }

  /**
   * Gets the single instance of InstanceEvent.
   * 
   * @return the instance.
   */
  public Instance getInstance() {
    return this.instanceContent.getInstance();
  }

  /**
   * Gets the instance index.
   * 
   * @return the index of the data vector.
   */
  public long getInstanceIndex() {
    return this.instanceContent.getInstanceIndex();
  }

  /**
   * Gets the class id.
   * 
   * @return the true class of the vector.
   */
  public int getClassId() {return this.instanceContent.getClassId();
  }

  /**
   * Checks if is training.
   * 
   * @return true if this is training data.
   */
  public boolean isTraining() {
    return this.instanceContent.isTraining();
  }

  /**
   * Set training flag.
   * 
   * @param training
   *          flag.
   */
  public void setTraining(boolean training) {this.instanceContent.setTraining(training);}


  /**
   * Checks if is testing.
   * 
   * @return true if this is testing data.
   */
  public boolean isTesting() {
    return this.instanceContent.isTesting();
  }

  /**
   * Set testing flag.
   * 
   * @param testing
   *          flag.
   */
  public void setTesting(boolean testing) {
    this.instanceContent.setTesting(testing);
  }

  /**
   * Gets the classifier index.
   * 
   * @return the classifier index
   */
  public int getClassifierIndex() {
    return this.instanceContent.getClassifierIndex();
  }

  /**
   * Sets the classifier index.
   * 
   * @param classifierIndex
   *          the new classifier index
   */
  public void setClassifierIndex(int classifierIndex) {
    this.instanceContent.setClassifierIndex(classifierIndex);
  }

  /**
   * Gets the evaluation index.
   * 
   * @return the evaluation index
   */
  public int getEvaluationIndex() {
    return this.instanceContent.getEvaluationIndex();
  }

  /**
   * Sets the evaluation index.
   * 
   * @param evaluationIndex
   *          the new evaluation index
   */
  public void setEvaluationIndex(int evaluationIndex) {
    this.instanceContent.setEvaluationIndex(evaluationIndex);
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
  public void setKey(String str) {
    this.instanceContent.setInstanceIndex(Long.parseLong(str));
  }

  @Override
  public boolean isLastEvent() {
    return this.instanceContent.isLastEvent();
  }

  public void setLast(boolean isLast) {
    this.instanceContent.setLast(isLast);
  }
  /**
   * Gets the Instance Content.
   *
   * @return the instance content
   */
  public InstanceContent getInstanceContent() {
    return instanceContent;
  }
}
