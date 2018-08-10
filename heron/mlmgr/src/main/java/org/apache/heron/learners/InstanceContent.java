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

import org.apache.samoa.core.SerializableInstance;
import org.apache.samoa.instances.Instance;

import java.io.Serializable;

/**
 * The Class InstanceContent.
 */
@Immutable
final public class InstanceContent implements Serializable {

  private static final long serialVersionUID = -8620668863064613841L;

  private long instanceIndex;
  private int classifierIndex;
  private int evaluationIndex;
  private SerializableInstance instance;
  private boolean isTraining;
  private boolean isTesting;
  private boolean isLast = false;

  public InstanceContent() {

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
  public InstanceContent(long index, Instance instance,
      boolean isTraining, boolean isTesting) {
    if (instance != null) {
      this.instance = new SerializableInstance(instance);
    }
    this.instanceIndex = index;
    this.isTraining = isTraining;
    this.isTesting = isTesting;
  }

  /**
   * Gets the single instance of InstanceEvent.
   * 
   * @return the instance.
   */
  public Instance getInstance() {
    return instance;
  }

  /**
   * Gets the instance index.
   * 
   * @return the index of the data vector.
   */
  public long getInstanceIndex() {
    return instanceIndex;
  }

  /**
   * Gets the class id.
   * 
   * @return the true class of the vector.
   */
  public int getClassId() {
    // return classId;
    return (int) instance.classValue();
  }

  /**
   * Checks if is training.
   * 
   * @return true if this is training data.
   */
  public boolean isTraining() {
    return isTraining;
  }

  /**
   * Set training flag.
   * 
   * @param training
   *          flag.
   */
  public void setTraining(boolean training) {
    this.isTraining = training;
  }

  /**
   * Checks if is testing.
   * 
   * @return true if this is testing data.
   */
  public boolean isTesting() {
    return isTesting;
  }

  /**
   * Set testing flag.
   * 
   * @param testing
   *          flag.
   */
  public void setTesting(boolean testing) {
    this.isTesting = testing;
  }

  /**
   * Gets the classifier index.
   * 
   * @return the classifier index
   */
  public int getClassifierIndex() {
    return classifierIndex;
  }

  /**
   * Sets the classifier index.
   * 
   * @param classifierIndex
   *          the new classifier index
   */
  public void setClassifierIndex(int classifierIndex) {
    this.classifierIndex = classifierIndex;
  }

  /**
   * Gets the evaluation index.
   * 
   * @return the evaluation index
   */
  public int getEvaluationIndex() {
    return evaluationIndex;
  }

  /**
   * Sets the evaluation index.
   * 
   * @param evaluationIndex
   *          the new evaluation index
   */
  public void setEvaluationIndex(int evaluationIndex) {
    this.evaluationIndex = evaluationIndex;
  }

  /**
   * Sets the instance index.
   *
   * @param instanceIndex
   *          the new evaluation index
   */
  public void setInstanceIndex(long instanceIndex) {
    this.instanceIndex = instanceIndex;
  }

  public boolean isLastEvent() {
    return isLast;
  }

  public void setLast(boolean isLast) {
    this.isLast = isLast;
  }

  @Override
  public String toString() {
    return String
        .format(
            "InstanceContent [instanceIndex=%s, classifierIndex=%s, evaluationIndex=%s, instance=%s, isTraining=%s, isTesting=%s, isLast=%s]",
            instanceIndex, classifierIndex, evaluationIndex, instance, isTraining, isTesting, isLast);
  }
}
