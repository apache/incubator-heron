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

import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.SerializableInstance;
import org.apache.samoa.instances.Instance;

/**
 * License
 */

/**
 * The Class ResultEvent.
 */
final public class ResultContentEvent implements ContentEvent {

  /**
	 * 
	 */
  private static final long serialVersionUID = -2650420235386873306L;
  private long instanceIndex;
  private int classifierIndex;
  private int evaluationIndex;
  private SerializableInstance instance;

  private int classId;
  private double[] classVotes;

  private final boolean isLast;

  public ResultContentEvent() {
    this.isLast = false;
  }

  public ResultContentEvent(boolean isLast) {
    this.isLast = isLast;
  }

  /**
   * Instantiates a new result event.
   * 
   * @param instanceIndex
   *          the instance index
   * @param instance
   *          the instance
   * @param classId
   *          the class id
   * @param classVotes
   *          the class votes
   */
  public ResultContentEvent(long instanceIndex, Instance instance, int classId,
      double[] classVotes, boolean isLast) {
    if (instance != null) {
      this.instance = new SerializableInstance(instance);
    }
    this.instanceIndex = instanceIndex;
    this.classId = classId;
    this.classVotes = classVotes;
    this.isLast = isLast;
  }

  /**
   * Gets the single instance of ResultEvent.
   * 
   * @return single instance of ResultEvent
   */
  public SerializableInstance getInstance() {
    return instance;
  }

  /**
   * Sets the instance.
   * 
   * @param instance
   *          the new instance
   */
  public void setInstance(SerializableInstance instance) {
    this.instance = instance;
  }

  /**
   * Gets the num classes.
   * 
   * @return the num classes
   */
  public int getNumClasses() { // To remove
    return instance.numClasses();
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
  public int getClassId() { // To remove
    return classId;// (int) instance.classValue();//classId;
  }

  /**
   * Gets the class votes.
   * 
   * @return the class votes
   */
  public double[] getClassVotes() {
    return classVotes;
  }

  /**
   * Sets the class votes.
   * 
   * @param classVotes
   *          the new class votes
   */
  public void setClassVotes(double[] classVotes) {
    this.classVotes = classVotes;
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

  /*
   * (non-Javadoc)
   * 
   * @see samoa.core.ContentEvent#getKey(int)
   */
  // @Override
  public String getKey(int key) {
    if (key == 0)
      return Long.toString(this.getEvaluationIndex());
    else
      return Long.toString(this.getEvaluationIndex()
          + 1000 * this.getInstanceIndex());
  }

  @Override
  public String getKey() {
    return Long.toString(this.getEvaluationIndex() % 100);
  }

  @Override
  public void setKey(String str) {
    this.evaluationIndex = Integer.parseInt(str);
  }

  @Override
  public boolean isLastEvent() {
    return isLast;
  }

}
