package org.apache.samoa.learners.classifiers;

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
import org.apache.samoa.core.Processor;
import org.apache.samoa.instances.Instance;
import org.apache.samoa.learners.InstanceContentEvent;
import org.apache.samoa.learners.ResultContentEvent;
import org.apache.samoa.moa.classifiers.core.driftdetection.ChangeDetector;
import org.apache.samoa.topology.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.samoa.moa.core.Utils.maxIndex;

/**
 * The Class LearnerProcessor.
 */
final public class LocalLearnerProcessor implements Processor {

  /**
	 * 
	 */
  private static final long serialVersionUID = -1577910988699148691L;

  private static final Logger logger = LoggerFactory.getLogger(LocalLearnerProcessor.class);

  private LocalLearner model;
  private Stream outputStream;
  private int modelId;
  private long instancesCount = 0;

  /**
   * Sets the learner.
   * 
   * @param model
   *          the model to set
   */
  public void setLearner(LocalLearner model) {
    this.model = model;
  }

  /**
   * Gets the learner.
   * 
   * @return the model
   */
  public LocalLearner getLearner() {
    return model;
  }

  /**
   * Set the output streams.
   * 
   * @param outputStream
   *          the new output stream
   */
  public void setOutputStream(Stream outputStream) {
    this.outputStream = outputStream;
  }

  /**
   * Gets the output stream.
   * 
   * @return the output stream
   */
  public Stream getOutputStream() {
    return outputStream;
  }

  /**
   * Gets the instances count.
   * 
   * @return number of observation vectors used in training iteration.
   */
  public long getInstancesCount() {
    return instancesCount;
  }

  /**
   * Update stats.
   * 
   * @param event
   *          the event
   */
  private void updateStats(InstanceContentEvent event) {
    Instance inst = event.getInstance();
    this.model.trainOnInstance(inst);
    this.instancesCount++;
    if (this.changeDetector != null) {
      boolean correctlyClassifies = this.correctlyClassifies(inst);
      double oldEstimation = this.changeDetector.getEstimation();
      this.changeDetector.input(correctlyClassifies ? 0 : 1);
      if (this.changeDetector.getChange() && this.changeDetector.getEstimation() > oldEstimation) {
        // Start a new classifier
        this.model.resetLearning();
        this.changeDetector.resetLearning();
      }
    }
  }

  /**
   * Gets whether this classifier correctly classifies an instance. Uses getVotesForInstance to obtain the prediction
   * and the instance to obtain its true class.
   * 
   * 
   * @param inst
   *          the instance to be classified
   * @return true if the instance is correctly classified
   */
  private boolean correctlyClassifies(Instance inst) {
    return maxIndex(model.getVotesForInstance(inst)) == (int) inst.classValue();
  }

  /** The test. */
  protected int test; // to delete

  /**
   * On event.
   * 
   * @param event
   *          the event
   * @return true, if successful
   */
  @Override
  public boolean process(ContentEvent event) {

    InstanceContentEvent inEvent = (InstanceContentEvent) event;
    Instance instance = inEvent.getInstance();

    if (inEvent.getInstanceIndex() < 0) {
      // end learning
      ResultContentEvent outContentEvent = new ResultContentEvent(-1, instance, 0,
          new double[0], inEvent.isLastEvent());
      outContentEvent.setClassifierIndex(this.modelId);
      outContentEvent.setEvaluationIndex(inEvent.getEvaluationIndex());
      outputStream.put(outContentEvent);
      return false;
    }

    if (inEvent.isTesting()) {
      double[] dist = model.getVotesForInstance(instance);
      ResultContentEvent outContentEvent = new ResultContentEvent(inEvent.getInstanceIndex(),
          instance, inEvent.getClassId(), dist, inEvent.isLastEvent());
      outContentEvent.setClassifierIndex(this.modelId);
      outContentEvent.setEvaluationIndex(inEvent.getEvaluationIndex());
      logger.trace(inEvent.getInstanceIndex() + " {} {}", modelId, dist);
      outputStream.put(outContentEvent);
    }

    if (inEvent.isTraining()) {
      updateStats(inEvent);
    }
    return false;
  }

  /*
   * (non-Javadoc)
   * 
   * @see samoa.core.Processor#onCreate(int)
   */
  @Override
  public void onCreate(int id) {
    this.modelId = id;
    model = model.create();
  }

  /*
   * (non-Javadoc)
   * 
   * @see samoa.core.Processor#newProcessor(samoa.core.Processor)
   */
  @Override
  public Processor newProcessor(Processor sourceProcessor) {
    LocalLearnerProcessor newProcessor = new LocalLearnerProcessor();
    LocalLearnerProcessor originProcessor = (LocalLearnerProcessor) sourceProcessor;

    if (originProcessor.getLearner() != null) {
      newProcessor.setLearner(originProcessor.getLearner().create());
    }

    if (originProcessor.getChangeDetector() != null) {
      newProcessor.setChangeDetector(originProcessor.getChangeDetector());
    }

    newProcessor.setOutputStream(originProcessor.getOutputStream());
    return newProcessor;
  }

  protected ChangeDetector changeDetector;

  public ChangeDetector getChangeDetector() {
    return this.changeDetector;
  }

  public void setChangeDetector(ChangeDetector cd) {
    this.changeDetector = cd;
  }

}
