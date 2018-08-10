package org.apache.samoa.learners.classifiers.ensemble;

import java.util.Arrays;
import java.util.Random;

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
import org.apache.samoa.topology.Stream;

/**
 * The Class BaggingDistributorPE.
 */
public class ShardingDistributorProcessor implements Processor {

  private static final long serialVersionUID = -1550901409625192730L;

  /** The ensemble size. */
  private int ensembleSize;

  /** The stream ensemble. */
  private Stream[] ensembleStreams;

  /** Ramdom number generator. */
  protected Random random = new Random(); //TODO make random seed configurable

  /**
   * On event.
   * 
   * @param event
   *          the event
   * @return true, if successful
   */
  public boolean process(ContentEvent event) {
    InstanceContentEvent inEvent = (InstanceContentEvent) event;
    if (inEvent.isLastEvent()) {
      // end learning
      for (Stream stream : ensembleStreams)
        stream.put(event);
      return false;
    }

    if (inEvent.isTesting()) {
      Instance testInstance = inEvent.getInstance();
      for (int i = 0; i < ensembleSize; i++) {
        Instance instanceCopy = testInstance.copy();
        InstanceContentEvent instanceContentEvent = new InstanceContentEvent(inEvent.getInstanceIndex(), instanceCopy,
            false, true);
        instanceContentEvent.setClassifierIndex(i); //TODO probably not needed anymore
        instanceContentEvent.setEvaluationIndex(inEvent.getEvaluationIndex()); //TODO probably not needed anymore
        ensembleStreams[i].put(instanceContentEvent);
      }
    }

    // estimate model parameters using the training data
    if (inEvent.isTraining()) {
      train(inEvent);
    }
    return false;
  }

  /**
   * Train.
   * 
   * @param inEvent
   *          the in event
   */
  protected void train(InstanceContentEvent inEvent) {
    Instance trainInst = inEvent.getInstance().copy();
    InstanceContentEvent instanceContentEvent = new InstanceContentEvent(inEvent.getInstanceIndex(), trainInst,
        true, false);
    int i = random.nextInt(ensembleSize);
    instanceContentEvent.setClassifierIndex(i);
    instanceContentEvent.setEvaluationIndex(inEvent.getEvaluationIndex());
    ensembleStreams[i].put(instanceContentEvent);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.s4.core.ProcessingElement#onCreate()
   */
  @Override
  public void onCreate(int id) {
    // do nothing
  }

  public Stream[] getOutputStreams() {
    return ensembleStreams;
  }

  public void setOutputStreams(Stream[] ensembleStreams) {
    this.ensembleStreams = ensembleStreams;
  }

  /**
   * Gets the size ensemble.
   * 
   * @return the size ensemble
   */
  public int getEnsembleSize() {
    return ensembleSize;
  }

  /**
   * Sets the size ensemble.
   * 
   * @param ensembleSize
   *          the new size ensemble
   */
  public void setEnsembleSize(int ensembleSize) {
    this.ensembleSize = ensembleSize;
  }

  /*
   * (non-Javadoc)
   * 
   * @see samoa.core.Processor#newProcessor(samoa.core.Processor)
   */
  @Override
  public Processor newProcessor(Processor sourceProcessor) {
    ShardingDistributorProcessor newProcessor = new ShardingDistributorProcessor();
    ShardingDistributorProcessor originProcessor = (ShardingDistributorProcessor) sourceProcessor;
    if (originProcessor.getOutputStreams() != null) {
      newProcessor.setOutputStreams(Arrays.copyOf(originProcessor.getOutputStreams(),
          originProcessor.getOutputStreams().length));
    }
    newProcessor.setEnsembleSize(originProcessor.getEnsembleSize());
    /*
     * if (originProcessor.getLearningCurve() != null){
     * newProcessor.setLearningCurve((LearningCurve)
     * originProcessor.getLearningCurve().copy()); }
     */
    return newProcessor;
  }
}
