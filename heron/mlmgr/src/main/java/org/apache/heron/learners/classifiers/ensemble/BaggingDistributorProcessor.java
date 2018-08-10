package org.apache.samoa.learners.classifiers.ensemble;

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

import java.util.Arrays;
import java.util.Random;

import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.Processor;
import org.apache.samoa.instances.Instance;
import org.apache.samoa.learners.InstanceContentEvent;
import org.apache.samoa.moa.core.MiscUtils;
import org.apache.samoa.topology.Stream;

import com.google.common.base.Preconditions;

/**
 * The Class BaggingDistributorPE.
 */
public class BaggingDistributorProcessor implements Processor {

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
    Preconditions.checkState(ensembleSize == ensembleStreams.length, String.format(
        "Ensemble size ({}) and number of enseble streams ({}) do not match.", ensembleSize, ensembleStreams.length));
    InstanceContentEvent inEvent = (InstanceContentEvent) event;

    if (inEvent.getInstanceIndex() < 0) {
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
    return true;
  }

  /**
   * Train.
   * 
   * @param inEvent
   *          the in event
   */
  protected void train(InstanceContentEvent inEvent) {
    Instance trainInstance = inEvent.getInstance();
    for (int i = 0; i < ensembleSize; i++) {
      int k = MiscUtils.poisson(1.0, this.random);
      if (k > 0) {
        Instance weightedInstance = trainInstance.copy();
        weightedInstance.setWeight(trainInstance.weight() * k);
        InstanceContentEvent instanceContentEvent = new InstanceContentEvent(inEvent.getInstanceIndex(),
            weightedInstance, true, false);
        instanceContentEvent.setClassifierIndex(i);
        instanceContentEvent.setEvaluationIndex(inEvent.getEvaluationIndex());
        ensembleStreams[i].put(instanceContentEvent);
      }
    }
  }

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

  public int getEnsembleSize() {
    return ensembleSize;
  }

  public void setEnsembleSize(int ensembleSize) {
    this.ensembleSize = ensembleSize;
  }

  @Override
  public Processor newProcessor(Processor sourceProcessor) {
    BaggingDistributorProcessor newProcessor = new BaggingDistributorProcessor();
    BaggingDistributorProcessor originProcessor = (BaggingDistributorProcessor) sourceProcessor;
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
