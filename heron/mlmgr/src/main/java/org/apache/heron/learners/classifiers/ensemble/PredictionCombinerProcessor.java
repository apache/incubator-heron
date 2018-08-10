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
import java.util.HashMap;
import java.util.Map;

import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.Processor;
import org.apache.samoa.learners.ResultContentEvent;
import org.apache.samoa.moa.core.DoubleVector;
import org.apache.samoa.topology.Stream;

/**
 * Combines predictions coming from an ensemble. Equivalent to a majority-vote classifier.
 */
public class PredictionCombinerProcessor implements Processor {

  private static final long serialVersionUID = -1606045723451191132L;

  /**
   * The ensemble size.
   */
  protected int ensembleSize;

  /**
   * The output stream.
   */
  protected Stream outputStream;

  /**
   * Sets the output stream.
   * 
   * @param stream
   *          the new output stream
   */
  public void setOutputStream(Stream stream) {
    outputStream = stream;
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
   * Gets the size ensemble.
   * 
   * @return the ensembleSize
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

  protected Map<Integer, Integer> mapCountsforInstanceReceived;

  protected Map<Integer, DoubleVector> mapVotesforInstanceReceived;

  /**
   * On event.
   * 
   * @param event
   *          the event
   * @return true, if successful
   */
  public boolean process(ContentEvent event) {

    ResultContentEvent inEvent = (ResultContentEvent) event;
    double[] prediction = inEvent.getClassVotes();
    int instanceIndex = (int) inEvent.getInstanceIndex();

    addStatisticsForInstanceReceived(instanceIndex, inEvent.getClassifierIndex(), prediction, 1);
    if (hasAllVotesArrivedInstance(instanceIndex)) {
      DoubleVector combinedVote = this.mapVotesforInstanceReceived.get(instanceIndex);
      if (combinedVote == null) {
        combinedVote = new DoubleVector(new double[inEvent.getInstance().numClasses()]);
      }
      ResultContentEvent outContentEvent = new ResultContentEvent(inEvent.getInstanceIndex(), inEvent.getInstance(),
          inEvent.getClassId(), combinedVote.getArrayCopy(), inEvent.isLastEvent());
      outContentEvent.setEvaluationIndex(inEvent.getEvaluationIndex());
      outputStream.put(outContentEvent);
      clearStatisticsInstance(instanceIndex);
      return true;
    }
    return false;

  }

  @Override
  public void onCreate(int id) {
    this.reset();
  }

  public void reset() {
  }

  /*
   * (non-Javadoc)
   * @see samoa.core.Processor#newProcessor(samoa.core.Processor)
   */
  @Override
  public Processor newProcessor(Processor sourceProcessor) {
    PredictionCombinerProcessor newProcessor = new PredictionCombinerProcessor();
    PredictionCombinerProcessor originProcessor = (PredictionCombinerProcessor) sourceProcessor;
    if (originProcessor.getOutputStream() != null) {
      newProcessor.setOutputStream(originProcessor.getOutputStream());
    }
    newProcessor.setEnsembleSize(originProcessor.getEnsembleSize());
    return newProcessor;
  }

  protected void addStatisticsForInstanceReceived(int instanceIndex, int classifierIndex, double[] prediction, int add) {
    if (this.mapCountsforInstanceReceived == null) {
      this.mapCountsforInstanceReceived = new HashMap<>();
      this.mapVotesforInstanceReceived = new HashMap<>();
    }
    DoubleVector vote = new DoubleVector(prediction);
    if (vote.sumOfValues() > 0.0) {
      vote.normalize();
      DoubleVector combinedVote = this.mapVotesforInstanceReceived.get(instanceIndex);
      if (combinedVote == null) {
        combinedVote = new DoubleVector();
      }
      vote.scaleValues(getEnsembleMemberWeight(classifierIndex));
      combinedVote.addValues(vote);

      this.mapVotesforInstanceReceived.put(instanceIndex, combinedVote);
    }
    Integer count = this.mapCountsforInstanceReceived.get(instanceIndex);
    if (count == null) {
      count = 0;
    }
    this.mapCountsforInstanceReceived.put(instanceIndex, count + add);
  }

  protected boolean hasAllVotesArrivedInstance(int instanceIndex) {
    return (this.mapCountsforInstanceReceived.get(instanceIndex) == this.ensembleSize);
  }

  protected void clearStatisticsInstance(int instanceIndex) {
    this.mapCountsforInstanceReceived.remove(instanceIndex);
    this.mapVotesforInstanceReceived.remove(instanceIndex);
  }

  protected double getEnsembleMemberWeight(int i) {
    return 1.0;
  }

}
