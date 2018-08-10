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
import java.util.Random;

import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.instances.Instance;
import org.apache.samoa.learners.InstanceContentEvent;
import org.apache.samoa.learners.ResultContentEvent;
import org.apache.samoa.moa.core.DoubleVector;
import org.apache.samoa.moa.core.Utils;
import org.apache.samoa.topology.Stream;

/**
 * The Class BoostingPredictionCombinerProcessor.
 */
public class BoostingPredictionCombinerProcessor extends PredictionCombinerProcessor {

  private static final long serialVersionUID = -1606045723451191232L;

  // Weigths classifier
  protected double[] scms;

  // Weights instance
  protected double[] swms;

  /**
   * On event.
   * 
   * @param event
   *          the event
   * @return true, if successful
   */
  @Override
  public boolean process(ContentEvent event) {

    ResultContentEvent inEvent = (ResultContentEvent) event;
    double[] prediction = inEvent.getClassVotes();
    int instanceIndex = (int) inEvent.getInstanceIndex();

    addStatisticsForInstanceReceived(instanceIndex, inEvent.getClassifierIndex(), prediction, 1);
    // Boosting
    addPredictions(instanceIndex, inEvent, prediction);

    if (inEvent.isLastEvent() || hasAllVotesArrivedInstance(instanceIndex)) {
      DoubleVector combinedVote = this.mapVotesforInstanceReceived.get(instanceIndex);
      if (combinedVote == null) {
        combinedVote = new DoubleVector();
      }
      ResultContentEvent outContentEvent = new ResultContentEvent(inEvent.getInstanceIndex(),
          inEvent.getInstance(), inEvent.getClassId(),
          combinedVote.getArrayCopy(), inEvent.isLastEvent());
      outContentEvent.setEvaluationIndex(inEvent.getEvaluationIndex());
      outputStream.put(outContentEvent);
      clearStatisticsInstance(instanceIndex);
      // Boosting
      computeBoosting(inEvent, instanceIndex);
      return true;
    }
    return false;

  }

  protected Random random;

  protected int trainingWeightSeenByModel;

  @Override
  protected double getEnsembleMemberWeight(int i) {
    double em = this.swms[i] / (this.scms[i] + this.swms[i]);
    if ((em == 0.0) || (em > 0.5)) {
      return 0.0;
    }
    double Bm = em / (1.0 - em);
    return Math.log(1.0 / Bm);
  }

  @Override
  public void reset() {
    this.random = new Random();
    this.trainingWeightSeenByModel = 0;
    this.scms = new double[this.ensembleSize];
    this.swms = new double[this.ensembleSize];
  }

  private boolean correctlyClassifies(int i, Instance inst, int instanceIndex) {
    int predictedClass = (int) mapPredictions.get(instanceIndex).getValue(i);
    return predictedClass == (int) inst.classValue();
  }

  protected Map<Integer, DoubleVector> mapPredictions;

  private void addPredictions(int instanceIndex, ResultContentEvent inEvent, double[] prediction) {
    if (this.mapPredictions == null) {
      this.mapPredictions = new HashMap<>();
    }
    DoubleVector predictions = this.mapPredictions.get(instanceIndex);
    if (predictions == null) {
      predictions = new DoubleVector();
    }
    predictions.setValue(inEvent.getClassifierIndex(), Utils.maxIndex(prediction));
    this.mapPredictions.put(instanceIndex, predictions);
  }

  private void computeBoosting(ResultContentEvent inEvent, int instanceIndex) {
    // Starts code for Boosting
    // Send instances to train
    double lambda_d = 1.0;
    for (int i = 0; i < this.ensembleSize; i++) {
      double k = lambda_d;
      Instance inst = inEvent.getInstance();
      if (k > 0.0) {
        Instance weightedInst = inst.copy();
        weightedInst.setWeight(inst.weight() * k);
        // this.ensemble[i].trainOnInstance(weightedInst);
        InstanceContentEvent instanceContentEvent = new InstanceContentEvent(
            inEvent.getInstanceIndex(), weightedInst, true, false);
        instanceContentEvent.setClassifierIndex(i);
        instanceContentEvent.setEvaluationIndex(inEvent.getEvaluationIndex());
        trainingStream.put(instanceContentEvent);
      }
      if (this.correctlyClassifies(i, inst, instanceIndex)) {
        this.scms[i] += lambda_d;
        lambda_d *= this.trainingWeightSeenByModel / (2 * this.scms[i]);
      } else {
        this.swms[i] += lambda_d;
        lambda_d *= this.trainingWeightSeenByModel / (2 * this.swms[i]);
      }
    }
  }

  /**
   * Gets the training stream.
   * 
   * @return the training stream
   */
  public Stream getTrainingStream() {
    return trainingStream;
  }

  /**
   * Sets the training stream.
   * 
   * @param trainingStream
   *          the new training stream
   */
  public void setTrainingStream(Stream trainingStream) {
    this.trainingStream = trainingStream;
  }

  /** The training stream. */
  private Stream trainingStream;

}
