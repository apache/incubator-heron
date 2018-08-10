package org.apache.samoa.learners.classifiers.rules.common;

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

import java.io.Serializable;

import org.apache.samoa.instances.Instance;
import org.apache.samoa.moa.core.DoubleVector;

/**
 * The base class for LearningNode for regression rule.
 * 
 * @author Anh Thu Vu
 * 
 */
public abstract class RuleRegressionNode implements Serializable {

  private static final long serialVersionUID = 9129659494380381126L;

  protected int predictionFunction;
  protected int ruleNumberID;
  // The statistics for this node:
  // Number of instances that have reached it
  // Sum of y values
  // Sum of squared y values
  protected DoubleVector nodeStatistics;

  protected Perceptron perceptron;
  protected TargetMean targetMean;
  protected double learningRatio;

  /*
   * Simple setters & getters
   */
  public Perceptron getPerceptron() {
    return perceptron;
  }

  public void setPerceptron(Perceptron perceptron) {
    this.perceptron = perceptron;
  }

  public TargetMean getTargetMean() {
    return targetMean;
  }

  public void setTargetMean(TargetMean targetMean) {
    this.targetMean = targetMean;
  }

  /*
   * Create a new RuleRegressionNode
   */
  public RuleRegressionNode(double[] initialClassObservations) {
    this.nodeStatistics = new DoubleVector(initialClassObservations);
  }

  public RuleRegressionNode() {
    this(new double[0]);
  }

  /*
   * Update statistics with input instance
   */
  public abstract void updateStatistics(Instance instance);

  /*
   * Predictions
   */
  public double[] getPrediction(Instance instance) {
    int predictionMode = this.getLearnerToUse(this.predictionFunction);
    return getPrediction(instance, predictionMode);
  }

  public double[] getSimplePrediction() {
    if (this.targetMean != null)
      return this.targetMean.getVotesForInstance();
    else
      return new double[] { 0 };
  }

  public double[] getPrediction(Instance instance, int predictionMode) {
    double[] ret;
    if (predictionMode == 1)
      ret = this.perceptron.getVotesForInstance(instance);
    else
      ret = this.targetMean.getVotesForInstance(instance);
    return ret;
  }

  public double getNormalizedPrediction(Instance instance) {
    double res;
    double[] aux;
    switch (this.predictionFunction) {
    // perceptron - 1
    case 1:
      res = this.perceptron.normalizedPrediction(instance);
      break;
    // target mean - 2
    case 2:
      aux = this.targetMean.getVotesForInstance();
      res = normalize(aux[0]);
      break;
    // adaptive - 0
    case 0:
      int predictionMode = this.getLearnerToUse(0);
      if (predictionMode == 1)
      {
        res = this.perceptron.normalizedPrediction(instance);
      }
      else {
        aux = this.targetMean.getVotesForInstance(instance);
        res = normalize(aux[0]);
      }
      break;
    default:
      throw new UnsupportedOperationException("Prediction mode not in range.");
    }
    return res;
  }

  /*
   * Get learner mode
   */
  public int getLearnerToUse(int predMode) {
    int predictionMode = predMode;
    if (predictionMode == 0) {
      double perceptronError = this.perceptron.getCurrentError();
      double meanTargetError = this.targetMean.getCurrentError();
      if (perceptronError < meanTargetError)
        predictionMode = 1; // PERCEPTRON
      else
        predictionMode = 2; // TARGET MEAN
    }
    return predictionMode;
  }

  /*
   * Error and change detection
   */
  public double computeError(Instance instance) {
    double normalizedPrediction = getNormalizedPrediction(instance);
    double normalizedClassValue = normalize(instance.classValue());
    return Math.abs(normalizedClassValue - normalizedPrediction);
  }

  public double getCurrentError() {
    double error;
    if (this.perceptron != null) {
      if (targetMean == null)
        error = perceptron.getCurrentError();
      else {
        double errorP = perceptron.getCurrentError();
        double errorTM = targetMean.getCurrentError();
        error = (errorP < errorTM) ? errorP : errorTM;
      }
    }
    else
      error = Double.MAX_VALUE;
    return error;
  }

  /*
   * no. of instances seen
   */
  public long getInstancesSeen() {
    if (nodeStatistics != null) {
      return (long) this.nodeStatistics.getValue(0);
    } else {
      return 0;
    }
  }

  public DoubleVector getNodeStatistics() {
    return this.nodeStatistics;
  }

  /*
   * Anomaly detection
   */
  public boolean isAnomaly(Instance instance,
      double uniVariateAnomalyProbabilityThreshold,
      double multiVariateAnomalyProbabilityThreshold,
      int numberOfInstanceesForAnomaly) {
    // AMRUles is equipped with anomaly detection. If on, compute the anomaly
    // value.
    long perceptronIntancesSeen = this.perceptron.getInstancesSeen();
    if (perceptronIntancesSeen >= numberOfInstanceesForAnomaly) {
      double attribSum;
      double attribSquaredSum;
      double D = 0.0;
      double N = 0.0;
      double anomaly;

      for (int x = 0; x < instance.numAttributes() - 1; x++) {
        // Perceptron is initialized each rule.
        // this is a local anomaly.
        int instAttIndex = modelAttIndexToInstanceAttIndex(x, instance);
        attribSum = this.perceptron.perceptronattributeStatistics.getValue(x);
        attribSquaredSum = this.perceptron.squaredperceptronattributeStatistics.getValue(x);
        double mean = attribSum / perceptronIntancesSeen;
        double sd = computeSD(attribSquaredSum, attribSum, perceptronIntancesSeen);
        double probability = computeProbability(mean, sd, instance.value(instAttIndex));

        if (probability > 0.0) {
          D = D + Math.abs(Math.log(probability));
          if (probability < uniVariateAnomalyProbabilityThreshold) {// 0.10
            N = N + Math.abs(Math.log(probability));
          }
        }
      }

      anomaly = 0.0;
      if (D != 0.0) {
        anomaly = N / D;
      }
      if (anomaly >= multiVariateAnomalyProbabilityThreshold) {
        // debuganomaly(instance,
        // uniVariateAnomalyProbabilityThreshold,
        // multiVariateAnomalyProbabilityThreshold,
        // anomaly);
        return true;
      }
    }
    return false;
  }

  /*
   * Helpers
   */
  public static double computeProbability(double mean, double sd, double value) {
    double probability = 0.0;

    if (sd > 0.0) {
      double k = (Math.abs(value - mean) / sd); // One tailed variant of Chebyshev's inequality
      probability = 1.0 / (1 + k * k);
    }

    return probability;
  }

  public static double computeHoeffdingBound(double range, double confidence, double n) {
    return Math.sqrt(((range * range) * Math.log(1.0 / confidence)) / (2.0 * n));
  }

  private double normalize(double value) {
    double meanY = this.nodeStatistics.getValue(1) / this.nodeStatistics.getValue(0);
    double sdY = computeSD(this.nodeStatistics.getValue(2), this.nodeStatistics.getValue(1),
        (long) this.nodeStatistics.getValue(0));
    double normalizedY = 0.0;
    if (sdY > 0.0000001) {
      normalizedY = (value - meanY) / (sdY);
    }
    return normalizedY;
  }

  public double computeSD(double squaredVal, double val, long size) {
    if (size > 1) {
      return Math.sqrt((squaredVal - ((val * val) / size)) / (size - 1.0));
    }
    return 0.0;
  }

  /**
   * Gets the index of the attribute in the instance, given the index of the attribute in the learner.
   * 
   * @param index
   *          the index of the attribute in the learner
   * @param inst
   *          the instance
   * @return the index in the instance
   */
  protected static int modelAttIndexToInstanceAttIndex(int index, Instance inst) {
    return index <= inst.classIndex() ? index : index + 1;
  }
}
