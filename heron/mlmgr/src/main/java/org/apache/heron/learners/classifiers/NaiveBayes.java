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

import java.util.HashMap;
import java.util.Map;

import org.apache.samoa.instances.Instance;
import org.apache.samoa.instances.Instances;
import org.apache.samoa.moa.classifiers.core.attributeclassobservers.GaussianNumericAttributeClassObserver;
import org.apache.samoa.moa.core.GaussianEstimator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of a non-distributed Naive Bayes classifier.
 * 
 * At the moment, the implementation models all attributes as numeric attributes.
 * 
 * @author Olivier Van Laere (vanlaere yahoo-inc dot com)
 */
public class NaiveBayes implements LocalLearner {

  /**
   * Default smoothing factor. For now fixed to 1E-20.
   */
  private static final double ADDITIVE_SMOOTHING_FACTOR = 1e-20;

  /**
   * serialVersionUID for serialization
   */
  private static final long serialVersionUID = 1325775209672996822L;

  /**
   * Instance of a logger for use in this class.
   */
  private static final Logger logger = LoggerFactory.getLogger(NaiveBayes.class);

  /**
   * The actual model.
   */
  protected Map<Integer, GaussianNumericAttributeClassObserver> attributeObservers;

  /**
   * Class statistics
   */
  protected Map<Integer, Double> classInstances;

  /**
   * Class zero-prototypes.
   */
  protected Map<Integer, Double> classPrototypes;

  /**
   * Retrieve the number of classes currently known to this local model
   * 
   * @return the number of classes currently known to this local model
   */
  protected int getNumberOfClasses() {
    return this.classInstances.size();
  }

  /**
   * Track training instances seen.
   */
  protected long instancesSeen = 0L;

  /**
   * Explicit no-arg constructor.
   */
  public NaiveBayes() {
    // Init the model
    resetLearning();
  }

  /**
   * Create an instance of this LocalLearner implementation.
   */
  @Override
  public LocalLearner create() {
    return new NaiveBayes();
  }

  /**
   * Predicts the class memberships for a given instance. If an instance is unclassified, the returned array elements
   * will be all zero.
   * 
   * Smoothing is being implemented by the AttributeClassObserver classes. At the moment, the
   * GaussianNumericProbabilityAttributeClassObserver needs no smoothing as it processes continuous variables.
   * 
   * Please note that we transform the scores to log space to avoid underflow, and we replace the multiplication with
   * addition.
   * 
   * The resulting scores are no longer probabilities, as a mixture of probability densities and probabilities can be
   * used in the computation.
   * 
   * @param inst
   *          the instance to be classified
   * @return an array containing the estimated membership scores of the test instance in each class, in log space.
   */
  @Override
  public double[] getVotesForInstance(Instance inst) {
    // Prepare the results array
    double[] votes = new double[getNumberOfClasses()];
    // Over all classes
    for (int classIndex = 0; classIndex < votes.length; classIndex++) {
      // Get the prior for this class
      votes[classIndex] = Math.log(getPrior(classIndex));
      // Iterate over the instance attributes
      for (int index = 0; index < inst.numAttributes(); index++) {
        int attributeID = inst.index(index);
        // Skip class attribute
        if (attributeID == inst.classIndex())
          continue;
        Double value = inst.value(attributeID);
        // Get the observer for the given attribute
        GaussianNumericAttributeClassObserver obs = attributeObservers.get(attributeID);
        // Init the estimator to null by default
        GaussianEstimator estimator = null;
        if (obs != null && obs.getEstimator(classIndex) != null) {
          // Get the estimator
          estimator = obs.getEstimator(classIndex);
        }
        double valueNonZero;
        // The null case should be handled by smoothing!
        if (estimator != null) {
          // Get the score for a NON-ZERO attribute value
          valueNonZero = estimator.probabilityDensity(value);
        }
        // We don't have an estimator
        else {
          // Assign a very small probability that we do see this value
          valueNonZero = ADDITIVE_SMOOTHING_FACTOR;
        }
        votes[classIndex] += Math.log(valueNonZero); // - Math.log(valueZero);
      }
      // Check for null in the case of prequential evaluation
      if (this.classPrototypes.get(classIndex) != null) {
        // Add the prototype for the class, already in log space
        votes[classIndex] += Math.log(this.classPrototypes.get(classIndex));
      }
    }
    return votes;
  }

  /**
   * Compute the prior for the given classIndex.
   * 
   * Implemented by maximum likelihood at the moment.
   * 
   * @param classIndex
   *          Id of the class for which we want to compute the prior.
   * @return Prior probability for the requested class
   */
  private double getPrior(int classIndex) {
    // Maximum likelihood
    Double currentCount = this.classInstances.get(classIndex);
    if (currentCount == null || currentCount == 0)
      return 0;
    else
      return currentCount * 1. / this.instancesSeen;
  }

  /**
   * Resets this classifier. It must be similar to starting a new classifier from scratch.
   */
  @Override
  public void resetLearning() {
    // Reset priors
    this.instancesSeen = 0L;
    this.classInstances = new HashMap<>();
    this.classPrototypes = new HashMap<>();
    // Init the attribute observers
    this.attributeObservers = new HashMap<>();
  }

  /**
   * Trains this classifier incrementally using the given instance.
   * 
   * @param inst
   *          the instance to be used for training
   */
  @Override
  public void trainOnInstance(Instance inst) {
    // Update class statistics with weights
    int classIndex = (int) inst.classValue();
    Double weight = this.classInstances.get(classIndex);
    if (weight == null)
      weight = 0.;
    this.classInstances.put(classIndex, weight + inst.weight());

    // Get the class prototype
    Double classPrototype = this.classPrototypes.get(classIndex);
    if (classPrototype == null)
      classPrototype = 1.;

    // Iterate over the attributes of the given instance
    for (int attributePosition = 0; attributePosition < inst
        .numAttributes(); attributePosition++) {
      // Get the attribute index - Dense -> 1:1, Sparse is remapped
      int attributeID = inst.index(attributePosition);
      // Skip class attribute
      if (attributeID == inst.classIndex())
        continue;
      // Get the attribute observer for the current attribute
      GaussianNumericAttributeClassObserver obs = this.attributeObservers
          .get(attributeID);
      // Lazy init of observers, if null, instantiate a new one
      if (obs == null) {
        // FIXME: At this point, we model everything as a numeric
        // attribute
        obs = new GaussianNumericAttributeClassObserver();
        this.attributeObservers.put(attributeID, obs);
      }

      // Get the probability density function under the current model
      GaussianEstimator obs_estimator = obs.getEstimator(classIndex);
      if (obs_estimator != null) {
        // Fetch the probability that the feature value is zero
        double probDens_zero_current = obs_estimator.probabilityDensity(0);
        classPrototype -= probDens_zero_current;
      }

      // FIXME: Sanity check on data values, for now just learn
      // Learn attribute value for given class
      obs.observeAttributeClass(inst.valueSparse(attributePosition),
          (int) inst.classValue(), inst.weight());

      // Update obs_estimator to fetch the pdf from the updated model
      obs_estimator = obs.getEstimator(classIndex);
      // Fetch the probability that the feature value is zero
      double probDens_zero_updated = obs_estimator.probabilityDensity(0);
      // Update the class prototype
      classPrototype += probDens_zero_updated;
    }
    // Store the class prototype
    this.classPrototypes.put(classIndex, classPrototype);
    // Count another training instance
    this.instancesSeen++;
  }

  @Override
  public void setDataset(Instances dataset) {
    // Do nothing
  }
}
