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

import java.io.Serializable;
import java.util.Map;

import org.apache.samoa.instances.Instance;
import org.apache.samoa.instances.Instances;

/**
 * Learner interface for non-distributed learners.
 * 
 * @author abifet
 */
public interface LocalLearner extends Serializable {

  /**
   * Creates a new learner object.
   * 
   * @return the learner
   */
  LocalLearner create();

  /**
   * Predicts the class memberships for a given instance. If an instance is unclassified, the returned array elements
   * must be all zero.
   * 
   * @param inst
   *          the instance to be classified
   * @return an array containing the estimated membership probabilities of the test instance in each class
   */
  double[] getVotesForInstance(Instance inst);

  /**
   * Resets this classifier. It must be similar to starting a new classifier from scratch.
   * 
   */
  void resetLearning();

  /**
   * Trains this classifier incrementally using the given instance.
   * 
   * @param inst
   *          the instance to be used for training
   */
  void trainOnInstance(Instance inst);

  /**
   * Sets where to obtain the information of attributes of Instances
   * 
   * @param dataset
   *          the dataset that contains the information
   */
  @Deprecated
  public void setDataset(Instances dataset);

}
