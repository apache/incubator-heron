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

import org.apache.samoa.moa.classifiers.core.driftdetection.ChangeDetector;

/**
 * The Interface Adaptive Learner. Initializing Classifier should initalize PI to connect the Classifier with the input
 * stream and initialize result stream so that other PI can connect to the classification result of this classifier
 */

public interface AdaptiveLearner extends Learner{

  /**
   * Gets the change detector item.
   * 
   * @return the change detector item
   */
  public ChangeDetector getChangeDetector();

  /**
   * Sets the change detector item.
   * 
   * @param cd
   *          the change detector item
   */
  public void setChangeDetector(ChangeDetector cd);

}
