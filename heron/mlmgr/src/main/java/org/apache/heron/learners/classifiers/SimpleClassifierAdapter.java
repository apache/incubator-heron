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
import org.apache.samoa.instances.Instance;
import org.apache.samoa.instances.Instances;
import org.apache.samoa.instances.InstancesHeader;
import org.apache.samoa.moa.classifiers.functions.MajorityClass;

import com.github.javacliparser.ClassOption;
import com.github.javacliparser.Configurable;

/**
 * 
 * Base class for adapting external classifiers.
 * 
 */
public class SimpleClassifierAdapter implements LocalLearner, Configurable {

  /**
     *
     */
  private static final long serialVersionUID = 4372366401338704353L;

  public ClassOption learnerOption = new ClassOption("learner", 'l',
      "Classifier to train.", org.apache.samoa.moa.classifiers.Classifier.class, MajorityClass.class.getName());
  /**
   * The learner.
   */
  protected org.apache.samoa.moa.classifiers.Classifier learner;

  /**
   * The is init.
   */
  protected Boolean isInit;

  /**
   * The dataset.
   */
  protected Instances dataset;

  @Override
  public void setDataset(Instances dataset) {
    this.dataset = dataset;
  }

  /**
   * Instantiates a new learner.
   * 
   * @param learner
   *          the learner
   * @param dataset
   *          the dataset
   */
  public SimpleClassifierAdapter(org.apache.samoa.moa.classifiers.Classifier learner, Instances dataset) {
    this.learner = learner.copy();
    this.isInit = false;
    this.dataset = dataset;
  }

  /**
   * Instantiates a new learner.
   * 
   */
  public SimpleClassifierAdapter() {
    this.learner = ((org.apache.samoa.moa.classifiers.Classifier) this.learnerOption.getValue()).copy();
    this.isInit = false;
  }

  /**
   * Creates a new learner object.
   * 
   * @return the learner
   */
  @Override
  public SimpleClassifierAdapter create() {
    SimpleClassifierAdapter l = new SimpleClassifierAdapter(learner, dataset);
    if (dataset == null) {
      System.out.println("dataset null while creating");
    }
    return l;
  }

  /**
   * Trains this classifier incrementally using the given instance.
   * 
   * @param inst
   *          the instance to be used for training
   */
  @Override
  public void trainOnInstance(Instance inst) {
    if (!this.isInit) {
      this.isInit = true;
      InstancesHeader instances = new InstancesHeader(dataset);
      this.learner.setModelContext(instances);
      this.learner.prepareForUse();
    }
    if (inst.weight() > 0) {
      inst.setDataset(dataset);
      learner.trainOnInstance(inst);
    }
  }

  /**
   * Predicts the class memberships for a given instance. If an instance is unclassified, the returned array elements
   * must be all zero.
   * 
   * @param inst
   *          the instance to be classified
   * @return an array containing the estimated membership probabilities of the test instance in each class
   */
  @Override
  public double[] getVotesForInstance(Instance inst) {
    double[] ret;
    inst.setDataset(dataset);
    if (!this.isInit) {
      ret = new double[dataset.numClasses()];
    } else {
      ret = learner.getVotesForInstance(inst);
    }
    return ret;
  }

  /**
   * Resets this classifier. It must be similar to starting a new classifier from scratch.
   * 
   */
  @Override
  public void resetLearning() {
    learner.resetLearning();
  }

}
