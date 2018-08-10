package org.apache.samoa.learners.clusterers;

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
import org.apache.samoa.moa.cluster.Clustering;
import org.apache.samoa.moa.clusterers.clustream.Clustream;

import com.github.javacliparser.ClassOption;
import com.github.javacliparser.Configurable;

/**
 * 
 * Base class for adapting Clustream clusterer.
 * 
 */
public class ClustreamClustererAdapter implements LocalClustererAdapter, Configurable {

  /**
     *
     */
  private static final long serialVersionUID = 4372366401338704353L;

  public ClassOption learnerOption = new ClassOption("learner", 'l',
      "Clusterer to train.", org.apache.samoa.moa.clusterers.Clusterer.class, Clustream.class.getName());
  /**
   * The learner.
   */
  protected org.apache.samoa.moa.clusterers.Clusterer learner;

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
  public ClustreamClustererAdapter(org.apache.samoa.moa.clusterers.Clusterer learner, Instances dataset) {
    this.learner = learner.copy();
    this.isInit = false;
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
  public ClustreamClustererAdapter() {
    this.learner = ((org.apache.samoa.moa.clusterers.Clusterer) this.learnerOption.getValue()).copy();
    this.isInit = false;
    // this.dataset = dataset;
  }

  /**
   * Creates a new learner object.
   * 
   * @return the learner
   */
  @Override
  public ClustreamClustererAdapter create() {
    ClustreamClustererAdapter l = new ClustreamClustererAdapter(learner, dataset);
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
    if (this.isInit == false) {
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
    if (this.isInit == false) {
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

  public boolean implementsMicroClusterer() {
    return this.learner.implementsMicroClusterer();
  }

  public Clustering getMicroClusteringResult() {
    return this.learner.getMicroClusteringResult();
  }

  public Instances getDataset() {
    return this.dataset;
  }

}
