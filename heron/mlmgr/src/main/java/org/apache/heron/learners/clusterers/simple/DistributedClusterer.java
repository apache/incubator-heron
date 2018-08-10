package org.apache.samoa.learners.clusterers.simple;

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

import com.google.common.collect.ImmutableSet;

import java.util.Set;

import org.apache.samoa.core.Processor;
import org.apache.samoa.instances.Instances;
import org.apache.samoa.learners.Learner;
import org.apache.samoa.learners.clusterers.*;
import org.apache.samoa.topology.ProcessingItem;
import org.apache.samoa.topology.Stream;
import org.apache.samoa.topology.TopologyBuilder;

import com.github.javacliparser.ClassOption;
import com.github.javacliparser.Configurable;
import com.github.javacliparser.IntOption;

/**
 * 
 * Learner that contain a single learner.
 * 
 */
public final class DistributedClusterer implements Learner, Configurable {

  private static final long serialVersionUID = 684111382631697031L;

  private Stream resultStream;

  private Instances dataset;

  public ClassOption learnerOption = new ClassOption("learner", 'l', "Clusterer to use.", LocalClustererAdapter.class,
      ClustreamClustererAdapter.class.getName());

  public IntOption paralellismOption = new IntOption("paralellismOption", 'P',
      "The paralellism level for concurrent processes", 2, 1, Integer.MAX_VALUE);

  private TopologyBuilder builder;

  // private ClusteringDistributorProcessor distributorP;
  private LocalClustererProcessor learnerP;

  // private Stream distributorToLocalStream;
  private Stream localToGlobalStream;

  // private int parallelism;

  @Override
  public void init(TopologyBuilder builder, Instances dataset, int parallelism) {
    this.builder = builder;
    this.dataset = dataset;
    // this.parallelism = parallelism;
    this.setLayout();
  }

  protected void setLayout() {
    // Distributor
    // distributorP = new ClusteringDistributorProcessor();
    // this.builder.addProcessor(distributorP, parallelism);
    // distributorToLocalStream = this.builder.createStream(distributorP);
    // distributorP.setOutputStream(distributorToLocalStream);
    // distributorToGlobalStream = this.builder.createStream(distributorP);

    // Local Clustering
    learnerP = new LocalClustererProcessor();
    LocalClustererAdapter learner = (LocalClustererAdapter) this.learnerOption.getValue();
    learner.setDataset(this.dataset);
    learnerP.setLearner(learner);
    builder.addProcessor(learnerP, this.paralellismOption.getValue());
    localToGlobalStream = this.builder.createStream(learnerP);
    learnerP.setOutputStream(localToGlobalStream);

    // Global Clustering
    LocalClustererProcessor globalClusteringCombinerP = new LocalClustererProcessor();
    LocalClustererAdapter globalLearner = (LocalClustererAdapter) this.learnerOption.getValue();
    globalLearner.setDataset(this.dataset);
    globalClusteringCombinerP.setLearner(learner);
    builder.addProcessor(globalClusteringCombinerP, 1);
    builder.connectInputAllStream(localToGlobalStream, globalClusteringCombinerP);

    // Output Stream
    resultStream = this.builder.createStream(globalClusteringCombinerP);
    globalClusteringCombinerP.setOutputStream(resultStream);
  }

  @Override
  public Processor getInputProcessor() {
    // return distributorP;
    return learnerP;
  }

  @Override
  public Set<Stream> getResultStreams() {
    Set<Stream> streams = ImmutableSet.of(this.resultStream);
    return streams;
  }
}
