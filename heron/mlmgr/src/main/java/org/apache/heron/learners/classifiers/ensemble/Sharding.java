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

import java.util.Set;

import org.apache.samoa.core.Processor;
import org.apache.samoa.instances.Instances;
import org.apache.samoa.learners.Learner;
import org.apache.samoa.learners.classifiers.trees.VerticalHoeffdingTree;
import org.apache.samoa.topology.Stream;
import org.apache.samoa.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.javacliparser.ClassOption;
import com.github.javacliparser.Configurable;
import com.github.javacliparser.IntOption;
import com.google.common.collect.ImmutableSet;

/**
 * Simple sharding meta-classifier. It trains an ensemble of learners by shuffling the training stream among them, so
 * that each learner is completely independent from each other.
 */
public class Sharding implements Learner, Configurable {

  private static final long serialVersionUID = -2971850264864952099L;
  private static final Logger logger = LoggerFactory.getLogger(Sharding.class);

  /** The base learner class. */
  public ClassOption baseLearnerOption = new ClassOption("baseLearner", 'l',
      "Classifier to train.", Learner.class, VerticalHoeffdingTree.class.getName());

  /** The ensemble size option. */
  public IntOption ensembleSizeOption = new IntOption("ensembleSize", 's',
      "The number of models in the bag.", 10, 1, Integer.MAX_VALUE);

  /** The distributor processor. */
  private ShardingDistributorProcessor distributor;

  /** The input streams for the ensemble, one per member. */
  private Stream[] ensembleStreams;

  /** The result stream. */
  protected Stream resultStream;

  /** The dataset. */
  private Instances dataset;

  protected Learner[] ensemble;

  /**
   * Sets the layout.
   */
  protected void setLayout() {

    int ensembleSize = this.ensembleSizeOption.getValue();

    distributor = new ShardingDistributorProcessor();
    distributor.setEnsembleSize(ensembleSize);
    this.builder.addProcessor(distributor, 1);

    // instantiate classifier
    ensemble = new Learner[ensembleSize];
    for (int i = 0; i < ensembleSize; i++) {
      try {
        ensemble[i] = (Learner) ClassOption.createObject(baseLearnerOption.getValueAsCLIString(),
            baseLearnerOption.getRequiredType());
      } catch (Exception e) {
        logger.error("Unable to create members of the ensemble. Please check your CLI parameters");
        e.printStackTrace();
        throw new IllegalArgumentException(e);
      }
      ensemble[i].init(builder, this.dataset, 1); // sequential
    }

    PredictionCombinerProcessor predictionCombiner = new PredictionCombinerProcessor();
    predictionCombiner.setEnsembleSize(ensembleSize);
    this.builder.addProcessor(predictionCombiner, 1);

    // Streams
    resultStream = this.builder.createStream(predictionCombiner);
    predictionCombiner.setOutputStream(resultStream);

    for (Learner member : ensemble) {
      for (Stream subResultStream : member.getResultStreams()) { // a learner can have multiple output streams
        this.builder.connectInputKeyStream(subResultStream, predictionCombiner); // the key is the instance id to combine predictions
      }
    }

    ensembleStreams = new Stream[ensembleSize];
    for (int i = 0; i < ensembleSize; i++) {
      ensembleStreams[i] = builder.createStream(distributor);
      builder.connectInputShuffleStream(ensembleStreams[i], ensemble[i].getInputProcessor()); // connect streams one-to-one with ensemble members (the type of connection does not matter)
    }
    
    distributor.setOutputStreams(ensembleStreams);
  }

  /** The builder. */
  private TopologyBuilder builder;

  @Override
  public void init(TopologyBuilder builder, Instances dataset, int parallelism) {
    this.builder = builder;
    this.dataset = dataset;
    this.setLayout();
  }

  @Override
  public Processor getInputProcessor() {
    return distributor;
  }

  /*
   * (non-Javadoc)
   * 
   * @see samoa.learners.Learner#getResultStreams()
   */
  @Override
  public Set<Stream> getResultStreams() {
    return ImmutableSet.of(this.resultStream);
  }
}
