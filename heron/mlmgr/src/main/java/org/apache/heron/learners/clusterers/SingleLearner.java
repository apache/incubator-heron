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

import com.google.common.collect.ImmutableSet;

import java.util.Set;

import org.apache.samoa.core.Processor;
import org.apache.samoa.instances.Instances;
import org.apache.samoa.learners.Learner;
import org.apache.samoa.topology.Stream;
import org.apache.samoa.topology.TopologyBuilder;

import com.github.javacliparser.ClassOption;
import com.github.javacliparser.Configurable;

/**
 * 
 * Learner that contain a single learner.
 * 
 */
public final class SingleLearner implements Learner, Configurable {

  private static final long serialVersionUID = 684111382631697031L;

  private LocalClustererProcessor learnerP;

  private Stream resultStream;

  private Instances dataset;

  public ClassOption learnerOption = new ClassOption("learner", 'l',
      "Learner to train.", LocalClustererAdapter.class, ClustreamClustererAdapter.class.getName());

  private TopologyBuilder builder;

  private int parallelism;

  @Override
  public void init(TopologyBuilder builder, Instances dataset, int parallelism) {
    this.builder = builder;
    this.dataset = dataset;
    this.parallelism = parallelism;
    this.setLayout();
  }

  protected void setLayout() {
    learnerP = new LocalClustererProcessor();
    LocalClustererAdapter learner = (LocalClustererAdapter) this.learnerOption.getValue();
    learner.setDataset(this.dataset);
    learnerP.setLearner(learner);

    this.builder.addProcessor(learnerP, this.parallelism);
    resultStream = this.builder.createStream(learnerP);

    learnerP.setOutputStream(resultStream);
  }

  /*
   * (non-Javadoc)
   * 
   * @see samoa.classifiers.Classifier#getInputProcessingItem()
   */
  @Override
  public Processor getInputProcessor() {
    return learnerP;
  }

  /*
   * (non-Javadoc)
   * 
   * @see samoa.learners.Learner#getResultStreams()
   */
  @Override
  public Set<Stream> getResultStreams() {
    Set<Stream> streams = ImmutableSet.of(this.resultStream);
    return streams;
  }
}
