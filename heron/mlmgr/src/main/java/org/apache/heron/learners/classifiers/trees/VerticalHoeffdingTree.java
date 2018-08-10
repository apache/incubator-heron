package org.apache.samoa.learners.classifiers.trees;

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
import org.apache.samoa.learners.AdaptiveLearner;
import org.apache.samoa.learners.ClassificationLearner;
import org.apache.samoa.moa.classifiers.core.attributeclassobservers.AttributeClassObserver;
import org.apache.samoa.moa.classifiers.core.attributeclassobservers.DiscreteAttributeClassObserver;
import org.apache.samoa.moa.classifiers.core.attributeclassobservers.NumericAttributeClassObserver;
import org.apache.samoa.moa.classifiers.core.driftdetection.ChangeDetector;
import org.apache.samoa.moa.classifiers.core.splitcriteria.SplitCriterion;
import org.apache.samoa.topology.Stream;
import org.apache.samoa.topology.TopologyBuilder;

import com.github.javacliparser.ClassOption;
import com.github.javacliparser.Configurable;
import com.github.javacliparser.FlagOption;
import com.github.javacliparser.FloatOption;
import com.github.javacliparser.IntOption;
import com.google.common.collect.ImmutableSet;

/**
 * Vertical Hoeffding Tree.
 * <p/>
 * Vertical Hoeffding Tree (VHT) classifier is a distributed classifier that utilizes vertical parallelism on top of
 * Very Fast Decision Tree (VFDT) classifier.
 * 
 * @author Arinto Murdopo
 */
public final class VerticalHoeffdingTree implements ClassificationLearner, AdaptiveLearner, Configurable {

  private static final long serialVersionUID = -4937416312929984057L;

  public ClassOption numericEstimatorOption = new ClassOption("numericEstimator",
      'n', "Numeric estimator to use.", NumericAttributeClassObserver.class,
      "GaussianNumericAttributeClassObserver");

  public ClassOption nominalEstimatorOption = new ClassOption("nominalEstimator",
      'd', "Nominal estimator to use.", DiscreteAttributeClassObserver.class,
      "NominalAttributeClassObserver");

  public ClassOption splitCriterionOption = new ClassOption("splitCriterion",
      's', "Split criterion to use.", SplitCriterion.class,
      "InfoGainSplitCriterion");

  public FloatOption splitConfidenceOption = new FloatOption(
      "splitConfidence",
      'c',
      "The allowable error in split decision, values closer to 0 will take longer to decide.",
      0.0000001, 0.0, 1.0);

  public FloatOption tieThresholdOption = new FloatOption("tieThreshold",
      't', "Threshold below which a split will be forced to break ties.",
      0.05, 0.0, 1.0);

  public IntOption gracePeriodOption = new IntOption(
      "gracePeriod",
      'g',
      "The number of instances a leaf should observe between split attempts.",
      200, 0, Integer.MAX_VALUE);

  public IntOption parallelismHintOption = new IntOption(
      "parallelismHint",
      'p',
      "The number of local statistics PI to do distributed computation",
      1, 1, Integer.MAX_VALUE);

  public IntOption timeOutOption = new IntOption(
      "timeOut",
      'o',
      "The duration to wait all distributed computation results from local statistics PI",
      30, 1, Integer.MAX_VALUE);

  public FlagOption binarySplitsOption = new FlagOption("binarySplits", 'b',
      "Only allow binary splits.");

  private Stream resultStream;

  private FilterProcessor filterProc;

  @Override
  public void init(TopologyBuilder topologyBuilder, Instances dataset, int parallelism) {

    this.filterProc = new FilterProcessor.Builder(dataset)
        .build();
    topologyBuilder.addProcessor(filterProc, parallelism);

    Stream filterStream = topologyBuilder.createStream(filterProc);
    this.filterProc.setOutputStream(filterStream);

    ModelAggregatorProcessor modelAggrProc = new ModelAggregatorProcessor.Builder(dataset)
        .splitCriterion((SplitCriterion) this.splitCriterionOption.getValue())
        .splitConfidence(splitConfidenceOption.getValue())
        .tieThreshold(tieThresholdOption.getValue())
        .gracePeriod(gracePeriodOption.getValue())
        .parallelismHint(parallelismHintOption.getValue())
        .timeOut(timeOutOption.getValue())
        .changeDetector(this.getChangeDetector())
        .build();

    topologyBuilder.addProcessor(modelAggrProc, parallelism);

    topologyBuilder.connectInputShuffleStream(filterStream, modelAggrProc);

    this.resultStream = topologyBuilder.createStream(modelAggrProc);
    modelAggrProc.setResultStream(resultStream);

    Stream attributeStream = topologyBuilder.createStream(modelAggrProc);
    modelAggrProc.setAttributeStream(attributeStream);

    Stream controlStream = topologyBuilder.createStream(modelAggrProc);
    modelAggrProc.setControlStream(controlStream);

    LocalStatisticsProcessor locStatProc = new LocalStatisticsProcessor.Builder()
        .splitCriterion((SplitCriterion) this.splitCriterionOption.getValue())
        .binarySplit(binarySplitsOption.isSet())
        .nominalClassObserver((AttributeClassObserver) this.nominalEstimatorOption.getValue())
        .numericClassObserver((AttributeClassObserver) this.numericEstimatorOption.getValue())
        .build();

    topologyBuilder.addProcessor(locStatProc, parallelismHintOption.getValue());
    topologyBuilder.connectInputKeyStream(attributeStream, locStatProc);
    topologyBuilder.connectInputAllStream(controlStream, locStatProc);

    Stream computeStream = topologyBuilder.createStream(locStatProc);

    locStatProc.setComputationResultStream(computeStream);
    topologyBuilder.connectInputAllStream(computeStream, modelAggrProc);
  }

  @Override
  public Processor getInputProcessor() {
    return this.filterProc;
  }

  @Override
  public Set<Stream> getResultStreams() {
    return ImmutableSet.of(this.resultStream);
  }

  protected ChangeDetector changeDetector;

  @Override
  public ChangeDetector getChangeDetector() {
    return this.changeDetector;
  }

  @Override
  public void setChangeDetector(ChangeDetector cd) {
    this.changeDetector = cd;
  }
  
  static class LearningNodeIdGenerator {

    // TODO: add code to warn user of when value reaches Long.MAX_VALUES
    private static long id = 0;

    static synchronized long generate() {
      return id++;
    }
  }
}
