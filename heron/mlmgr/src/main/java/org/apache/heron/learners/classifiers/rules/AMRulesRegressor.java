package org.apache.samoa.learners.classifiers.rules;

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

import com.google.common.collect.ImmutableSet;

import java.util.Set;

import org.apache.samoa.core.Processor;
import org.apache.samoa.instances.Instances;
import org.apache.samoa.learners.RegressionLearner;
import org.apache.samoa.learners.classifiers.rules.centralized.AMRulesRegressorProcessor;
import org.apache.samoa.moa.classifiers.rules.core.attributeclassobservers.FIMTDDNumericAttributeClassLimitObserver;
import org.apache.samoa.moa.classifiers.rules.core.voting.ErrorWeightedVote;
import org.apache.samoa.topology.Stream;
import org.apache.samoa.topology.TopologyBuilder;

import com.github.javacliparser.Configurable;
import com.github.javacliparser.ClassOption;
import com.github.javacliparser.FlagOption;
import com.github.javacliparser.FloatOption;
import com.github.javacliparser.IntOption;
import com.github.javacliparser.MultiChoiceOption;

/**
 * AMRules Regressor is the task for the serialized implementation of AMRules algorithm for regression rule. It is
 * adapted to SAMOA from the implementation of AMRules in MOA.
 * 
 * @author Anh Thu Vu
 * 
 */

public class AMRulesRegressor implements RegressionLearner, Configurable {

  /**
	 * 
	 */
  private static final long serialVersionUID = 1L;

  // Options
  public FloatOption splitConfidenceOption = new FloatOption(
      "splitConfidence",
      'c',
      "Hoeffding Bound Parameter. The allowable error in split decision, values closer to 0 will take longer to decide.",
      0.0000001, 0.0, 1.0);

  public FloatOption tieThresholdOption = new FloatOption("tieThreshold",
      't', "Hoeffding Bound Parameter. Threshold below which a split will be forced to break ties.",
      0.05, 0.0, 1.0);

  public IntOption gracePeriodOption = new IntOption("gracePeriod",
      'g', "Hoeffding Bound Parameter. The number of instances a leaf should observe between split attempts.",
      200, 1, Integer.MAX_VALUE);

  public FlagOption DriftDetectionOption = new FlagOption("DoNotDetectChanges", 'H',
      "Drift Detection. Page-Hinkley.");

  public FloatOption pageHinckleyAlphaOption = new FloatOption(
      "pageHinckleyAlpha",
      'a',
      "The alpha value to use in the Page Hinckley change detection tests.",
      0.005, 0.0, 1.0);

  public IntOption pageHinckleyThresholdOption = new IntOption(
      "pageHinckleyThreshold",
      'l',
      "The threshold value (Lambda) to be used in the Page Hinckley change detection tests.",
      35, 0, Integer.MAX_VALUE);

  public FlagOption noAnomalyDetectionOption = new FlagOption("noAnomalyDetection", 'A',
      "Disable anomaly Detection.");

  public FloatOption multivariateAnomalyProbabilityThresholdOption = new FloatOption(
      "multivariateAnomalyProbabilityThresholdd",
      'm',
      "Multivariate anomaly threshold value.",
      0.99, 0.0, 1.0);

  public FloatOption univariateAnomalyProbabilityThresholdOption = new FloatOption(
      "univariateAnomalyprobabilityThreshold",
      'u',
      "Univariate anomaly threshold value.",
      0.10, 0.0, 1.0);

  public IntOption anomalyNumInstThresholdOption = new IntOption(
      "anomalyThreshold",
      'n',
      "The threshold value of anomalies to be used in the anomaly detection.",
      30, 0, Integer.MAX_VALUE); // num minimum of instances to detect anomalies anomalies. 15.

  public FlagOption unorderedRulesOption = new FlagOption("setUnorderedRulesOn", 'U',
      "unorderedRules.");

  public ClassOption numericObserverOption = new ClassOption("numericObserver",
      'z', "Numeric observer.",
      FIMTDDNumericAttributeClassLimitObserver.class,
      "FIMTDDNumericAttributeClassLimitObserver");

  public MultiChoiceOption predictionFunctionOption = new MultiChoiceOption(
      "predictionFunctionOption", 'P', "The prediction function to use.", new String[] {
          "Adaptative", "Perceptron", "Target Mean" }, new String[] {
          "Adaptative", "Perceptron", "Target Mean" }, 0);

  public FlagOption constantLearningRatioDecayOption = new FlagOption(
      "learningRatio_Decay_set_constant", 'd',
      "Learning Ratio Decay in Perceptron set to be constant. (The next parameter).");

  public FloatOption learningRatioOption = new FloatOption(
      "learningRatio", 's',
      "Constante Learning Ratio to use for training the Perceptrons in the leaves.", 0.025);

  public ClassOption votingTypeOption = new ClassOption("votingType",
      'V', "Voting Type.",
      ErrorWeightedVote.class,
      "InverseErrorWeightedVote");

  // Processor
  private AMRulesRegressorProcessor processor;

  // Stream
  private Stream resultStream;

  @Override
  public void init(TopologyBuilder topologyBuilder, Instances dataset, int parallelism) {
    this.processor = new AMRulesRegressorProcessor.Builder(dataset)
        .threshold(pageHinckleyThresholdOption.getValue())
        .alpha(pageHinckleyAlphaOption.getValue())
        .changeDetection(this.DriftDetectionOption.isSet())
        .predictionFunction(predictionFunctionOption.getChosenIndex())
        .constantLearningRatioDecay(constantLearningRatioDecayOption.isSet())
        .learningRatio(learningRatioOption.getValue())
        .splitConfidence(splitConfidenceOption.getValue())
        .tieThreshold(tieThresholdOption.getValue())
        .gracePeriod(gracePeriodOption.getValue())
        .noAnomalyDetection(noAnomalyDetectionOption.isSet())
        .multivariateAnomalyProbabilityThreshold(multivariateAnomalyProbabilityThresholdOption.getValue())
        .univariateAnomalyProbabilityThreshold(univariateAnomalyProbabilityThresholdOption.getValue())
        .anomalyNumberOfInstancesThreshold(anomalyNumInstThresholdOption.getValue())
        .unorderedRules(unorderedRulesOption.isSet())
        .numericObserver((FIMTDDNumericAttributeClassLimitObserver) numericObserverOption.getValue())
        .voteType((ErrorWeightedVote) votingTypeOption.getValue())
        .build();

    topologyBuilder.addProcessor(processor, parallelism);

    this.resultStream = topologyBuilder.createStream(processor);
    this.processor.setResultStream(resultStream);
  }

  @Override
  public Processor getInputProcessor() {
    return processor;
  }

  @Override
  public Set<Stream> getResultStreams() {
    return ImmutableSet.of(this.resultStream);
  }
}
