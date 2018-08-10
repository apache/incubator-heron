/*
 *    TargetMean.java
 *    Copyright (C) 2014 - 2015 Apache Software Foundation
 *    @author  J. Duarte, A. Bifet, J. Gama
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *    
 *    
 */
package org.apache.samoa.learners.classifiers.rules.common;

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
 * Prediction scheme using TargetMean:
 * TargetMean - Returns the mean of the target variable of the training instances
 * 
 * @author Joao Duarte 
 * 
 *  */

import org.apache.samoa.instances.Instance;
import org.apache.samoa.moa.classifiers.AbstractClassifier;
import org.apache.samoa.moa.classifiers.Regressor;
import org.apache.samoa.moa.core.Measurement;
import org.apache.samoa.moa.core.StringUtils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.github.javacliparser.FloatOption;

public class TargetMean extends AbstractClassifier implements Regressor {

  /**
	 * 
	 */
  protected long n;
  protected double sum;
  protected double errorSum;
  protected double nError;
  private double fadingErrorFactor;

  private static final long serialVersionUID = 7152547322803559115L;

  public FloatOption fadingErrorFactorOption = new FloatOption(
      "fadingErrorFactor", 'e',
      "Fading error factor for the TargetMean accumulated error", 0.99, 0, 1);

  @Override
  public boolean isRandomizable() {
    return false;
  }

  @Override
  public double[] getVotesForInstance(Instance inst) {
    return getVotesForInstance();
  }

  public double[] getVotesForInstance() {
    double[] currentMean = new double[1];
    if (n > 0)
      currentMean[0] = sum / n;
    else
      currentMean[0] = 0;
    return currentMean;
  }

  @Override
  public void resetLearningImpl() {
    sum = 0;
    n = 0;
    errorSum = Double.MAX_VALUE;
    nError = 0;
  }

  @Override
  public void trainOnInstanceImpl(Instance inst) {
    updateAccumulatedError(inst);
    ++this.n;
    this.sum += inst.classValue();
  }

  protected void updateAccumulatedError(Instance inst) {
    double mean = 0;
    nError = 1 + fadingErrorFactor * nError;
    if (n > 0)
      mean = sum / n;
    errorSum = Math.abs(inst.classValue() - mean) + fadingErrorFactor * errorSum;
  }

  @Override
  protected Measurement[] getModelMeasurementsImpl() {
    return null;
  }

  @Override
  public void getModelDescription(StringBuilder out, int indent) {
    StringUtils.appendIndented(out, indent, "Current Mean: " + this.sum / this.n);
    StringUtils.appendNewline(out);

  }

  /*
   * JD Resets the learner but initializes with a starting point
   */
  public void reset(double currentMean, long numberOfInstances) {
    this.sum = currentMean * numberOfInstances;
    this.n = numberOfInstances;
    this.resetError();
  }

  /*
   * JD Resets the learner but initializes with a starting point
   */
  public double getCurrentError() {
    if (this.nError > 0)
      return this.errorSum / this.nError;
    else
      return Double.MAX_VALUE;
  }

  public TargetMean(TargetMean t) {
    super();
    this.n = t.n;
    this.sum = t.sum;
    this.errorSum = t.errorSum;
    this.nError = t.nError;
    this.fadingErrorFactor = t.fadingErrorFactor;
    this.fadingErrorFactorOption = t.fadingErrorFactorOption;
  }

  public TargetMean(TargetMeanData td) {
    this();
    this.n = td.n;
    this.sum = td.sum;
    this.errorSum = td.errorSum;
    this.nError = td.nError;
    this.fadingErrorFactor = td.fadingErrorFactor;
    this.fadingErrorFactorOption.setValue(td.fadingErrorFactorOptionValue);
  }

  public TargetMean() {
    super();
    fadingErrorFactor = fadingErrorFactorOption.getValue();
  }

  public void resetError() {
    this.errorSum = 0;
    this.nError = 0;
  }

  public static class TargetMeanData {
    private long n;
    private double sum;
    private double errorSum;
    private double nError;
    private double fadingErrorFactor;
    private double fadingErrorFactorOptionValue;

    public TargetMeanData() {

    }

    public TargetMeanData(TargetMean tm) {
      this.n = tm.n;
      this.sum = tm.sum;
      this.errorSum = tm.errorSum;
      this.nError = tm.nError;
      this.fadingErrorFactor = tm.fadingErrorFactor;
      if (tm.fadingErrorFactorOption != null)
        this.fadingErrorFactorOptionValue = tm.fadingErrorFactorOption.getValue();
      else
        this.fadingErrorFactorOptionValue = 0.99;
    }

    public TargetMean build() {
      return new TargetMean(this);
    }
  }

  public static final class TargetMeanSerializer extends Serializer<TargetMean> {

    @Override
    public void write(Kryo kryo, Output output, TargetMean t) {
      kryo.writeObjectOrNull(output, new TargetMeanData(t), TargetMeanData.class);
    }

    @Override
    public TargetMean read(Kryo kryo, Input input, Class<TargetMean> type) {
      TargetMeanData data = kryo.readObjectOrNull(input, TargetMeanData.class);
      return data.build();
    }
  }
}
