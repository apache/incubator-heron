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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Compute content event is the message that is sent by Model Aggregator Processor to request Local Statistic PI to
 * start the local statistic calculation for splitting
 * 
 * @author Arinto Murdopo
 * 
 */
public final class ComputeContentEvent extends ControlContentEvent {

  private static final long serialVersionUID = 5590798490073395190L;

  private final double[] preSplitDist;
  private final long splitId;

  public ComputeContentEvent() {
    super(-1);
    preSplitDist = null;
    splitId = -1;
  }

  ComputeContentEvent(long splitId, long id, double[] preSplitDist) {
    super(id);
    // this.preSplitDist = Arrays.copyOf(preSplitDist, preSplitDist.length);
    this.preSplitDist = preSplitDist;
    this.splitId = splitId;
  }

  @Override
  LocStatControl getType() {
    return LocStatControl.COMPUTE;
  }

  double[] getPreSplitDist() {
    return this.preSplitDist;
  }

  long getSplitId() {
    return this.splitId;
  }

  /**
   * The Kryo serializer class for ComputeContentEevent when executing on top of Storm. This class allow us to change
   * the precision of the statistics.
   * 
   * @author Arinto Murdopo
   * 
   */
  public static final class ComputeCESerializer extends Serializer<ComputeContentEvent> {

    private static double PRECISION = 1000000.0;

    @Override
    public void write(Kryo kryo, Output output, ComputeContentEvent object) {
      output.writeLong(object.splitId, true);
      output.writeLong(object.learningNodeId, true);

      output.writeInt(object.preSplitDist.length, true);
      for (int i = 0; i < object.preSplitDist.length; i++) {
        output.writeDouble(object.preSplitDist[i], PRECISION, true);
      }
    }

    @Override
    public ComputeContentEvent read(Kryo kryo, Input input,
        Class<ComputeContentEvent> type) {
      long splitId = input.readLong(true);
      long learningNodeId = input.readLong(true);

      int dataLength = input.readInt(true);
      double[] preSplitDist = new double[dataLength];

      for (int i = 0; i < dataLength; i++) {
        preSplitDist[i] = input.readDouble(PRECISION, true);
      }

      return new ComputeContentEvent(splitId, learningNodeId, preSplitDist);
    }
  }

  /**
   * The Kryo serializer class for ComputeContentEevent when executing on top of Storm with full precision of the
   * statistics.
   * 
   * @author Arinto Murdopo
   * 
   */
  public static final class ComputeCEFullPrecSerializer extends Serializer<ComputeContentEvent> {

    @Override
    public void write(Kryo kryo, Output output, ComputeContentEvent object) {
      output.writeLong(object.splitId, true);
      output.writeLong(object.learningNodeId, true);

      output.writeInt(object.preSplitDist.length, true);
      for (int i = 0; i < object.preSplitDist.length; i++) {
        output.writeDouble(object.preSplitDist[i]);
      }
    }

    @Override
    public ComputeContentEvent read(Kryo kryo, Input input,
        Class<ComputeContentEvent> type) {
      long splitId = input.readLong(true);
      long learningNodeId = input.readLong(true);

      int dataLength = input.readInt(true);
      double[] preSplitDist = new double[dataLength];

      for (int i = 0; i < dataLength; i++) {
        preSplitDist[i] = input.readDouble();
      }

      return new ComputeContentEvent(splitId, learningNodeId, preSplitDist);
    }

  }

}
