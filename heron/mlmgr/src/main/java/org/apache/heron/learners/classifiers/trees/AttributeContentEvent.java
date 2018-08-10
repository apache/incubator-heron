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

import org.apache.samoa.core.ContentEvent;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Attribute Content Event represents the instances that split vertically based on their attribute
 * 
 * @author Arinto Murdopo
 * 
 */
public final class AttributeContentEvent implements ContentEvent {

  private static final long serialVersionUID = 6652815649846676832L;

  private final long learningNodeId;
  private final int obsIndex;
  private final double attrVal;
  private final int classVal;
  private final double weight;
  private final transient String key;
  private final boolean isNominal;

  public AttributeContentEvent() {
    learningNodeId = -1;
    obsIndex = -1;
    attrVal = 0.0;
    classVal = -1;
    weight = 0.0;
    key = "";
    isNominal = true;
  }

  private AttributeContentEvent(Builder builder) {
    this.learningNodeId = builder.learningNodeId;
    this.obsIndex = builder.obsIndex;
    this.attrVal = builder.attrVal;
    this.classVal = builder.classVal;
    this.weight = builder.weight;
    this.isNominal = builder.isNominal;
    this.key = builder.key;
  }

  @Override
  public String getKey() {
    return this.key;
  }

  @Override
  public void setKey(String str) {
    // do nothing, maybe useful when we want to reuse the object for
    // serialization/deserialization purpose
  }

  @Override
  public boolean isLastEvent() {
    return false;
  }

  long getLearningNodeId() {
    return this.learningNodeId;
  }

  int getObsIndex() {
    return this.obsIndex;
  }

  int getClassVal() {
    return this.classVal;
  }

  double getAttrVal() {
    return this.attrVal;
  }

  double getWeight() {
    return this.weight;
  }

  boolean isNominal() {
    return this.isNominal;
  }

  static final class Builder {

    // required parameters
    private final long learningNodeId;
    private final int obsIndex;
    private final String key;

    // optional parameters
    private double attrVal = 0.0;
    private int classVal = 0;
    private double weight = 0.0;
    private boolean isNominal = false;

    Builder(long id, int obsIndex, String key) {
      this.learningNodeId = id;
      this.obsIndex = obsIndex;
      this.key = key;
    }

    private Builder(long id, int obsIndex) {
      this.learningNodeId = id;
      this.obsIndex = obsIndex;
      this.key = "";
    }

    Builder attrValue(double val) {
      this.attrVal = val;
      return this;
    }

    Builder classValue(int val) {
      this.classVal = val;
      return this;
    }

    Builder weight(double val) {
      this.weight = val;
      return this;
    }

    Builder isNominal(boolean val) {
      this.isNominal = val;
      return this;
    }

    AttributeContentEvent build() {
      return new AttributeContentEvent(this);
    }
  }

  /**
   * The Kryo serializer class for AttributeContentEvent when executing on top of Storm. This class allow us to change
   * the precision of the statistics.
   * 
   * @author Arinto Murdopo
   * 
   */
  public static final class AttributeCESerializer extends Serializer<AttributeContentEvent> {

    private static double PRECISION = 1000000.0;

    @Override
    public void write(Kryo kryo, Output output, AttributeContentEvent event) {
      output.writeLong(event.learningNodeId, true);
      output.writeInt(event.obsIndex, true);
      output.writeDouble(event.attrVal, PRECISION, true);
      output.writeInt(event.classVal, true);
      output.writeDouble(event.weight, PRECISION, true);
      output.writeBoolean(event.isNominal);
    }

    @Override
    public AttributeContentEvent read(Kryo kryo, Input input,
        Class<AttributeContentEvent> type) {
      AttributeContentEvent ace = new AttributeContentEvent.Builder(input.readLong(true), input.readInt(true))
          .attrValue(input.readDouble(PRECISION, true))
          .classValue(input.readInt(true))
          .weight(input.readDouble(PRECISION, true))
          .isNominal(input.readBoolean())
          .build();
      return ace;
    }
  }

  /**
   * The Kryo serializer class for AttributeContentEvent when executing on top of Storm with full precision of the
   * statistics.
   * 
   * @author Arinto Murdopo
   * 
   */
  public static final class AttributeCEFullPrecSerializer extends Serializer<AttributeContentEvent> {

    @Override
    public void write(Kryo kryo, Output output, AttributeContentEvent event) {
      output.writeLong(event.learningNodeId, true);
      output.writeInt(event.obsIndex, true);
      output.writeDouble(event.attrVal);
      output.writeInt(event.classVal, true);
      output.writeDouble(event.weight);
      output.writeBoolean(event.isNominal);
    }

    @Override
    public AttributeContentEvent read(Kryo kryo, Input input,
        Class<AttributeContentEvent> type) {
      AttributeContentEvent ace = new AttributeContentEvent.Builder(input.readLong(true), input.readInt(true))
          .attrValue(input.readDouble())
          .classValue(input.readInt(true))
          .weight(input.readDouble())
          .isNominal(input.readBoolean())
          .build();
      return ace;
    }

  }
}
