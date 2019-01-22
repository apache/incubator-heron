/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.heron.api.tuple;

import java.io.Serializable;
import java.util.List;

import org.apache.heron.api.generated.TopologyAPI;

/**
 * The tuple is the main data structure in Heron. A tuple is a named list of values,
 * where each value can be any type. Tuples are dynamically typed -- the types of the fields
 * do not need to be declared. Tuples have helper methods like getInteger and getString
 * to get field values without having to cast the result.
 * <p>
 * Heron needs to know how to serialize all the values in a tuple. By default, Heron
 * knows how to serialize the primitive types, strings, and byte arrays. If you want to
 * use another type, you'll need to implement and register a serializer for that type.
 * @see <a href="https://storm.apache.org/documentation/Serialization.html">Storm serialization</a>
 */
public interface Tuple extends Serializable {

  /**
   * Returns the number of fields in this tuple.
   */
  int size();

  /**
   * Returns the position of the specified field in this tuple.
   */
  int fieldIndex(String field);

  /**
   * Returns true if this tuple contains the specified name of the field.
   */
  boolean contains(String field);

  /**
   * Gets the field at position i in the tuple. Returns object since tuples are dynamically typed.
   */
  Object getValue(int i);

  /**
   * Returns the String at position i in the tuple. If that field is not a String,
   * you will get a runtime error.
   */
  String getString(int i);

  /**
   * Returns the Integer at position i in the tuple. If that field is not an Integer,
   * you will get a runtime error.
   */
  Integer getInteger(int i);

  /**
   * Returns the Long at position i in the tuple. If that field is not a Long,
   * you will get a runtime error.
   */
  Long getLong(int i);

  /**
   * Returns the Boolean at position i in the tuple. If that field is not a Boolean,
   * you will get a runtime error.
   */
  Boolean getBoolean(int i);

  /**
   * Returns the Short at position i in the tuple. If that field is not a Short,
   * you will get a runtime error.
   */
  Short getShort(int i);

  /**
   * Returns the Byte at position i in the tuple. If that field is not a Byte,
   * you will get a runtime error.
   */
  Byte getByte(int i);

  /**
   * Returns the Double at position i in the tuple. If that field is not a Double,
   * you will get a runtime error.
   */
  Double getDouble(int i);

  /**
   * Returns the Float at position i in the tuple. If that field is not a Float,
   * you will get a runtime error.
   */
  Float getFloat(int i);

  /**
   * Returns the byte array at position i in the tuple. If that field is not a byte array,
   * you will get a runtime error.
   */
  byte[] getBinary(int i);


  Object getValueByField(String field);

  String getStringByField(String field);

  Integer getIntegerByField(String field);

  Long getLongByField(String field);

  Boolean getBooleanByField(String field);

  Short getShortByField(String field);

  Byte getByteByField(String field);

  Double getDoubleByField(String field);

  Float getFloatByField(String field);

  byte[] getBinaryByField(String field);

  /**
   * Gets all the values in this tuple.
   */
  List<Object> getValues();

  /**
   * Gets the names of the fields in this tuple.
   */
  Fields getFields();

  /**
   * Returns a subset of the tuple based on the fields selector.
   */
  List<Object> select(Fields selector);

  /**
   * Returns the global stream id (component + stream) of this tuple.
   */

  TopologyAPI.StreamId getSourceGlobalStreamId();

  /**
   * Gets the id of the component that created this tuple.
   */
  String getSourceComponent();

  /**
   * Gets the id of the task that created this tuple.
   */
  int getSourceTask();

  /**
   * Gets the id of the stream that this tuple was emitted to.
   */
  String getSourceStreamId();

  /**
   * Gets the message id that associated with this tuple.
   */
    /*
    TODO:- does anyone use this
    public MessageId getMessageId();
    */

  /**
   * Resets the tuple values to null
   * TODO:- Is this needed
   */
  void resetValues();
}
