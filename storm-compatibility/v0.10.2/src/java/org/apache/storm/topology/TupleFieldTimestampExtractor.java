/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.topology;

import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TimestampExtractor;

/**
 * A {@link TimestampExtractor} that extracts timestamp from a specific field in the tuple.
 */
public final class TupleFieldTimestampExtractor implements TimestampExtractor {
  private static final long serialVersionUID = 5212225232807940572L;
  private final String fieldName;

  private TupleFieldTimestampExtractor(String fieldName) {
    this.fieldName = fieldName;
  }

  @Override
  public long extractTimestamp(Tuple tuple) {
    return tuple.getLongByField(fieldName);
  }

  public static TupleFieldTimestampExtractor of(String fieldName) {
    return new TupleFieldTimestampExtractor(fieldName);
  }

  @Override
  public String toString() {
    return "TupleFieldTimestampExtractor{" + "fieldName='" + fieldName + '\'' + '}';
  }
}
