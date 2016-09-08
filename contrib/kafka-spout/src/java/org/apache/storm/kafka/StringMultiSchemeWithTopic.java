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
package org.apache.storm.kafka;

import java.util.Collections;
import java.util.List;

import org.apache.storm.spout.MultiScheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class StringMultiSchemeWithTopic
    implements MultiScheme {
  public static final String STRING_SCHEME_KEY = "str";

  public static final String TOPIC_KEY = "topic";
  private static final long serialVersionUID = 1985667152241541458L;

  @Override
  public Iterable<List<Object>> deserialize(byte[] bytes) {
    throw new UnsupportedOperationException();
  }

  public Iterable<List<Object>> deserializeWithTopic(String topic, byte[] bytes) {
    List<Object> items = new Values(StringScheme.deserializeString(bytes), topic);
    return Collections.singletonList(items);
  }

  public Fields getOutputFields() {
    return new Fields(STRING_SCHEME_KEY, TOPIC_KEY);
  }
}
