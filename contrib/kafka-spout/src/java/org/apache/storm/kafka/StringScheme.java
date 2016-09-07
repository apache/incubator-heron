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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class StringScheme implements Scheme {
  private static final Charset UTF8_CHARSET = StandardCharsets.UTF_8;
  public static final String STRING_SCHEME_KEY = "str";
  private static final long serialVersionUID = -1806461122261145598L;

  public List<Object> deserialize(byte[] bytes) {
    return new Values(deserializeString(bytes));
  }

  public static String deserializeString(byte[] bytes) {
    return new String(bytes, UTF8_CHARSET);
  }

  public Fields getOutputFields() {
    return new Fields(STRING_SCHEME_KEY);
  }
}
