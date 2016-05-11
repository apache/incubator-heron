/*
 * Copyright 2016 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.heron.spouts.kafka;

import org.json.simple.JSONObject;

import java.io.Serializable;
import java.util.List;

public abstract class FilterOperator implements Serializable {
  /** Config for Filter operator. It will be passed as storm config. The filter will be applied
   * to the spout 'spoutname'. Typically your topology has many kafka spouts, so set the value
   * of TOPOLOGY_FILTER_CONFIG to be an array of FilterConfig.*/
  public static class FilterConfig {
    public String classname;
    public String spoutname;
    public String parameter;
    public String topic;
    @Override
    public String toString() {
      JSONObject object = new JSONObject();
      object.put("classname", classname);
      object.put("spoutname", spoutname);
      object.put("parameter", parameter);
      object.put("topic", topic);
      return object.toJSONString();
    }
  }

  /** Must implement a ctor which takes a string parameter as argument. Only this constructor
   * will be called to instantiate the operator via reflection. */
  public FilterOperator(String parameter) {
    // Do Nothing.
  }
  /** Every blob read from kafka is passed thru this filter
   * operator. The kafkaLag is what the spout sees is the current
   * kafkaLag. A positive return value indicates that we need to
   * suppress this tuple. This is called before the
   * blob is passed to the scheme's deserializer.
   */
  public boolean filter(byte[] tuple, Long kafkaLag) {
    return false;
  }
  /** Every tuple read from kafka is passed thru this filter
   * operator. A positive return value indicates that we need to
   * suppress this tuple.
   * Note:- Each blob read from kafka can contain one or more
   * tuples(as deserialized by the scheme).
   */
  public boolean filter(List<Object> tuple, Long kafkaLag) {
    return false;
  }
}
