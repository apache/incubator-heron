//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package com.twitter.heron.pulsar;

import java.io.Serializable;

import com.yahoo.pulsar.client.api.Message;

import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.tuple.Values;

public interface MessageToValuesMapper extends Serializable {

  /**
   * Convert {@link com.yahoo.pulsar.client.api.Message} to tuple values.
   *
   * @param message
   * @return
   */
  Values toValues(Message message);

  /**
   * Declare the output schema for the spout.
   *
   * @param declarer
   */
  void declareOutputFields(OutputFieldsDeclarer declarer);
}
