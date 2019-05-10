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

package org.apache.heron.integration_test.topology.serialization;

import java.util.logging.Logger;

import org.apache.heron.api.bolt.BaseBasicBolt;
import org.apache.heron.api.bolt.BasicOutputCollector;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.api.tuple.Tuple;
import org.apache.heron.api.tuple.Values;

/**
 * A bolt that checks deserialization works fine for UnserializableCustomObject
 */
public class UnserializableCustomCheckBolt extends BaseBasicBolt {
  private static final Logger LOG = Logger.getLogger(CustomCheckBolt.class.getName());
  private int nItems;
  private CustomObject[] inputObjects;

  public UnserializableCustomCheckBolt(CustomObject[] inputObjects) {
    this.nItems = 0;
    this.inputObjects = inputObjects;
  }

  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {
    LOG.info("Received input tuple: " + input.getValueByField("custom").toString());
    UnserializableCustomObject unserializable =
        (UnserializableCustomObject) input.getValueByField("custom");
    if (unserializable.getObj().equals(inputObjects[(nItems++) % inputObjects.length])) {
      collector.emit(new Values(unserializable.getObj()));
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("custom"));
  }
}
