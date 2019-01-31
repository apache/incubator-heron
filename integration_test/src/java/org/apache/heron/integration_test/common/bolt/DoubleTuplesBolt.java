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

package org.apache.heron.integration_test.common.bolt;


import org.apache.heron.api.bolt.BaseBasicBolt;
import org.apache.heron.api.bolt.BasicOutputCollector;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.api.tuple.Tuple;

/**
 * This bolt will doubly emit what it receives.
 */

public class DoubleTuplesBolt extends BaseBasicBolt {
  private static final long serialVersionUID = -3453913602865161439L;
  private Fields fields;

  public DoubleTuplesBolt(Fields fields) {
    this.fields = fields;
  }

  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {
    collector.emit(input.getValues());
    collector.emit(input.getValues());
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(fields);
  }
}
