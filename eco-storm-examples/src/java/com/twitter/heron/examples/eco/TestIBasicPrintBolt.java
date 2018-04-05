//  Copyright 2018 Twitter. All rights reserved.
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
package com.twitter.heron.examples.eco;

import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import static org.apache.storm.utils.Utils.tuple;

@SuppressWarnings({"serial", "rawtypes", "unchecked", "HiddenField"})
public class TestIBasicPrintBolt implements IBasicBolt {

  public String someProperty = "set ";

  public TestUnits testUnits;

  @Override
  public void prepare(Map stormConf, TopologyContext context) {

  }

  public void sampleConfigurationMethod(String someProperty, TestUnits testUnits) {
    this.someProperty += someProperty;
    this.testUnits = testUnits;
  }

  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {
    System.out.println("The configuration method has set \"someProperty\" to : "
        + this.someProperty);
    System.out.println("The configuration method has set TestUnits to " + testUnits);
    System.out.println("Emitting : " + input);
    collector.emit(tuple(input.getValues().get(0)));

  }

  @Override
  public void cleanup() {

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("ibasic-print"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
