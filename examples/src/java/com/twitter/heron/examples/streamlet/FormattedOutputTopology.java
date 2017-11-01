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

package com.twitter.heron.examples.streamlet;

import java.io.Serializable;
import java.util.List;
import java.util.Random;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.twitter.heron.api.utils.Utils;
import com.twitter.heron.examples.streamlet.utils.StreamletUtils;
import com.twitter.heron.streamlet.Builder;
import com.twitter.heron.streamlet.Config;
import com.twitter.heron.streamlet.Runner;

public final class FormattedOutputTopology {
  private FormattedOutputTopology() {    
  }
  
  private static final Logger LOG =
      Logger.getLogger(FormattedOutputTopology.class.getName());

  private static final List<String> DEVICES = IntStream.range(1, 100)
      .mapToObj(i -> String.format("device%d", i))
      .collect(Collectors.toList());

  private static class SensorReading implements Serializable {
    private static final long serialVersionUID = 3418308641606699744L;
    private String deviceId;
    private double temperature;
    private double humidity;

    SensorReading() {
      Utils.sleep(2000);
      this.deviceId = StreamletUtils.randomFromList(DEVICES);
      // Each temperature reading is a double between 70 and 100
      this.temperature = 70 + 30 * new Random().nextDouble();
      // Each humidity reading is a percentage between 80 and 100
      this.humidity = (80 + 20 * new Random().nextDouble()) / 100;
    }

    String getDeviceId() {
      return deviceId;
    }

    double getTemperature() {
      return temperature;
    }

    double getHumidity() {
      return humidity;
    }
  }

  public static void main(String[] args) throws Exception {
    Builder processingGraphBuilder = Builder.createBuilder();

    processingGraphBuilder
        .newSource(SensorReading::new)
        .filter(reading -> (reading.humidity < .9 && reading.temperature < 90))
        .consume(reading -> LOG.info(
            String.format("Reading from device %s: (temp: %f, humidity: %f)",
                reading.getDeviceId(),
                reading.getTemperature(),
                reading.getHumidity())
        ));

    String topologyName = StreamletUtils.getTopologyName(args);

    Config config = new Config();

    new Runner().run(topologyName, config, processingGraphBuilder);
  }
}
