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


package org.apache.heron.examples.streamlet;

import java.io.Serializable;
import java.util.List;
import java.util.Random;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.heron.examples.streamlet.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Runner;

/**
 * This topology demonstrates the use of consume operations in the Heron
 * Streamlet API for Java. A consume operation terminates a processing
 * graph, like a simpler version of a sink (which requires implementing
 * the Sink interface). Here, the consume operation is used to produce
 * custom-formatted logging output for a processing graph in which random
 * sensor readings are fed into the graph every two seconds (a simple
 * filter is also applied to this source streamlet prior to logging).
 */
public final class FormattedOutputTopology {
  private FormattedOutputTopology() {
  }

  private static final Logger LOG =
      Logger.getLogger(FormattedOutputTopology.class.getName());

  /**
   * A list of devices emitting sensor readings ("device1" through "device100").
   */
  private static final List<String> DEVICES = IntStream.range(1, 100)
      .mapToObj(i -> String.format("device%d", i))
      .collect(Collectors.toList());

  /**
   * Sensor readings consist of a device ID, a temperature reading, and
   * a humidity reading. The temperature and humidity readings are
   * randomized within a range.
   */
  private static class SensorReading implements Serializable {
    private static final long serialVersionUID = 3418308641606699744L;
    private String deviceId;
    private double temperature;
    private double humidity;

    SensorReading() {
      // Readings are produced only every two seconds
      StreamletUtils.sleep(2000);
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
    Builder processingGraphBuilder = Builder.newBuilder();

    processingGraphBuilder
        // The source streamlet is an indefinite series of sensor readings
        // emitted every two seconds
        .newSource(SensorReading::new)
        // A simple filter that excludes a percentage of the sensor readings
        .filter(reading -> reading.getHumidity() < .9 && reading.getTemperature() < 90)
        // In the consumer operation, each reading is converted to a formatted
        // string and logged
        .consume(reading -> LOG.info(
            String.format("Reading from device %s: (temp: %f, humidity: %f)",
                reading.getDeviceId(),
                reading.getTemperature(),
                reading.getHumidity())
        ));

    // Fetches the topology name from the first command-line argument
    String topologyName = StreamletUtils.getTopologyName(args);

    Config config = Config.defaultConfig();

    // Finally, the processing graph and configuration are passed to the Runner, which converts
    // the graph into a Heron topology that can be run in a Heron cluster.
    new Runner().run(topologyName, config, processingGraphBuilder);
  }
}
