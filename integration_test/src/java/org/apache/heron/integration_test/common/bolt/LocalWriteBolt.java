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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import org.apache.heron.api.bolt.BaseBasicBolt;
import org.apache.heron.api.bolt.BasicOutputCollector;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.api.tuple.Tuple;

/**
 * Given a local file path, this bolt will emit every line of received in String format, as well as write
 * every line to the local file.
 * <p>
 * Note: The number of parallelisms for this spout should be equal to the number of files/paths
 * to read.
 */
public class LocalWriteBolt extends BaseBasicBolt {
  private static final long serialVersionUID = 2320175828567970635L;
  private String path;
  private BufferedWriter bw = null;

  public LocalWriteBolt(String path) {
    this.path = path;
  }

  @Override
  public void prepare(Map<String, Object> map, TopologyContext topologyContext) {
    try {
      File outputFile = new File(path);
      if (!outputFile.exists()) {
        outputFile.createNewFile();
      }
      bw = new BufferedWriter(
          new FileWriter(outputFile.getAbsoluteFile(), true),
          1024 * 1024
      );
    } catch (IOException e) {
      // Clean stuff if any exceptions
      try {
        if (bw != null) {
          bw.close();
        }
      } catch (IOException e1) {
        throw new RuntimeException("Unable to close file writer", e1);
      }
      throw new RuntimeException("Failed to create BufferedWriter from file path", e);
    }
  }

  // We do not explicitly close the buffered writer in LocalWriteBolt as we cannot guarantee which
  // tuple is the last tuple. Writer will be closed automatically on process close
  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {
    try {
      String data = input.getString(0);
      bw.write(data);
      bw.newLine();
      bw.flush();
    } catch (IOException e) {
      // Clean stuff if any exceptions
      try {
        // Close the outmost is enough
        if (bw != null) {
          bw.close();
        }
      } catch (IOException e1) {
        throw new RuntimeException("Unable to close stream writer", e1);
      }
      throw new RuntimeException("Unable to write to file or emit tuples", e);
    }
    collector.emit(input.getValues());
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("string"));
  }
}
