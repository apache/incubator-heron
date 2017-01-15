// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.twitter.heron.integration_test.core;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.logging.Logger;

import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Tuple;

/**
 * A Bolt which collects the strings it is sent into a buffer
 * and on terminal, writes the lines in the buffer to the local
 * file specificed by localFilePath
 */
public class LocalAggregatorBolt extends BaseBatchBolt implements ITerminalBolt {
  private static final long serialVersionUID = 7363942149997565188L;
  private static final Logger LOG = Logger.getLogger(LocalAggregatorBolt.class.getName());
  private final String localFilePath;
  private BufferedWriter bw = null;

  public LocalAggregatorBolt(String localFilePath) {
    LOG.info("Local File Path : " + localFilePath);
    this.localFilePath = localFilePath;
  }

  @Override
  public void finishBatch() {
    writeFinishedData();
  }

  @Override
  public void prepare(Map<String, Object> map,
                      TopologyContext topologyContext,
                      OutputCollector outputCollector) {
    try {
      File outputFile = new File(localFilePath);
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

  @Override
  public void execute(Tuple tuple) {
    try {
      String data = tuple.getString(0);
      bw.write(data);
      bw.newLine();
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
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    // The last bolt we append, nothing to emit.
  }

  @Override
  public void writeFinishedData() {
    try {
      bw.flush();
    } catch (IOException e) {
      throw new RuntimeException("Unable to write to file", e);
    }
  }
}
