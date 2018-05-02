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

package org.apache.heron.integration_test.common.spout;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import org.apache.heron.api.spout.BaseRichSpout;
import org.apache.heron.api.spout.SpoutOutputCollector;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.api.tuple.Values;

/**
 * Given a list of local file paths, the spout will emit every line of the file in String format.
 * When we fetch all items from local file BufferedReader, we would set the BufferedReader as null. We
 * would like to check whether (BufferedReader == null) to see whether the data is taken completely.
 * <p>
 * Note: The number of parallelisms for this spout should be equal to the number of files/paths
 * to read.
 */
public class LocalFileSpout extends BaseRichSpout {
  private static final long serialVersionUID = 8536862528794112057L;
  // Hadoop file related
  private BufferedReader br = null;
  // Topology related
  private SpoutOutputCollector collector;
  private String[] paths;

  public LocalFileSpout(String path) {
    this(new String[]{path});
  }

  public LocalFileSpout(String[] paths) {
    this.paths = paths;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("string"));
  }

  @Override
  public void open(Map<String, Object> stormConf,
                   TopologyContext context,
                   SpoutOutputCollector newCollector) {
    int numTasks = context.getComponentTasks(context.getThisComponentId()).size();
    // Pre-condition: the number of tasks is equal to the number of files to read
    if (paths.length != numTasks) {
      throw new RuntimeException(
          String.format("Number of specified files %d not equal to number of tasks %d",
              paths.length, numTasks));
    }
    try {
      this.collector = newCollector;
      int index = context.getThisTaskIndex();
      String path = paths[index];
      // read from local file
      br = new BufferedReader(
          new FileReader(path),
          1024 * 1024
      );

    } catch (IOException e) {
      // Clean stuff if any exceptions
      try {
        // Close the outmost is enough
        if (br != null) {
          br.close();
        }
      } catch (IOException e1) {
        throw new RuntimeException("Unable to close file reader", e1);
      }

      // Here we should not close the FileSystem explicitly
      // Since different threads in a process may use a shared object,
      // if we close the FileSystem here, it will close the shared object, and other threads
      // reading data by using this shared object will throw Exception"
      // The FileSystem will be closed automatically when the process dies (the topology is killed)

      throw new RuntimeException("Failed to create BufferedReader from file path", e);
    }
  }

  // We do not explicitly close the buffered reader, even on EoF. This is in case more content is
  // added to file, we will read that content as well
  @Override
  public void nextTuple() {
    if (br == null) {
      return;
    }

    try {
      String currentLine;
      if ((currentLine = br.readLine()) != null) {
        collector.emit(new Values(currentLine));
      } else {
        br.close();
        br = null;
      }
    } catch (IOException e) {
      // Clean stuff if any exceptions
      try {
        // Close the outmost is enough
        if (br != null) {
          br.close();
        }
      } catch (IOException e1) {
        throw new RuntimeException("Unable to close stream reader", e1);
      }
      throw new RuntimeException("Unable to emit tuples normally", e);
    }
  }
}

