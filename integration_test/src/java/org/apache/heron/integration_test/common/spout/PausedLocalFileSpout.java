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
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.heron.api.spout.BaseRichSpout;
import org.apache.heron.api.spout.SpoutOutputCollector;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.api.tuple.Values;

/**
 * Given a list of local file paths, the spout will wait for file contents to be added, then
 * emit every line of the file in String format. First, we block in open method until file is created.
 * If using in integration test, to ensure atomicity, write to a separate file and then rename file to what this class
 * is polling from.
 * When we fetch all items from local file BufferedReader, we do not close the file. Instead, we keep polling for appended lines.
 * <p>
 * Note: The number of parallelisms for this spout should be equal to the number of files/paths
 * to read.
 */
public class PausedLocalFileSpout extends BaseRichSpout {
  private static final long serialVersionUID = 7233454257997083024L;
  private static final Logger LOG = Logger.getLogger(PausedLocalFileSpout.class.getName());

  private BufferedReader br = null;
  private SpoutOutputCollector collector;
  private String[] paths;

  public PausedLocalFileSpout(String path) {
    this(new String[]{path});
  }

  public PausedLocalFileSpout(String[] paths) {
    this.paths = paths;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("string"));
  }

  // Here, the spout will block until the file at the path exists
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
    this.collector = newCollector;
    int index = context.getThisTaskIndex();
    String path = paths[index];
    File file = new File(path);
    while (!file.exists()) {
      // busy loop until file is created. Don't throw any exceptions
    }
    try {
      LOG.info("Creating reader for input data from file " + file.getAbsolutePath());
      // read from local file
      br = new BufferedReader(
          new FileReader(file),
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

  // each nextTuple will either read the current line as null, and not emit anything
  // or emit one line from the text file
  // We do not explicitly close buffered reader. This is so the spout will read any new data
  // appended to the file. On spout close, buffered reader will close automatically
  @Override
  public void nextTuple() {
    if (br == null) {
      return;
    }

    try {
      String currentLine;
      // if at EoF, do not close buffered reader. Instead, keep polling from file until there is
      // more content, and do not emit anything if data is null
      if ((currentLine = br.readLine()) != null) {
        LOG.info("Emitting tuple from input data file: " + currentLine);
        collector.emit(new Values(currentLine), "MESSAGE_ID");
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

