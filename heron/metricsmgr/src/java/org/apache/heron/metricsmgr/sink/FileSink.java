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

package org.apache.heron.metricsmgr.sink;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.heron.common.basics.TypeUtils;
import org.apache.heron.spi.metricsmgr.metrics.ExceptionInfo;
import org.apache.heron.spi.metricsmgr.metrics.MetricsInfo;
import org.apache.heron.spi.metricsmgr.metrics.MetricsRecord;
import org.apache.heron.spi.metricsmgr.sink.IMetricsSink;
import org.apache.heron.spi.metricsmgr.sink.SinkContext;

/**
 * A metrics sink that writes to a file  in json format
 * We would create/overwrite a file every time the flush() in invoked
 * We would save at most fileMaximum metrics file in disk
 */
public class FileSink implements IMetricsSink {
  private static final Logger LOG = Logger.getLogger(FileSink.class.getName());

  private static final String FILENAME_KEY = "filename-output";
  private static final String MAXIMUM_FILE_COUNT_KEY = "file-maximum";

  // Metrics Counter Name
  private static final String METRICS_COUNT = "metrics-count";
  private static final String EXCEPTIONS_COUNT = "exceptions-count";
  private static final String FLUSH_COUNT = "flush-count";
  private static final String RECORD_PROCESS_COUNT = "record-process-count";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  // We would convert a file's metrics into a JSON object, i.e. array
  // So we need to add "[" at the start and "]" at the end
  private boolean isFileStart = true;
  private PrintStream writer;
  private String filenamePrefix;
  private int fileMaximum = 1;
  private int currentFileIndex = 0;
  private SinkContext sinkContext;

  @Override
  public void init(Map<String, Object> conf, SinkContext context) {
    verifyConf(conf);
    filenamePrefix = conf.get(FILENAME_KEY) + "." + context.getMetricsMgrId();
    fileMaximum = TypeUtils.getInteger(conf.get(MAXIMUM_FILE_COUNT_KEY));
    sinkContext = context;
  }

  private void verifyConf(Map<String, Object> conf) {
    if (!conf.containsKey(FILENAME_KEY)) {
      throw new IllegalArgumentException("Require: " + FILENAME_KEY);
    }
    if (!conf.containsKey(MAXIMUM_FILE_COUNT_KEY)) {
      throw new IllegalArgumentException("Require: " + MAXIMUM_FILE_COUNT_KEY);
    }
  }

  private PrintStream openNewFile(String filename) {
    // If the file already exists, set it Writable to avoid permission denied
    File f = new File(filename);
    if (f.exists() && !f.isDirectory()) {
      f.setWritable(true);
    }

    try {
      return new PrintStream(new FileOutputStream(filename, false), true, "UTF-8");
    } catch (FileNotFoundException | UnsupportedEncodingException e) {
      throw new RuntimeException("Error creating " + filename, e);
    }
  }

  private String convertRecordToJSON(MetricsRecord record) {
    int metricsCount = 0;
    int exceptionsCount = 0;

    // Pack metrics as a map
    Map<String, String> metrics = new HashMap<String, String>();
    for (MetricsInfo metricsInfo : record.getMetrics()) {
      metrics.put(metricsInfo.getName(), metricsInfo.getValue());

      metricsCount++;
    }

    // Pack exceptions as a list of map
    LinkedList<Object> exceptions = new LinkedList<Object>();
    for (ExceptionInfo exceptionInfo : record.getExceptions()) {
      Map<String, Object> exception = new HashMap<String, Object>();
      exception.put("firstTime", exceptionInfo.getFirstTime());
      exception.put("lastTime", exceptionInfo.getLastTime());
      exception.put("logging", exceptionInfo.getLogging());
      exception.put("stackTrace", exceptionInfo.getStackTrace());
      exception.put("count", exceptionInfo.getCount());
      exceptions.add(exception);

      exceptionsCount++;
    }

    // Pack the whole MetricsRecord as a map
    Map<String, Object> jsonToWrite = new HashMap<String, Object>();
    jsonToWrite.put("timestamp", record.getTimestamp());
    jsonToWrite.put("source", record.getSource());
    jsonToWrite.put("context", record.getContext());
    jsonToWrite.put("metrics", metrics);
    jsonToWrite.put("exceptions", exceptions);

    // Update metrics
    sinkContext.exportCountMetric(METRICS_COUNT, metricsCount);
    sinkContext.exportCountMetric(EXCEPTIONS_COUNT, exceptionsCount);

    String result = "";
    try {
      result = MAPPER.writeValueAsString(jsonToWrite);
    } catch (JsonProcessingException e) {
      LOG.log(Level.SEVERE, "Could not convert map to JSONString: " + jsonToWrite.toString(), e);
    }

    return result;
  }

  @Override
  public void processRecord(MetricsRecord record) {
    if (isFileStart) {
      writer = openNewFile(String.format("%s.%d", filenamePrefix, currentFileIndex));

      writer.print("[");
      isFileStart = false;
    } else {
      writer.print(",");
    }

    // Convert the map to String in json
    writer.print(convertRecordToJSON(record));

    // Update the Metrics
    sinkContext.exportCountMetric(RECORD_PROCESS_COUNT, 1);
  }

  @Override
  public void flush() {
    if (isFileStart) {
      // No record has been processed since the previous flush, so create a new file
      // and output an empty JSON array.
      writer = openNewFile(String.format("%s.%d", filenamePrefix, currentFileIndex));
      writer.print("[");
    }
    writer.print("]");
    writer.flush();
    writer.close();
    new File(String.format("%s.%s", filenamePrefix, currentFileIndex)).setReadOnly();

    currentFileIndex = (currentFileIndex + 1) % fileMaximum;

    isFileStart = true;

    // Update the Metrics
    sinkContext.exportCountMetric(FLUSH_COUNT, 1);
  }

  @Override
  public void close() {
    if (writer != null) {
      writer.close();
    }
  }
}
