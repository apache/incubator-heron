package com.twitter.heron.metricsmgr.sink;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.twitter.heron.api.utils.Utils;
import com.twitter.heron.metricsmgr.api.metrics.ExceptionInfo;
import com.twitter.heron.metricsmgr.api.metrics.MetricsInfo;
import com.twitter.heron.metricsmgr.api.metrics.MetricsRecord;
import com.twitter.heron.metricsmgr.api.sink.IMetricsSink;
import com.twitter.heron.metricsmgr.api.sink.SinkContext;

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
  private static final String PRETTY_PRINT = "pretty-print";

  private PrintStream writer;

  private String filenamePrefix;

  private boolean prettyPrint;

  private int fileMaximum = 1;
  private int currentFileIndex = 0;

  // We would convert a file's metrics into a JSON object, i.e. array
  // So we need to add "[" at the start and "]" at the end
  private static boolean isFileStart = true;

  private SinkContext sinkContext;

  private static final ObjectMapper mapper = new ObjectMapper();

  @Override
  public void init(Map<String, Object> conf, SinkContext context) {
    filenamePrefix = (String) conf.get(FILENAME_KEY) + "." + context.getMetricsMgrId();
    fileMaximum = Utils.getInt(conf.get(MAXIMUM_FILE_COUNT_KEY));
    sinkContext = context;
    prettyPrint = Boolean.parseBoolean((String) conf.get(PRETTY_PRINT));

    // We set System.out as writer's default value here to avoid null object
    writer = System.out;
  }

  private void openNewFile(String filename) {
    // If the file already exists, set it Writable to avoid permission denied
    File f = new File(filename);
    if (f.exists() && !f.isDirectory()) {
      f.setWritable(true);
    }

    try {
      writer = filename == null ? System.out
          : new PrintStream(new FileOutputStream(filename, false),
          true, "UTF-8");
    } catch (Exception e) {
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
      if (prettyPrint) {
        result = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(jsonToWrite);
      } else {
        result = mapper.writeValueAsString(jsonToWrite);
      }
    } catch (JsonProcessingException e) {
      LOG.log(Level.SEVERE, "Could not convert map to JSONString: " + jsonToWrite.toString(), e);
    }

    return result;
  }

  @Override
  public void processRecord(MetricsRecord record) {
    if (isFileStart) {
      openNewFile(String.format("%s.%s", filenamePrefix, currentFileIndex));

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
    writer.close();
  }
}
