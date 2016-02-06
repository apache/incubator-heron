package com.twitter.heron.metricsmgr.sink;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.scribe.LogEntry;
import org.apache.scribe.ResultCode;
import org.apache.scribe.scribe;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;

import com.twitter.heron.common.basics.TypeUtils;
import com.twitter.heron.common.core.base.Constants;
import com.twitter.heron.spi.metricsmgr.metrics.MetricsInfo;
import com.twitter.heron.spi.metricsmgr.metrics.MetricsRecord;
import com.twitter.heron.spi.metricsmgr.sink.IMetricsSink;
import com.twitter.heron.spi.metricsmgr.sink.SinkContext;

/**
 * A metrics sink that writes to Scribe with format required by Twitter Cuckoo
 */
public class ScribeSink implements IMetricsSink {
  private static final Logger LOG = Logger.getLogger(ScribeSink.class.getName());

  // Counters' key
  private static final String MESSAGE = "message";
  private static final String OK = "ok";
  private static final String TRY_AGAIN = "try_again";
  private static final String FAILED = "failed";

  // These configs would be read from sink-configs.yaml
  private static final String KEY_SCRIBE_HOST = "scribe-host";
  private static final String KEY_SCRIBE_PORT = "scribe-port";
  private static final String KEY_CATEGORY = "scribe-category";
  private static final String KEY_SERVICE_NAMESPACE = "service-namespace";
  private static final String KEY_SCRIBE_TIMEOUT_MS = "scribe-timeout-ms";
  private static final String KEY_SCRIBE_CONNECT_SERVER_ATTEMPTS = "scribe-connect-server-attempts";
  private static final String KEY_SCRIBE_RETRY_ATTEMPTS = "scribe-retry-attempts";
  private static final String KEY_SCRIBE_RETRY_INTERVAL_MS = "scribe-retry-interval-ms";

  // Metrics Counter Name
  private static final String FLUSH_COUNT = "flush-count";
  private static final String RECORD_PROCESS_COUNT = "record-process-count";
  private static final String MESSAGE_COUNT = "message-count";
  private static final String OK_COUNT = "ok-count";
  private static final String TRY_AGAIN_COUNT = "try-again-count";
  private static final String FAILED_COUNT = "failed-count";
  private static final String ILLEGAL_METRICS_COUNT = "illegal-metrics-count";

  // The SinkConfig for ScribeSink, which is parsed from sink-configs.yaml
  private Map<String, Object> config;

  private static final Map<String, Long> counters = new HashMap<String, Long>();

  private TFramedTransport transport;

  private scribe.Client client;

  private SinkContext sinkContext;

  private String topologyName;

  private int connectRetryAttempts;

  private static final ObjectMapper mapper = new ObjectMapper();

  @Override
  public void init(Map<String, Object> conf, SinkContext context) {
    // Init the counters
    counters.put(MESSAGE, 0L);
    counters.put(OK, 0L);
    counters.put(TRY_AGAIN, 0L);
    counters.put(FAILED, 0L);

    config = conf;

    sinkContext = context;

    topologyName = context.getTopologyName();

    connectRetryAttempts = TypeUtils.getInt(config.get(KEY_SCRIBE_CONNECT_SERVER_ATTEMPTS));

    // Open the TTransport connection and client to scribe server
    open();
  }

  @Override
  public void processRecord(MetricsRecord record) {
    // increase the processed counters
    counters.put(MESSAGE, counters.get(MESSAGE) + 1);

    // Check whether the TSocket is already open
    // If not, try to open the TSocket again
    if (!transport.isOpen() && !open()) {
      counters.put(FAILED, counters.get(FAILED) + 1);
      LOG.severe("Failed due to TTransport is not open");

      if (counters.get(FAILED) >= connectRetryAttempts) {
        throw new RuntimeException("The scribe sink failed to connect to server; exceeds " +
            connectRetryAttempts + " attempts");
      }

      return;
    }

    // Convert MetricsRecord to Twitter Cuckoo Format
    LogEntry logEntry = new LogEntry();
    logEntry.category = (String) config.get(KEY_CATEGORY);
    logEntry.message = makeJSON(record);
    LOG.fine("Metrics to log to Scribe" + logEntry.message);

    List<LogEntry> pendingEntries = new LinkedList<LogEntry>();
    pendingEntries.add(logEntry);

    // Log to Scribe with retry
    logToScribe(pendingEntries);

    // Update the Metrics
    sinkContext.exportCountMetric(RECORD_PROCESS_COUNT, 1);
  }

  @Override
  public void flush() {
    // We would directly log to scribe in processRecord(...)
    // So just flush counters here
    flushCounters();

    // Update the Metrics
    sinkContext.exportCountMetric(FLUSH_COUNT, 1);
    sinkContext.exportCountMetric(MESSAGE_COUNT, counters.get(MESSAGE).intValue());
    sinkContext.exportCountMetric(OK_COUNT, counters.get(OK).intValue());
    sinkContext.exportCountMetric(TRY_AGAIN_COUNT, counters.get(TRY_AGAIN).intValue());
    sinkContext.exportCountMetric(FAILED_COUNT, counters.get(FAILED).intValue());
  }

  @Override
  public void close() {
    LOG.info("Closing ScribeSink");
    transport.close();

    flushCounters();
  }

  // Open the TTransport connection and client to scribe server
  private boolean open() {
    try {
      TSocket socket = new TSocket((String) config.get(KEY_SCRIBE_HOST),
          TypeUtils.getInt(config.get(KEY_SCRIBE_PORT)),
          TypeUtils.getInt(config.get(KEY_SCRIBE_TIMEOUT_MS)));

      transport = new TFramedTransport(socket);
      transport.open();
    } catch (TException tx) {
      LOG.log(Level.SEVERE, "Failed to open connection to scribe server " + connectionString(), tx);
      return false;
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Failed to open connection to scribe server " + connectionString(), e);
      return false;
    }

    LOG.info("Opened connection to scribe server " + connectionString());
    TProtocol protocol = new TBinaryProtocol(transport);
    client = new scribe.Client(protocol);

    return true;
  }

  // Log the message to scribe, optionally retrying
  private void logToScribe(List<LogEntry> pendingEntries) {
    int retryAttempts = TypeUtils.getInt(config.get(KEY_SCRIBE_RETRY_ATTEMPTS));
    long retryIntervalMs = TypeUtils.getLong(config.get(KEY_SCRIBE_RETRY_INTERVAL_MS));
    try {
      for (int attempt = 0; attempt < retryAttempts; attempt++) {
        ResultCode result = client.Log(pendingEntries);

        // If successfully, we are done
        if (result.equals(ResultCode.OK)) {
          counters.put(OK, counters.get(OK) + pendingEntries.size());
          return;
        }

        // otherwise, try once more
        if (result.equals(ResultCode.TRY_LATER)) {
          counters.put(TRY_AGAIN, counters.get(TRY_AGAIN) + 1);
        }

        // Sleep a while to avoid to hit scribe server heavily
        Utils.sleep(retryIntervalMs);
      }
    } catch (TException te) {
      LOG.log(Level.SEVERE, "Message sending failed due to TransportException: ", te);
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Message sending failed due to exception: ", e);
    }

    counters.put(FAILED, counters.get(FAILED) + 1);
    close();
  }

  // Convert a record into json format required by Twitter Cuckoo
  private String makeJSON(MetricsRecord record) {
    String serviceName = String.format("%s/%s",
        config.get(KEY_SERVICE_NAMESPACE), topologyName);
    // The source format is "host:port/componentName/instanceId"
    // However, we need just "/componentName/instanceId"
    String[] sources = record.getSource().split("/");
    String source = String.format("/%s/%s", sources[1], sources[2]);
    // The timestamp is in ms, however, we need to convert it in seconds to fit Twitter Infra
    long timestamp = record.getTimestamp() / Constants.SECONDS_TO_MILLISECONDS;

    Map<String, Object> json = new HashMap<String, Object>();
    // Add the service name
    json.put("service", serviceName);
    // Add the service name
    json.put("source", source);
    // Add the time stamp
    json.put("timestamp", timestamp);

    // Cuckoo_json allows multi-metrics in a single JSON, so we would like to
    // package all metrics received into one single JSON
    int metricsToWrite = 0;
    for (MetricsInfo info : record.getMetrics()) {

      // First check whether the metric  value is legal
      // since scribe would only accept a Double value as metric value
      // We would just skip it
      Double val;
      try {
        val = Double.valueOf(info.getValue());
      } catch (NumberFormatException ne) {
        LOG.log(Level.SEVERE, "Could not parse illegal metric: " + info.toString());

        sinkContext.exportCountMetric(ILLEGAL_METRICS_COUNT, 1);
        continue;
      }
      json.put(info.getName(), val);
      metricsToWrite++;
    }

    LOG.info(metricsToWrite + " metrics added");

    String result = "";
    try {
      result = mapper.writeValueAsString(json);
    } catch (JsonProcessingException e) {
      LOG.log(Level.SEVERE, "Could not convert map to JSONString: " + json.toString(), e);
    }

    return result;
  }

  private String connectionString() {
    return String.format("<%s:%d>", config.get(KEY_SCRIBE_HOST), config.get(KEY_SCRIBE_PORT));
  }

  private void flushCounters() {
    LOG.info(counters.toString());
  }
}
