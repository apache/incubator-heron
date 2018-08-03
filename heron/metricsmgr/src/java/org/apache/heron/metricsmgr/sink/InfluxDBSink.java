package org.apache.heron.metricsmgr.sink;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.apache.heron.spi.metricsmgr.metrics.MetricsInfo;
import org.apache.heron.spi.metricsmgr.metrics.MetricsRecord;
import org.apache.heron.spi.metricsmgr.sink.IMetricsSink;
import org.apache.heron.spi.metricsmgr.sink.SinkContext;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.BatchOptions;

/**
 * A metrics sink for forwarding topology metrics to an
 * <a href="https://www.influxdata.com/time-series-platform/influxdb/">InfluxDB</a> time series
 * database server. Each {@link org.apache.heron.spi.metricsmgr.metrics.MetricsInfo MetricInfo}
 * instance inside a {@link org.apache.heron.spi.metricsmgr.metrics.MetricsRecord MetricsRecord}
 * for a given metrics source is converted into a {@link org.influxdb.dto.Point Point} and issued
 * to the InfluxDB client. Writes to the database can either be as soon as recieved or batched for
 * higher throughput.
 */
public class InfluxDBSink implements IMetricsSink {

  private static final Logger LOG = Logger.getLogger(InfluxDBSink.class.getName());

  /** InluxDB connection instance **/
  private InfluxDB influxDB;
  /** Heron Topology ID string **/
  private String topology;
  /** Heron Cluster name **/
  private String cluster;
  /** The role used to launch the topology **/
  private String role;
  /** The environment the topology was launched in  **/
  private String environ;
  /** The URL of the InfluxDB server host */
  private String serverHost;
  /** The port for the InfluxDB server on the host*/
  private String serverPort;
  /** The prefix string appended to the front of all topology database names */
  private String dbPrefix;
  /** The name of the database on InfluxDB server that metrics will sent to */
  private String dbName;
  /** Indicates if metrics should be written as soon as they are processed or batched according to
   * the flush frequency.
    */
  private boolean batchEnabled;

  // Set the config key names for the metrics_sink.yaml config file
  static final String SERVER_HOST_KEY = "influx-host";
  static final String SERVER_PORT_KEY = "influx-port";
  static final String METRIC_DB_PREFIX_KEY = "influx-db-prefix";
  static final String DB_USERNAME_KEY = "influx-db-username";
  static final String DB_PASSWORD_KEY = "influx-db-password";
  static final String BATCH_PROCESS_KEY = "enable-batch-processing";
  static final String MAIN_BUFFER_KEY = "main-buffer-size";
  static final String FAIL_BUFFER_KEY = "fail-buffer-size";

  /**
   * Initialisation method for the InfluxDB metrics sink.
   *
   * @param conf An unmodifiableMap containing basic configuration information parsed from the
   *             metrics-sinks.yaml config file.
   * @param context context object containing information on the Topology and Heron system this sink
   *                is registered with.
   */
  public void init(Map<String, Object> conf, SinkContext context) {

    LOG.info("Configuration: " + conf.toString());
    topology = context.getTopologyName();
    cluster = context.getCluster();
    role = context.getRole();
    environ = context.getEnvironment();

    serverHost = (String) conf.get(SERVER_HOST_KEY);

    // The InfluxDB client expects the host connection string to begin with http or https
    if(!serverHost.contains("http://") & !serverHost.contains("https://")){
      serverHost = "http://" + serverHost;
      LOG.warning("The server host key (" + SERVER_HOST_KEY + ") did not contain the http " +
                  "string. Setting server host string to: "+ serverHost);
      LOG.warning("If your influx instances uses https please alter the " + SERVER_HOST_KEY +
                  "in the appropriate metrics-sinks.yaml file");
    }

    // Account for the fact that the port could be entered as a sting or and integer in the
    // config file
    try {
        serverPort = (String) conf.get(SERVER_PORT_KEY);
    } catch(ClassCastException cce) {
        serverPort = String.valueOf((Integer) conf.get(SERVER_PORT_KEY));
    }

    LOG.info("Creating Influx connection client");
    // Check if username and passwords fields have been set
    if(conf.containsKey(DB_USERNAME_KEY) && conf.containsKey(DB_PASSWORD_KEY)) {
      String dbUser = (String) conf.get(DB_USERNAME_KEY);
      String dbPwd= (String) conf.get(DB_PASSWORD_KEY);
      LOG.info("Configuring InfluxDB client for user: " + dbUser);
      influxDB = InfluxDBFactory.connect(serverHost + ":" + serverPort, dbUser, dbPwd);
    } else {
        influxDB = InfluxDBFactory.connect(serverHost + ":" + serverPort);
    }

    // TODO: Add config options for ssl and UDP setups

    // Create and/or set the database name for the topology
    dbPrefix = (String) conf.get(METRIC_DB_PREFIX_KEY);
    dbName = dbPrefix + "-" + cluster + "-"  + environ + "-" + topology;

    if(!influxDB.databaseExists(dbName)){
      LOG.info("Creating topology database: " + dbName);
      influxDB.createDatabase(dbName);
    } else {
      LOG.info("Topology database: " + dbName + "already exists. Metrics will be written to " +
               "this existing database");
    }

    influxDB.setDatabase(dbName);

    // Check if batch processing is to be used
    batchEnabled = Boolean.valueOf((String) conf.get(BATCH_PROCESS_KEY));

    if(batchEnabled){
      LOG.info("Configuring InfluxDB client for batch processing");
      Integer flushFrequency = (Integer) conf.get("flush-frequency-ms");
      Integer mainBufferSize = (Integer) conf.get(MAIN_BUFFER_KEY);
      Integer failBufferSize = (Integer) conf.get(FAIL_BUFFER_KEY);

      // Set the batch options - Set the flush frequency to be twice the Heron value to ensure Heron
      // controls the flush frequency not the Influx Client
      BatchOptions batchOptions = BatchOptions.DEFAULTS
          .actions(mainBufferSize)
          .bufferLimit(failBufferSize)
          .flushDuration(flushFrequency * 2);

      influxDB.enableBatch(batchOptions);
      LOG.info("Influx Connection created with batch processing enabled. " +
               "Metrics will be sent at the flush frequency.");
    } else {
      LOG.info("Influx Connection created with batch processing disabled. " +
          "Metrics will be sent as they are received.");
    }

	}

  /**
   * Process a {@link org.apache.heron.spi.metricsmgr.metrics.MetricsRecord MetricsRecord} and add
   * the data from each {@link org.apache.heron.spi.metricsmgr.metrics.MetricsInfo MetricInfo}
   * instance it contains as a separate {@link org.influxdb.dto.Point Point} to either a
   * {@link org.influxdb.dto.BatchPoints BatchPoints} collection which is written immediatly to the
   * Influx Database (if batching is disabled) or written to the
   * {@link org.influxdb.dto.InfluxDB InfluxDB} client's buffer to be sent at intervals according
   * to the flush frequency (if batching is enabled).
   *
   * @param record The MetricsRecord to sent to InfluxDB
   */
	public void processRecord(MetricsRecord record) {

	    // The time stamp and source for this record will be applied to all InfluxDB points that come
      // from this record.
      Long timestamp = record.getTimestamp();
      String source = record.getSource();

      if(batchEnabled) {

        // Cycle through the metrics and convert each MetricsInfo instance into a InfluxDB Point
        for (MetricsInfo metric : record.getMetrics()) {

          Point point = Point.measurement(metric.getName())
              .time(timestamp, TimeUnit.MILLISECONDS)
              .tag("Topology", topology)
              .tag("Cluster", cluster)
              .tag("Role", role)
              .tag("Environment", environ)
              .tag("Source", source)
              .addField("value", metric.getValue())
              .build();

          // As batch is enabled this will just add the point to the InfluxDB client's buffer to be
          // sent when flush() is called on the sink (or when the main buffer is full).
          influxDB.write(point);
        }

      } else {

        // If batching is not enabled then we convert each MetricsInfo instance in the MetricsRecord
        // into a Point and create a batch of them that are issued as one block as soon as
        // they are all processed.

        BatchPoints points = BatchPoints
            .database(dbName)
            .tag("Topology", topology)
            .tag("Cluster", cluster)
            .tag("Role", role)
            .tag("Environment", environ)
            .tag("Source", source)
            .build();

        // Cycle through the metrics and convert each MetricsInfo instance into a InfluxDB Point
        for (MetricsInfo metric : record.getMetrics()) {

          Point point = Point.measurement(metric.getName())
              .time(timestamp, TimeUnit.MILLISECONDS)
              .addField("value", metric.getValue())
              .build();

            // Add the current metric point to the batch
            points.point(point);
          }


        try {
          //Write the batch of points to InfluxDB.
          influxDB.write(points);
          LOG.info(points.getPoints().size() + " metrics sent to " +
              "InfluxDB with timestamp: " + timestamp);
        } catch (NullPointerException n) {
          LOG.warning("Sending to InfluxDB raised a NullPointer Exception");
          n.printStackTrace();
        } catch (Exception e) {
          LOG.warning("Sending to InfluxDB raised a generic exception");
          for(Point point : points.getPoints())
            System.err.println(point);
          e.printStackTrace();
        }
      }

	}

  /**
   * Flush the InfluxDB client's main buffer. This will only have an effect if batch processing
   * is enabled.
   */
	public void flush() {
	  if(batchEnabled) {
      LOG.info("Flushing buffered metrics to InfluxDB");
      influxDB.flush();
    }
	}

  /**
   * Closes the InfluxDB client connection. This is important if batched processing is enabled, to
   * ensure that the buffers thread pool is released correctly.
   */
	public void close() {
    LOG.info("Closing InfluxDB connection");
    try{
        influxDB.close();
    } catch (NullPointerException npe) {
       LOG.severe("Unable to close InfluxDB client connection as it was never created");
    }
	}

}
