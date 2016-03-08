package com.twitter.heron.instance;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

import com.twitter.heron.common.config.SystemConfig;
import com.twitter.heron.common.basics.Communicator;
import com.twitter.heron.common.basics.NIOLooper;
import com.twitter.heron.common.basics.SingletonRegistry;
import com.twitter.heron.common.network.HeronSocketOptions;
import com.twitter.heron.common.utils.metrics.JVMMetrics;
import com.twitter.heron.common.utils.metrics.MetricsCollector;
import com.twitter.heron.common.utils.misc.Constants;
import com.twitter.heron.common.utils.misc.ErrorReportLoggingHandler;
import com.twitter.heron.metrics.GatewayMetrics;
import com.twitter.heron.network.MetricsManagerClient;
import com.twitter.heron.network.StreamManagerClient;
import com.twitter.heron.proto.system.HeronTuples;
import com.twitter.heron.proto.system.Metrics;
import com.twitter.heron.proto.system.PhysicalPlans;

/**
 * Gateway is a Runnable and will be executed in a thread.
 * It will new the streamManagerClient and metricsManagerClient in constructor and
 * ask them to connect with corresponding socket endpoint in run().
 */
public class Gateway implements Runnable {
  private static final Logger LOG = Logger.getLogger(Gateway.class.getName());

  // Some pre-defined value
  private static final String streamMgrHost = "127.0.0.1";
  private static final String metricsMgrHost = "127.0.0.1";

  // MetricsManagerClient will communicate with Metrics Manager
  private final MetricsManagerClient metricsManagerClient;
  // StreamManagerClient will communicate with Stream Manager
  private final StreamManagerClient streamManagerClient;

  private final NIOLooper gatewayLooper;

  private final MetricsCollector gatewayMetricsCollector;

  private final JVMMetrics jvmMetrics;
  private final GatewayMetrics gatewayMetrics;

  private final SystemConfig systemConfig;

  public Gateway(String topologyName, String topologyId, PhysicalPlans.Instance instance,
                 int streamPort, int metricsPort, final NIOLooper gatewayLooper,
                 final Communicator<HeronTuples.HeronTupleSet> inStreamQueue,
                 final Communicator<HeronTuples.HeronTupleSet> outStreamQueue,
                 final Communicator<InstanceControlMsg> inControlQueue,
                 final List<Communicator<Metrics.MetricPublisherPublishMessage>> outMetricsQueues)
      throws IOException {
    systemConfig =
        (SystemConfig) SingletonRegistry.INSTANCE.getSingleton(SystemConfig.HERON_SYSTEM_CONFIG);

    // New the client
    this.gatewayLooper = gatewayLooper;
    this.gatewayMetricsCollector = new MetricsCollector(gatewayLooper, outMetricsQueues.get(0));

    // JVM Metrics are auto-sample metrics so we do not have to insert it inside singleton
    // since it should not be used in other places
    jvmMetrics = new JVMMetrics();
    jvmMetrics.registerMetrics(gatewayMetricsCollector);

    // since we need to call its methods in a lot of classes
    gatewayMetrics = new GatewayMetrics();
    gatewayMetrics.registerMetrics(gatewayMetricsCollector);

    // Init the ErrorReportHandler
    ErrorReportLoggingHandler.init(
        instance.getInstanceId(), gatewayMetricsCollector,
        systemConfig.getHeronMetricsExportIntervalSec());

    // Initialize the corresponding 2 socket clients with corresponding socket options
    HeronSocketOptions socketOptions = new HeronSocketOptions(
        systemConfig.getInstanceNetworkWriteBatchSizeBytes(),
        systemConfig.getInstanceNetworkWriteBatchTimeMs(),
        systemConfig.getInstanceNetworkReadBatchSizeBytes(),
        systemConfig.getInstanceNetworkReadBatchTimeMs(),
        systemConfig.getInstanceNetworkOptionsSocketSendBufferSizeBytes(),
        systemConfig.getInstanceNetworkOptionsSocketReceivedBufferSizeBytes()
    );
    this.streamManagerClient =
        new StreamManagerClient(gatewayLooper, streamMgrHost, streamPort,
            topologyName, topologyId, instance,
            inStreamQueue, outStreamQueue, inControlQueue,
            socketOptions, gatewayMetrics);
    this.metricsManagerClient = new MetricsManagerClient(gatewayLooper, metricsMgrHost,
        metricsPort, instance, outMetricsQueues, socketOptions, gatewayMetrics);

    // Attach sample Runnable to gatewayMetricsCollector
    gatewayMetricsCollector.registerMetricSampleRunnable(jvmMetrics.getJVMSampleRunnable(),
        systemConfig.getInstanceMetricsSystemSampleIntervalSec());
    Runnable sampleStreamQueuesSize = new Runnable() {
      @Override
      public void run() {
        gatewayMetrics.setInStreamQueueSize(inStreamQueue.size());
        gatewayMetrics.setOutStreamQueueSize(outStreamQueue.size());
        gatewayMetrics.setInStreamQueueExpectedCapacity(inStreamQueue.getExpectedAvailableCapacity());
        gatewayMetrics.setOutStreamQueueExpectedCapacity(outStreamQueue.getExpectedAvailableCapacity());
      }
    };
    gatewayMetricsCollector.registerMetricSampleRunnable(sampleStreamQueuesSize,
        systemConfig.getInstanceMetricsSystemSampleIntervalSec());

    final long instanceTuningIntervalMs = systemConfig.getInstanceTuningIntervalMs() *
        com.twitter.heron.common.basics.Constants.MILLISECONDS_TO_NANOSECONDS;

    // Attache Runnable to update the expected stream's expected available capacity
    Runnable tuningStreamQueueSize = new Runnable() {


      @Override
      public void run() {
        inStreamQueue.updateExpectedAvailableCapacity();
        outStreamQueue.updateExpectedAvailableCapacity();
        gatewayLooper.registerTimerEventInNanoSeconds(instanceTuningIntervalMs, this);
      }
    };
    gatewayLooper.registerTimerEventInSeconds(systemConfig.getInstanceMetricsSystemSampleIntervalSec(),
        tuningStreamQueueSize);
  }

  @Override
  public void run() {
    Thread.currentThread().setName(Constants.THREAD_GATEWAY_NAME);

    streamManagerClient.start();
    metricsManagerClient.start();

    gatewayLooper.loop();
  }

  public void close() {
    LOG.info("Closing the Gateway thread");
    this.gatewayMetricsCollector.forceGatherAllMetrics();

    this.metricsManagerClient.sendAllMessage();
    this.streamManagerClient.sendAllMessage();

    this.metricsManagerClient.stop();
    this.streamManagerClient.stop();
  }
}
