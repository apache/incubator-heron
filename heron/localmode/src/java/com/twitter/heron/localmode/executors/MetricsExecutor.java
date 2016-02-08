package com.twitter.heron.localmode.executors;

import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

import com.twitter.heron.common.config.SystemConfig;
import com.twitter.heron.common.core.base.Communicator;
import com.twitter.heron.common.core.base.SlaveLooper;
import com.twitter.heron.common.core.base.WakeableLooper;
import com.twitter.heron.common.utils.metrics.JVMMetrics;
import com.twitter.heron.common.utils.metrics.MetricsCollector;
import com.twitter.heron.proto.system.Metrics;

/**
 * MetricsExecutor would run in a separate thread via WakeableLooper,
 * and block until it is waken up by metrics pushed from other InstanceExecutor.
 * <p/>
 * Then it would look up all InstanceExecutor added and invoke
 * handleExecutorsMetrics(InstanceExecutor instance) to handle the metrics
 */
public class MetricsExecutor implements Runnable {
  private static final Logger LOG = Logger.getLogger(InstanceExecutor.class.getName());

  private final List<InstanceExecutor> instanceExecutors;

  private final WakeableLooper looper;

  // MetricsCollector used to collect internal metrics of MetricsExecutor
  private final MetricsCollector metricsCollector;
  // Communicator to be bind with MetricsCollector to collect metrics
  private final Communicator<Metrics.MetricPublisherPublishMessage> metricsQueue;

  private final SystemConfig systemConfig;

  private final String executorId = "LocalMode_Metrics_Executor";

  public MetricsExecutor(SystemConfig systemConfig) {
    instanceExecutors = new LinkedList<>();
    looper = createWakeableLooper();

    this.metricsQueue =
        new Communicator<Metrics.MetricPublisherPublishMessage>(null, this.looper);
    this.metricsCollector = new MetricsCollector(this.looper, metricsQueue);

    this.systemConfig = systemConfig;
  }

  public void addInstanceExecutor(InstanceExecutor instanceExecutor) {
    // Set the InstanceExecutor's metricsOutQueue's consumer
    instanceExecutor.getMetricsOutQueue().setConsumer(looper);

    instanceExecutors.add(instanceExecutor);
  }

  private void setupJVMMetrics() {
    JVMMetrics jvmMetrics = new JVMMetrics();
    jvmMetrics.registerMetrics(metricsCollector);

    // Attach sample Runnable to gatewayMetricsCollector
    this.metricsCollector.registerMetricSampleRunnable(jvmMetrics.getJVMSampleRunnable(),
        systemConfig.getHeronMetricsExportIntervalSec() / 2);
  }

  @Override
  public void run() {
    Thread.currentThread().setName(executorId);

    LOG.info("Metrics_Executor starts");

    setupJVMMetrics();
    addMetricsExecutorTasks();
    looper.loop();
  }

  public void stop() {
    looper.exitLoop();
  }

  protected void addMetricsExecutorTasks() {

    Runnable metricsExecutorsTasks = new Runnable() {
      @Override
      public void run() {
        for (InstanceExecutor instance : instanceExecutors) {
          handleExecutorsMetrics(instance);
        }

        // Handle internal metrics
        while (!metricsQueue.isEmpty()) {
          handleMetricPublisherPublishMessage(executorId, metricsQueue.poll());
        }
      }
    };

    looper.addTasksOnWakeup(metricsExecutorsTasks);
  }

  protected void handleExecutorsMetrics(InstanceExecutor instance) {
    // TODO(mfu): We might also need to handle the instance's info
    while (!instance.getMetricsOutQueue().isEmpty()) {
      handleMetricPublisherPublishMessage(instance.getInstanceId(), instance.getMetricsOutQueue().poll());
    }
  }

  protected void handleMetricPublisherPublishMessage(String instanceId, Metrics.MetricPublisherPublishMessage message) {
    // TODO(mfu): Currently we just log the metrics; would add more handling in future
    LOG.info(
        String.format("Metrics from %s at time %s:\n%s",
            instanceId,
            System.currentTimeMillis(),
            message.toString()));
  }

  protected WakeableLooper createWakeableLooper() {
    return new SlaveLooper();
  }
}
