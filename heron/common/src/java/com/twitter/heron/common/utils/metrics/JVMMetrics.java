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

package com.twitter.heron.common.utils.metrics;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.List;

import com.twitter.heron.api.metric.AssignableMetric;
import com.twitter.heron.api.metric.MeanReducer;
import com.twitter.heron.api.metric.MeanReducerState;
import com.twitter.heron.api.metric.MultiAssignableMetric;
import com.twitter.heron.api.metric.ReducedMetric;
import com.twitter.heron.common.basics.Constants;
import com.twitter.heron.common.basics.SingletonRegistry;
import com.twitter.heron.common.config.SystemConfig;
import com.twitter.heron.common.utils.misc.ThreadNames;

/**
 * JVM metrics to be collected
 */
public class JVMMetrics {
  private final Runtime runtime = Runtime.getRuntime();
  private final MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
  private final RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
  private final OperatingSystemMXBean osMbean = ManagementFactory.getOperatingSystemMXBean();
  private final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
  private final List<MemoryPoolMXBean> memoryPoolMXBeanList =
      ManagementFactory.getMemoryPoolMXBeans();
  private final List<BufferPoolMXBean> bufferPoolMXBeanList =
      ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);

  // Metric for time spent in GC per generational collection, and the sum total of all collections.
  private final MultiAssignableMetric<Long> jvmGCTimeMsPerGCType;

  // Metrics for count of GC per generational collection, and the sum total of all collections.
  private final MultiAssignableMetric<Long> jvmGCCountPerGCType;

  // Metric for total live jvm threads
  private final AssignableMetric<Integer> jvmThreadCount;

  // Metric for total live jvm daemon threads
  private final AssignableMetric<Integer> jvmDaemonThreadCount;

  // Metric for number of open file descriptors
  private final AssignableMetric<Long> fdCount;

  // Metric for max file descriptors allowed per jvm process
  private final AssignableMetric<Long> fdLimit;

  // The accumulated time spending on Garbage Collection in MilliSeconds
  private AssignableMetric<Long> jvmGCTimeMs;

  // The accumulated account of JVM Garbage Collection
  private AssignableMetric<Long> jvmGCCount;

  // The JVM up times
  private AssignableMetric<Long> jvmUpTimeSecs;

  /*
   * Returns the CPU time used by the process on which the Java virtual machine is running in nanoseconds.
   * The value is of nanoseconds precision but not necessarily nanoseconds accuracy.
   */
  private AssignableMetric<Long> processCPUTimeNs;

  /*
   * Returns the total CPU time for a thread of the specified ID in nanoseconds.
   * The returned value is of nanoseconds precision but not necessarily nanoseconds accuracy.
   * If the implementation distinguishes between user mode time and system mode time,
   * the returned CPU time is the amount of time that the thread has executed in user mode
   * or system mode.
   * If the thread of the specified ID is not alive or does not exist,
   * this method returns -1. If CPU time measurement is disabled, this method returns -1.
   * A thread is alive if it has been started and has not yet died.
   * <p/>
   * If CPU time measurement is enabled after the thread has started,
   * the Java virtual machine implementation may choose any time up to and including the
   * time that the capability is enabled as the point where CPU time measurement starts.
   */
  private MultiAssignableMetric<Long> threadsCPUTimeNs;

  // The cpu time used by threads other than SlaveThread and GatewayThread
  private AssignableMetric<Long> otherThreadsCPUTimeNs;

  /*
   * Returns the CPU time that a thread of the specified ID has executed in user mode in nanosecs.
   * The returned value is of nanoseconds precision but not necessarily nanoseconds accuracy.
   * If the thread of the specified ID is not alive or does not exist, this method returns -1.
   * If CPU time measurement is disabled, this method returns -1.
   * A thread is alive if it has been started and has not yet died.
   * <p/>
   * If CPU time measurement is enabled after the thread has started,
   * the Java virtual machine implementation may choose any time up to and including the
   * time that the capability is enabled as the point where CPU time measurement starts.
   */
  private MultiAssignableMetric<Long> threadsUserCPUTimeNs;

  // The user cpu time used by threads other than SlaveThread and GatewayThread
  private AssignableMetric<Long> otherThreadsUserCPUTimeNs;

  /*
   * The "recent cpu usage" for the Java Virtual Machine process.
   * This value is a double in the [0.0,1.0] interval.
   * A value of 0.0 means that none of the CPUs were running threads from the JVM process
   * during the recent period of time observed,
   * while a value of 1.0 means that all CPUs were actively running threads from the JVM
   * 100% of the time during the recent period being observed.
   * Threads from the JVM include the application threads as well as the JVM internal threads.
   * All values betweens 0.0 and 1.0 are possible depending of the activities going on in
   * the JVM process and the whole system. If the Java Virtual Machine recent CPU usage is
   * not available, the method returns a negative value.
   */
  private ReducedMetric<MeanReducerState, Number, Double> processCPULoad;

  // Metrics that measure memory, memory's heap and memory's non-heap
  private ReducedMetric<MeanReducerState, Number, Double> jvmMemoryFreeMB;
  private ReducedMetric<MeanReducerState, Number, Double> jvmMemoryUsedMB;
  private ReducedMetric<MeanReducerState, Number, Double> jvmMemoryTotalMB;
  private ReducedMetric<MeanReducerState, Number, Double> jvmMemoryHeapUsedMB;
  private ReducedMetric<MeanReducerState, Number, Double> jvmMemoryHeapCommittedMB;
  private ReducedMetric<MeanReducerState, Number, Double> jvmMemoryHeapMaxMB;
  private ReducedMetric<MeanReducerState, Number, Double> jvmMemoryNonHeapUsedMB;
  private ReducedMetric<MeanReducerState, Number, Double> jvmMemoryNonHeapCommittedMB;
  private ReducedMetric<MeanReducerState, Number, Double> jvmMemoryNonHeapMaxMB;

  // Gather metrics for different memory pools in heap, for instance:
  // Par Eden Space, Par Survivor Space, CMS Old Gen, CMS Perm Gen

  // The peak memory usage of a memory pool since the Java virtual machine was started
  // or since the peak was reset.
  private MultiAssignableMetric<Long> jvmPeakUsagePerMemoryPool;

  // The memory usage after the Java virtual machine most recently expended effort in recycling
  // unused objects in a memory pool.
  private MultiAssignableMetric<Long> jvmCollectionUsagePerMemoryPool;

  // An estimate of the memory usage of a memory pool.
  private MultiAssignableMetric<Long> jvmEstimatedUsagePerMemoryPool;

  /*
   * Metrics for mapped and direct buffer pool usage.
   */
  private MultiAssignableMetric<Long> jvmBufferPoolMemoryUsage;

  public JVMMetrics() {
    jvmGCTimeMs = new AssignableMetric<>(0L);
    jvmGCCount = new AssignableMetric<>(0L);

    jvmGCCountPerGCType = new MultiAssignableMetric<>(0L);
    jvmGCTimeMsPerGCType = new MultiAssignableMetric<>(0L);

    jvmUpTimeSecs = new AssignableMetric<>(0L);

    jvmThreadCount = new AssignableMetric<>(0);
    jvmDaemonThreadCount = new AssignableMetric<>(0);
    processCPUTimeNs = new AssignableMetric<>(0L);
    threadsCPUTimeNs = new MultiAssignableMetric<>(0L);
    otherThreadsCPUTimeNs = new AssignableMetric<>(0L);
    threadsUserCPUTimeNs = new MultiAssignableMetric<>(0L);
    otherThreadsUserCPUTimeNs = new AssignableMetric<>(0L);

    processCPULoad = new ReducedMetric<>(new MeanReducer());

    fdCount = new AssignableMetric<>(0L);
    fdLimit = new AssignableMetric<>(0L);

    jvmMemoryFreeMB = new ReducedMetric<>(new MeanReducer());
    jvmMemoryUsedMB = new ReducedMetric<>(new MeanReducer());
    jvmMemoryTotalMB = new ReducedMetric<>(new MeanReducer());
    jvmMemoryHeapUsedMB = new ReducedMetric<>(new MeanReducer());
    jvmMemoryHeapCommittedMB = new ReducedMetric<>(new MeanReducer());
    jvmMemoryHeapMaxMB = new ReducedMetric<>(new MeanReducer());
    jvmMemoryNonHeapUsedMB = new ReducedMetric<>(new MeanReducer());
    jvmMemoryNonHeapCommittedMB = new ReducedMetric<>(new MeanReducer());
    jvmMemoryNonHeapMaxMB = new ReducedMetric<>(new MeanReducer());

    jvmPeakUsagePerMemoryPool = new MultiAssignableMetric<>(0L);
    jvmCollectionUsagePerMemoryPool = new MultiAssignableMetric<>(0L);
    jvmEstimatedUsagePerMemoryPool = new MultiAssignableMetric<>(0L);

    jvmBufferPoolMemoryUsage = new MultiAssignableMetric<>(0L);
  }

  /**
   * Register metrics with the metrics collector
   */
  public void registerMetrics(MetricsCollector metricsCollector) {
    SystemConfig systemConfig = (SystemConfig) SingletonRegistry.INSTANCE.getSingleton(
        SystemConfig.HERON_SYSTEM_CONFIG);

    int interval = systemConfig.getHeronMetricsExportIntervalSec();

    metricsCollector.registerMetric("__jvm-gc-collection-time-ms", jvmGCTimeMs, interval);
    metricsCollector.registerMetric("__jvm-gc-collection-count", jvmGCCount, interval);

    metricsCollector.registerMetric("__jvm-gc-time-ms", jvmGCTimeMsPerGCType, interval);
    metricsCollector.registerMetric("__jvm-gc-count", jvmGCCountPerGCType, interval);

    metricsCollector.registerMetric("__jvm-uptime-secs", jvmUpTimeSecs, interval);

    metricsCollector.registerMetric("__jvm-thread-count", jvmThreadCount, interval);
    metricsCollector.registerMetric("__jvm-daemon-thread-count", jvmDaemonThreadCount, interval);
    metricsCollector.registerMetric("__jvm-process-cpu-time-nanos", processCPUTimeNs, interval);
    metricsCollector.registerMetric("__jvm-threads-cpu-time-nanos", threadsCPUTimeNs, interval);
    metricsCollector.registerMetric(
        "__jvm-other-threads-cpu-time-nanos", otherThreadsCPUTimeNs, interval);
    metricsCollector.registerMetric(
        "__jvm-threads-user-cpu-time-nanos", threadsUserCPUTimeNs, interval);
    metricsCollector.registerMetric(
        "__jvm-other-threads-user-cpu-time-nanos", otherThreadsUserCPUTimeNs, interval);
    metricsCollector.registerMetric("__jvm-process-cpu-load", processCPULoad, interval);

    metricsCollector.registerMetric("__jvm-fd-count", fdCount, interval);
    metricsCollector.registerMetric("__jvm-fd-limit", fdLimit, interval);

    metricsCollector.registerMetric("__jvm-memory-free-mb", jvmMemoryFreeMB, interval);
    metricsCollector.registerMetric("__jvm-memory-used-mb", jvmMemoryUsedMB, interval);
    metricsCollector.registerMetric("__jvm-memory-mb-total", jvmMemoryTotalMB, interval);
    metricsCollector.registerMetric("__jvm-memory-heap-mb-used", jvmMemoryHeapUsedMB, interval);
    metricsCollector.registerMetric(
        "__jvm-memory-heap-mb-committed", jvmMemoryHeapCommittedMB, interval);
    metricsCollector.registerMetric("__jvm-memory-heap-mb-max", jvmMemoryHeapMaxMB, interval);
    metricsCollector.registerMetric(
        "__jvm-memory-non-heap-mb-used", jvmMemoryNonHeapUsedMB, interval);
    metricsCollector.registerMetric(
        "__jvm-memory-non-heap-mb-committed", jvmMemoryNonHeapCommittedMB, interval);
    metricsCollector.registerMetric(
        "__jvm-memory-non-heap-mb-max", jvmMemoryNonHeapMaxMB, interval);

    metricsCollector.registerMetric(
        "__jvm-peak-usage", jvmPeakUsagePerMemoryPool, interval);
    metricsCollector.registerMetric(
        "__jvm-collection-usage", jvmCollectionUsagePerMemoryPool, interval);
    metricsCollector.registerMetric(
        "__jvm-estimated-usage", jvmEstimatedUsagePerMemoryPool, interval);

    metricsCollector.registerMetric("__jvm-buffer-pool", jvmBufferPoolMemoryUsage, interval);
  }

  public Runnable getJVMSampleRunnable() {
    final Runnable sampleRunnable = new Runnable() {
      @Override
      public void run() {
        updateGcMetrics();

        jvmUpTimeSecs.setValue(runtimeMXBean.getUptime() / Constants.SECONDS_TO_MILLISECONDS);

        processCPUTimeNs.setValue(getProcessCPUTimeNs());
        getThreadsMetrics();

        // We multiple # of processors to measure a process cpu load based on cores rather than
        // overall machine
        processCPULoad.update(getProcessCPULoad() * runtime.availableProcessors());

        updateFdMetrics();

        updateMemoryPoolMetrics();
        updateBufferPoolMetrics();

        long freeMemory = runtime.freeMemory();
        long totalMemory = runtime.totalMemory();
        jvmMemoryFreeMB.update(freeMemory / Constants.MB_TO_BYTES);
        jvmMemoryTotalMB.update(totalMemory / Constants.MB_TO_BYTES);
        jvmMemoryUsedMB.update((totalMemory - freeMemory) / Constants.MB_TO_BYTES);
        jvmMemoryHeapUsedMB.update(
            memoryBean.getHeapMemoryUsage().getUsed() / Constants.MB_TO_BYTES);
        jvmMemoryHeapCommittedMB.update(
            memoryBean.getHeapMemoryUsage().getCommitted() / Constants.MB_TO_BYTES);
        jvmMemoryHeapMaxMB.update(
            memoryBean.getHeapMemoryUsage().getMax() / Constants.MB_TO_BYTES);
        jvmMemoryNonHeapUsedMB.update(
            memoryBean.getNonHeapMemoryUsage().getUsed() / Constants.MB_TO_BYTES);
        jvmMemoryNonHeapCommittedMB.update(
            memoryBean.getNonHeapMemoryUsage().getCommitted() / Constants.MB_TO_BYTES);
        jvmMemoryNonHeapMaxMB.update(
            memoryBean.getNonHeapMemoryUsage().getMax() / Constants.MB_TO_BYTES);
      }
    };
    return sampleRunnable;
  }

  // Gather metrics related to both direct and mapped byte buffers in the jvm.
  // These metrics can be useful for diagnosing native memory usage.
  private void updateBufferPoolMetrics() {
    for (BufferPoolMXBean bufferPoolMXBean : bufferPoolMXBeanList) {
      String normalizedKeyName = bufferPoolMXBean.getName().replaceAll("[^\\w]", "-");

      final long memoryUsedMB = bufferPoolMXBean.getMemoryUsed() / Constants.MB_TO_BYTES;
      final long totalCapacityMB = bufferPoolMXBean.getTotalCapacity() / Constants.MB_TO_BYTES;
      final long countMB = bufferPoolMXBean.getCount() / Constants.MB_TO_BYTES;

      // The estimated memory the JVM is using for this buffer pool
      jvmBufferPoolMemoryUsage.safeScope(normalizedKeyName + "-memory-used").setValue(memoryUsedMB);

      // The estimated total capacity of the buffers in this pool
      jvmBufferPoolMemoryUsage.safeScope(
          normalizedKeyName + "-total-capacity").setValue(totalCapacityMB);

      // THe estimated number of buffers in this pool
      jvmBufferPoolMemoryUsage.safeScope(normalizedKeyName + "-count").setValue(countMB);
    }
  }

  // Gather metrics for different memory pools in heap, for instance:
  // Par Eden Space, Par Survivor Space, CMS Old Gen, CMS Perm Gen
  private void updateMemoryPoolMetrics() {
    for (MemoryPoolMXBean memoryPoolMXBean : memoryPoolMXBeanList) {
      String normalizedKeyName = memoryPoolMXBean.getName().replaceAll("[^\\w]", "-");
      MemoryUsage peakUsage = memoryPoolMXBean.getPeakUsage();
      if (peakUsage != null) {
        jvmPeakUsagePerMemoryPool.safeScope(
            normalizedKeyName + "-used").setValue(peakUsage.getUsed() / Constants.MB_TO_BYTES);
        jvmPeakUsagePerMemoryPool.safeScope(
            normalizedKeyName + "-committed").setValue(
                peakUsage.getCommitted() / Constants.MB_TO_BYTES);
        jvmPeakUsagePerMemoryPool.safeScope(
            normalizedKeyName + "-max").setValue(peakUsage.getMax() / Constants.MB_TO_BYTES);
      }

      MemoryUsage collectionUsage = memoryPoolMXBean.getCollectionUsage();
      if (collectionUsage != null) {
        jvmCollectionUsagePerMemoryPool.safeScope(
            normalizedKeyName + "-used").setValue(
                collectionUsage.getUsed() / Constants.MB_TO_BYTES);
        jvmCollectionUsagePerMemoryPool.safeScope(
            normalizedKeyName + "-committed").setValue(
                collectionUsage.getCommitted() / Constants.MB_TO_BYTES);
        jvmCollectionUsagePerMemoryPool.safeScope(
            normalizedKeyName + "-max").setValue(
                collectionUsage.getMax() / Constants.MB_TO_BYTES);
      }

      MemoryUsage estimatedUsage = memoryPoolMXBean.getUsage();
      if (estimatedUsage != null) {
        jvmEstimatedUsagePerMemoryPool.safeScope(
            normalizedKeyName + "-used").setValue(estimatedUsage.getUsed() / Constants.MB_TO_BYTES);
        jvmEstimatedUsagePerMemoryPool.safeScope(
            normalizedKeyName + "-committed").setValue(
                estimatedUsage.getCommitted() / Constants.MB_TO_BYTES);
        jvmEstimatedUsagePerMemoryPool.safeScope(
            normalizedKeyName + "-max").setValue(estimatedUsage.getMax() / Constants.MB_TO_BYTES);
      }
    }
  }

  private void getThreadsMetrics() {
    // Set the cpu usage for every single thread
    if (threadMXBean.isThreadCpuTimeSupported()) {
      threadMXBean.setThreadCpuTimeEnabled(true);

      long tmpOtherThreadsCpuTime = 0;
      long tmpOtherThreadsUserCpuTime = 0;

      for (long id : threadMXBean.getAllThreadIds()) {
        long cpuTime = threadMXBean.getThreadCpuTime(id);
        long cpuUserTime = threadMXBean.getThreadUserTime(id);

        ThreadInfo threadInfo = threadMXBean.getThreadInfo(id);
        if (threadInfo != null) {
          String threadName = threadInfo.getThreadName();

          if (threadName.equals(ThreadNames.THREAD_GATEWAY_NAME)
              || threadName.equals(ThreadNames.THREAD_SLAVE_NAME)) {
            threadsCPUTimeNs.scope(threadName).setValue(cpuTime);
            threadsUserCPUTimeNs.scope(threadName).setValue(cpuUserTime);
          } else {
            tmpOtherThreadsCpuTime += cpuTime;
            tmpOtherThreadsUserCpuTime += cpuUserTime;
          }
        }
      }

      otherThreadsCPUTimeNs.setValue(tmpOtherThreadsCpuTime);
      otherThreadsUserCPUTimeNs.setValue(tmpOtherThreadsUserCpuTime);
      jvmThreadCount.setValue(threadMXBean.getThreadCount());
      jvmDaemonThreadCount.setValue(threadMXBean.getDaemonThreadCount());
    }
  }

  //All gc related metrics should be updated here
  private void updateGcMetrics() {
    updateGcTimes();
    updateGcCounts();
  }

  private void updateGcTimes() {
    long totalTimeMs = 0;
    for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
      long collectionTimeMs = bean.getCollectionTime();
      totalTimeMs += collectionTimeMs;
      // Replace all non alpha-numeric characters to '-'
      String normalizedKeyName = bean.getName().replaceAll("[^\\w]", "-");
      jvmGCTimeMsPerGCType.safeScope(normalizedKeyName).setValue(collectionTimeMs);
    }
    jvmGCTimeMs.setValue(totalTimeMs);
  }

  private void updateGcCounts() {
    long totalCount = 0;
    for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
      long collectionCount = bean.getCollectionCount();
      totalCount += collectionCount;
      // Replace all non alpha-numeric characters to '-'
      String normalizedKeyName = bean.getName().replaceAll("[^\\w]", "-");
      jvmGCCountPerGCType.safeScope(normalizedKeyName).setValue(collectionCount);
    }
    jvmGCCount.setValue(totalCount);
  }

  private long getProcessCPUTimeNs() {
    if (osMbean instanceof com.sun.management.OperatingSystemMXBean) {
      final com.sun.management.OperatingSystemMXBean sunOsMbean =
          (com.sun.management.OperatingSystemMXBean) osMbean;
      return sunOsMbean.getProcessCpuTime();
    }

    return -1;
  }

  private double getProcessCPULoad() {
    if (osMbean instanceof com.sun.management.OperatingSystemMXBean) {
      final com.sun.management.OperatingSystemMXBean sunOsMbean =
          (com.sun.management.OperatingSystemMXBean) osMbean;
      return sunOsMbean.getProcessCpuLoad();
    }

    return -1;
  }

  //Update file descriptor metrics
  private void updateFdMetrics() {
    if (osMbean instanceof com.sun.management.UnixOperatingSystemMXBean) {
      final com.sun.management.UnixOperatingSystemMXBean unix =
          (com.sun.management.UnixOperatingSystemMXBean) osMbean;

      fdCount.setValue(unix.getOpenFileDescriptorCount());
      fdLimit.setValue(unix.getMaxFileDescriptorCount());
    }
  }
}
