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

package org.apache.heron.common.utils.logging;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.logging.Handler;
import java.util.logging.LogRecord;

import org.apache.heron.api.metric.ConcurrentCountMetric;
import org.apache.heron.api.metric.IMetric;
import org.apache.heron.common.utils.metrics.MetricsCollector;
import org.apache.heron.proto.system.Metrics;

/**
 * JUL logging handler which report any log message associated with a Throwable object to
 * persistent storage. Since error volume can be immense in worst case (e.g throwing exception in a
 * tight loop) we dedupe the trace based on first element in the throwable trace.
 */
public class ErrorReportLoggingHandler extends Handler {
  public static final String NO_TRACE = "No Trace";

  private static volatile boolean initialized = false;
  private static volatile int exceptionsLimit = Integer.MAX_VALUE;
  private static volatile ConcurrentCountMetric droppedExceptionsCount
      = new ConcurrentCountMetric();

  public ErrorReportLoggingHandler() {
    super();
  }

  public static String getExceptionLocation(String trace) {
    if (trace == null) {
      return NO_TRACE;
    }
    String[] firstLine = trace.split("\n");

    // Try to get first 2 line of exception and that will define the location of exception.
    // e.g if the exception trace is :
    // Exception in thread "main" java.lang.RuntimeException
    //   at org.apache.myproject.Foo.myfunc(Foo.java:420)
    //   at ...
    // The First 2 line will be used as location.
    // TODO: Decide if exception message (first line) should be part of location.
    if (firstLine.length == 0) {
      return NO_TRACE;
    } else if (firstLine.length == 1) {
      return firstLine[0];
    } else {
      return firstLine[0] + "\n" + firstLine[1];
    }
  }

  public static synchronized void init(MetricsCollector collector,
                                       Duration interval, int maxExceptions) {
    if (!initialized) {
      collector.registerMetric(
          "exception_info", ExceptionRepositoryAsMetrics.INSTANCE, (int) interval.getSeconds());
      collector.registerMetric(
          "dropped_exceptions_count", droppedExceptionsCount, (int) interval.getSeconds());
      exceptionsLimit = maxExceptions;
    }

    initialized = true;
  }

  // All the throwables are logged to in memory ExceptionRepositoryAsMetrics store. This metrics
  // will flush the exception to metrics manager during getValueAndReset call.
  @Override
  public void publish(LogRecord record) {
    // Convert Log
    Throwable throwable = record.getThrown();
    if (throwable != null) {
      synchronized (ExceptionRepositoryAsMetrics.INSTANCE) {
        // We would not include the message if already exceeded the exceptions limit
        if (ExceptionRepositoryAsMetrics.INSTANCE.getExceptionsCount() >= exceptionsLimit) {
          droppedExceptionsCount.incr();
          return;
        }

        // Convert the record
        StringWriter sink = new StringWriter();
        throwable.printStackTrace(new PrintWriter(sink, true));
        String trace = sink.toString();

        Metrics.ExceptionData.Builder exceptionDataBuilder =
            ExceptionRepositoryAsMetrics.INSTANCE.getExceptionInfo(trace);
        exceptionDataBuilder.setCount(exceptionDataBuilder.getCount() + 1);
        exceptionDataBuilder.setLasttime(new Date().toString());
        exceptionDataBuilder.setStacktrace(trace);

        // Can cause NPE and crash HI
        //exceptionDataBuilder.setLogging(record.getMessage());

        if (record.getMessage() == null) {
          exceptionDataBuilder.setLogging("");
        } else {
          exceptionDataBuilder.setLogging(record.getMessage());
        }
      }
    }
  }

  @Override
  public void close() {
    flush();
  }

  @Override
  public void flush() {
    // Call getValueAndReset and hope that makes it to metrics manager. Also log to stdout.
    // Logging to stdout is much more likely to succeed it case of apocalyptic shutdown.
    System.out.print(ExceptionRepositoryAsMetrics.INSTANCE.getValue().toString());
  }

  // Exception will be stored in this Metrics. Use of metrics simplify exporting the error to
  // metrics manager.
  public enum ExceptionRepositoryAsMetrics
      implements IMetric<Collection<Metrics.ExceptionData.Builder>> {
    INSTANCE;
    private HashMap<String, Metrics.ExceptionData.Builder> exceptionStore;

    ExceptionRepositoryAsMetrics() {
      exceptionStore = new HashMap<String, Metrics.ExceptionData.Builder>();
    }

    @Override
    public Collection<Metrics.ExceptionData.Builder> getValueAndReset() {
      synchronized (ExceptionRepositoryAsMetrics.INSTANCE) {
        Collection<Metrics.ExceptionData.Builder> metricsValue = exceptionStore.values();
        exceptionStore = new HashMap<String, Metrics.ExceptionData.Builder>();
        return metricsValue;
      }
    }

    protected int getExceptionsCount() {
      return exceptionStore.size();
    }

    // Get the underneath exception info without reset
    // It could be used when we just want to check or query the content
    public Object getValue() {
      synchronized (ExceptionRepositoryAsMetrics.INSTANCE) {
        return exceptionStore.values();
      }
    }

    // Returns ExceptionData.Builder object for the trace.
    protected Metrics.ExceptionData.Builder getExceptionInfo(String trace) {
      Metrics.ExceptionData.Builder exceptionDataBuilder =
          exceptionStore.get(getExceptionLocation(trace));
      if (exceptionDataBuilder == null) {
        exceptionDataBuilder = Metrics.ExceptionData.newBuilder();
        exceptionDataBuilder.setFirsttime(new Date().toString());
        exceptionDataBuilder.setCount(0);
        exceptionDataBuilder.setStacktrace(NO_TRACE);
        exceptionStore.put(getExceptionLocation(trace), exceptionDataBuilder);
      }
      return exceptionDataBuilder;
    }
  }
}
