package com.twitter.heron.spi.metricsmgr.metrics;

/**
 * An immutable class providing a view of ExceptionInfo
 */
public class ExceptionInfo {
  private final String stackTrace;
  private final String lastTime;
  private final String firstTime;
  private final int count;
  private final String logging;

  public ExceptionInfo(String stackTrace, String lastTime, String firstTime,
                       int count, String logging) {
    this.stackTrace = stackTrace;
    this.lastTime = lastTime;
    this.firstTime = firstTime;
    this.count = count;
    this.logging = logging;
  }

  /**
   * Get stack trace of exception. First two lines of stack trace is used for aggregating exception.
   *
   * @return Stack trace of exception
   */
  public String getStackTrace() {
    return stackTrace;
  }

  /**
   * Get last time the exception occurred in the metrics collection interval
   *
   * @return Last Time the exception occurred
   */
  public String getLastTime() {
    return lastTime;
  }

  /**
   * Get first time the exception occurred in the metrics collection interval
   *
   * @return First time the exception occurred
   */
  public String getFirstTime() {
    return firstTime;
  }

  /**
   * Get number of time exception occurred in the metrics collection interval
   *
   * @return Number of time exception occurred
   */
  public int getCount() {
    return count;
  }

  /**
   * Get additional text logged.
   *
   * @return Additional text logged.
   */
  public String getLogging() {
    return logging;
  }

  @Override
  public String toString() {
    return String.format("{stack trace = %s, last time = %s, first time = %s, count = %d, logging = %s}",
        getStackTrace(), getLastTime(), getFirstTime(), getCount(), getLogging());
  }
}
