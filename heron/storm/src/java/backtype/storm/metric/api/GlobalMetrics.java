package backtype.storm.metric.api;

public class GlobalMetrics {
  /**
   * Not thread safe increment of counterName. Counter doesn't exist unless incremented once
   */
  public static void incr(String counterName) {
    com.twitter.heron.api.metric.GlobalMetrics.incr(counterName);
  }

  /**
   * Not thread safe 'incrementing by' of counterName. Counter doesn't exist unless incremented once
   */
  public static void incrBy(String counterName, int N) {
    com.twitter.heron.api.metric.GlobalMetrics.incrBy(counterName, N);
  }

  /**
   * Thread safe created increment of counterName. (Slow)
   */
  public static void safeIncr(String counterName) {
    com.twitter.heron.api.metric.GlobalMetrics.safeIncr(counterName);
  }

  /**
   * Thread safe created increment of counterName. (Slow)
   */
  public static void safeIncrBy(String counterName, int N) {
    com.twitter.heron.api.metric.GlobalMetrics.safeIncrBy(counterName, N);
  }
}
