package com.twitter.heron.api.metric;


public interface IMetricsRegister {
  /*
 * Register a IMetric instance.
 * Heron will then call getValueAndReset on the metric every timeBucketSizeInSecs
 * and the returned value is sent to all metrics consumers.
 * You must call this during IBolt::prepare or ISpout::open.
 * @return The IMetric argument unchanged.
 */
  public <T extends IMetric> T registerMetric(String name, T metric, int timeBucketSizeInSecs);
}
