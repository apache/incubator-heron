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

package org.apache.heron.api.metric;

import java.util.HashMap;
import java.util.Map;
import java.util.TimerTask;

import org.apache.heron.api.utils.Utils;

/**
 * Acts as a Latency Metric, but also keeps track of approximate latency
 * for the last 10 mins, 3 hours, 1 day, and all time.
 */
public class LatencyStatAndMetric implements IMetric<Double> {
  //The current lat and count buckets are protected by a different lock
  // from the other buckets.  This is to reduce the lock contention
  // When doing complex calculations.  Never grab the instance object lock
  // while holding currentLock to avoid deadlocks
  private final Object currentLock = new byte[0];
  private long currentLatBucket;
  private long currentCountBucket;

  // All internal state except for the current buckets are
  // protected using the Object Lock
  private long bucketStart;

  //exact variable time, that is added to the current bucket
  private long exactExtraLat;
  private long exactExtraCount;

  //10 min values
  private final int tmSize;
  private final long[] tmLatBuckets;
  private final long[] tmCountBuckets;
  private final long[] tmTime;

  //3 hour values
  private final int thSize;
  private final long[] thLatBuckets;
  private final long[] thCountBuckets;
  private final long[] thTime;

  //1 day values
  private final int odSize;
  private final long[] odLatBuckets;
  private final long[] odCountBuckets;
  private final long[] odTime;

  //all time
  private long allTimeLat;
  private long allTimeCount;

  private final TimerTask task;

  /**
   *
   * Constructor
   *
   * @param numBuckets the number of buckets to divide the time periods into.
   */
  public LatencyStatAndMetric(int numBuckets) {
    this(numBuckets, -1);
  }

  /**
   * Constructor
   *
   * @param numBuckets the number of buckets to divide the time periods into.
   * @param startTime if positive the simulated time to start the from.
   */
  LatencyStatAndMetric(int numBuckets, long startTime) {
    int numBucketsCorrected = Math.max(numBuckets, 2);
    //We want to capture the full time range, so the target size is as
    // if we had one bucket less, then we do
    tmSize = 10 * 60 * 1000 / (numBucketsCorrected - 1);
    thSize = 3 * 60 * 60 * 1000 / (numBucketsCorrected - 1);
    odSize = 24 * 60 * 60 * 1000 / (numBucketsCorrected - 1);
    if (tmSize < 1 || thSize < 1 || odSize < 1) {
      throw new IllegalArgumentException("number of buckets is too large to be supported");
    }
    tmLatBuckets = new long[numBucketsCorrected];
    tmCountBuckets = new long[numBucketsCorrected];
    tmTime = new long[numBucketsCorrected];
    thLatBuckets = new long[numBucketsCorrected];
    thCountBuckets = new long[numBucketsCorrected];
    thTime = new long[numBucketsCorrected];
    odLatBuckets = new long[numBucketsCorrected];
    odCountBuckets = new long[numBucketsCorrected];
    odTime = new long[numBucketsCorrected];
    allTimeLat = 0;
    allTimeCount = 0;
    exactExtraLat = 0;
    exactExtraCount = 0;

    bucketStart = startTime >= 0 ? startTime : System.currentTimeMillis();
    currentLatBucket = 0;
    currentCountBucket = 0;
    if (startTime < 0) {
      task = new Fresher();
      MetricStatTimer.timer.scheduleAtFixedRate(task, tmSize, tmSize);
    } else {
      task = null;
    }
  }

  /**
   * Record a specific latency
   *
   * @param latency what we are recording
   */
  public void record(long latency) {
    synchronized (currentLock) {
      currentLatBucket += latency;
      currentCountBucket++;
    }
  }

  @Override
  public synchronized Double getValueAndReset() {
    return getValueAndReset(System.currentTimeMillis());
  }

  synchronized Double getValueAndReset(long now) {
    long lat;
    long count;
    synchronized (currentLock) {
      lat = currentLatBucket;
      count = currentCountBucket;
      currentLatBucket = 0;
      currentCountBucket = 0;
    }

    long timeSpent = now - bucketStart;
    long exactExtraCountSum = count + exactExtraCount;
    double ret = Utils.zeroIfNaNOrInf(
        ((double) (lat + exactExtraLat)) / exactExtraCountSum);
    bucketStart = now;
    exactExtraLat = 0;
    exactExtraCount = 0;
    rotateBuckets(lat, count, timeSpent);
    return ret;
  }

  synchronized void rotateSched(long now) {
    long lat;
    long count;
    synchronized (currentLock) {
      lat = currentLatBucket;
      count = currentCountBucket;
      currentLatBucket = 0;
      currentCountBucket = 0;
    }

    long timeSpent = now - bucketStart;
    exactExtraLat += lat;
    exactExtraCount += count;
    bucketStart = now;
    rotateBuckets(lat, count, timeSpent);
  }

  synchronized void rotateBuckets(long lat, long count, long timeSpent) {
    rotate(lat, count, timeSpent, tmSize, tmTime, tmLatBuckets, tmCountBuckets);
    rotate(lat, count, timeSpent, thSize, thTime, thLatBuckets, thCountBuckets);
    rotate(lat, count, timeSpent, odSize, odTime, odLatBuckets, odCountBuckets);
    allTimeLat += lat;
    allTimeCount += count;
  }

  private synchronized void rotate(long lat, long count, long timeSpent, long targetSize,
                                   long[] times, long[] latBuckets, long[] countBuckets) {
    times[0] += timeSpent;
    latBuckets[0] += lat;
    countBuckets[0] += count;

    long currentTime = 0;
    long currentLat = 0;
    long currentCount = 0;
    if (times[0] >= targetSize) {
      for (int i = 0; i < latBuckets.length; i++) {
        long tmpTime = times[i];
        times[i] = currentTime;
        currentTime = tmpTime;

        long lt = latBuckets[i];
        latBuckets[i] = currentLat;
        currentLat = lt;

        long cnt = countBuckets[i];
        countBuckets[i] = currentCount;
        currentCount = cnt;
      }
    }
  }

  /**
   *
   * Get time latency average
   *
   * @return a map of time window to average latency.
   * Keys are "600" for last 10 mins
   * "10800" for the last 3 hours
   * "86400" for the last day
   * ":all-time" for all time
   */
  public synchronized Map<String, Double> getTimeLatAvg() {
    return getTimeLatAvg(System.currentTimeMillis());
  }

  synchronized Map<String, Double> getTimeLatAvg(long now) {
    Map<String, Double> ret = new HashMap<>();
    long lat;
    long count;
    synchronized (currentLock) {
      lat = currentLatBucket;
      count = currentCountBucket;
    }
    long timeSpent = now - bucketStart;
    ret.put("600", readApproximateLatAvg(lat, count, timeSpent, tmTime,
        tmLatBuckets, tmCountBuckets, 600 * 1000));
    ret.put("10800", readApproximateLatAvg(lat, count, timeSpent, thTime,
        thLatBuckets, thCountBuckets, 10800 * 1000));
    ret.put("86400", readApproximateLatAvg(lat, count, timeSpent, odTime,
        odLatBuckets, odCountBuckets, 86400 * 1000));
    long allTimeCountSum = count + allTimeCount;
    ret.put(":all-time", Utils.zeroIfNaNOrInf(
        (double) lat + allTimeLat) / allTimeCountSum);
    return ret;
  }

  double readApproximateLatAvg(long lat, long count, long timeSpent, long[] bucketTime,
                               long[] latBuckets, long[] countBuckets, long desiredTime) {
    long timeNeeded = desiredTime - timeSpent;
    long totalLat = lat;
    long totalCount = count;
    for (int i = 0; i < bucketTime.length && timeNeeded > 0; i++) {
      //Don't pro-rate anything, it is all approximate so an extra bucket is not that bad.
      totalLat += latBuckets[i];
      totalCount += countBuckets[i];
      timeNeeded -= bucketTime[i];
    }
    return Utils.zeroIfNaNOrInf(((double) totalLat) / totalCount);
  }

  public void close() {
    if (task != null) {
      task.cancel();
    }
  }

  private class Fresher extends TimerTask {
    public void run() {
      rotateSched(System.currentTimeMillis());
    }
  }
}
