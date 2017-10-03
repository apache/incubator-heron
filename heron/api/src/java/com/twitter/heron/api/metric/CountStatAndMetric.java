//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package com.twitter.heron.api.metric;

import java.util.HashMap;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Acts as a Count Metric, but also keeps track of approximate counts
 * for the last 10 mins, 3 hours, 1 day, and all time.
 */
public class CountStatAndMetric implements IMetric<Long> {
  private final AtomicLong currentBucket;
  // All internal state except for the count of the current bucket are
  // protected using a lock on this counter
  private long bucketStart;

  //exact variable time, that is added to the current bucket
  private long exactExtra;

  //10 min values
  private final int tmSize;
  private final long[] tmBuckets;
  private final long[] tmTime;

  //3 hour values
  private final int thSize;
  private final long[] thBuckets;
  private final long[] thTime;

  //1 day values
  private final int odSize;
  private final long[] odBuckets;
  private final long[] odTime;

  //all time
  private long allTime;

  private final TimerTask task;

  /**
   *  Constructor
   *
   * @param numBuckets the number of buckets to divide the time periods into.
   */
  public CountStatAndMetric(int numBuckets) {
    this(numBuckets, -1);
  }

  /**
   * Constructor
   *
   * @param numBuckets the number of buckets to divide the time periods into.
   * @param startTime if positive the simulated time to start the from.
   */
  CountStatAndMetric(int numBuckets, long startTime) {
    int numBucketsCorrected = Math.max(numBuckets, 2);
    //We want to capture the full time range, so the target size is as
    // if we had one bucket less, then we do
    tmSize = 10 * 60 * 1000 / (numBucketsCorrected - 1);
    thSize = 3 * 60 * 60 * 1000 / (numBucketsCorrected - 1);
    odSize = 24 * 60 * 60 * 1000 / (numBucketsCorrected - 1);
    if (tmSize < 1 || thSize < 1 || odSize < 1) {
      throw new IllegalArgumentException("number of buckets is too large to be supported");
    }
    tmBuckets = new long[numBucketsCorrected];
    tmTime = new long[numBucketsCorrected];
    thBuckets = new long[numBucketsCorrected];
    thTime = new long[numBucketsCorrected];
    odBuckets = new long[numBucketsCorrected];
    odTime = new long[numBucketsCorrected];
    allTime = 0;
    exactExtra = 0;

    bucketStart = startTime >= 0 ? startTime : System.currentTimeMillis();
    currentBucket = new AtomicLong(0);
    if (startTime < 0) {
      task = new Fresher();
      MetricStatTimer.timer.scheduleAtFixedRate(task, tmSize, tmSize);
    } else {
      task = null;
    }
  }

  /**
   * Increase the count by the given value.
   *
   * @param count number to count
   */
  public void incBy(long count) {
    currentBucket.addAndGet(count);
  }


  @Override
  public synchronized Long getValueAndReset() {
    return getValueAndReset(System.currentTimeMillis());
  }

  synchronized Long getValueAndReset(long now) {
    long value = currentBucket.getAndSet(0);
    long timeSpent = now - bucketStart;
    long ret = value + exactExtra;
    bucketStart = now;
    exactExtra = 0;
    rotateBuckets(value, timeSpent);
    return ret;
  }

  synchronized void rotateSched(long now) {
    long value = currentBucket.getAndSet(0);
    long timeSpent = now - bucketStart;
    exactExtra += value;
    bucketStart = now;
    rotateBuckets(value, timeSpent);
  }

  synchronized void rotateBuckets(long value, long timeSpent) {
    rotate(value, timeSpent, tmSize, tmTime, tmBuckets);
    rotate(value, timeSpent, thSize, thTime, thBuckets);
    rotate(value, timeSpent, odSize, odTime, odBuckets);
    allTime += value;
  }

  private synchronized void rotate(long value, long timeSpent,
                                   long targetSize, long[] times, long[] buckets) {
    times[0] += timeSpent;
    buckets[0] += value;

    long currentTime = 0;
    long currentVal = 0;
    if (times[0] >= targetSize) {
      for (int i = 0; i < buckets.length; i++) {
        long tmpTime = times[i];
        times[i] = currentTime;
        currentTime = tmpTime;

        long cnt = buckets[i];
        buckets[i] = currentVal;
        currentVal = cnt;
      }
    }
  }

  /**
   *
   * Get current time counts
   *
   * @return a map of time window to count.
   * Keys are "600" for last 10 mins
   * "10800" for the last 3 hours
   * "86400" for the last day
   * ":all-time" for all time
   */
  public synchronized Map<String, Long> getTimeCounts() {
    return getTimeCounts(System.currentTimeMillis());
  }

  synchronized Map<String, Long> getTimeCounts(long now) {
    Map<String, Long> ret = new HashMap<>();
    long value = currentBucket.get();
    long timeSpent = now - bucketStart;
    ret.put("600", readApproximateTime(value, timeSpent, tmTime, tmBuckets, 600 * 1000));
    ret.put("10800", readApproximateTime(value, timeSpent, thTime, thBuckets, 10800 * 1000));
    ret.put("86400", readApproximateTime(value, timeSpent, odTime, odBuckets, 86400 * 1000));
    ret.put(":all-time", value + allTime);
    return ret;
  }

  long readApproximateTime(long value, long timeSpent, long[] bucketTime,
                           long[] buckets, long desiredTime) {
    long timeNeeded = desiredTime - timeSpent;
    long total = value;
    for (int i = 0; i < bucketTime.length; i++) {
      if (timeNeeded < bucketTime[i]) {
        double pct = timeNeeded / ((double) bucketTime[i]);
        total += (long) (pct * buckets[i]);
        timeNeeded = 0;
        break;
      }
      total += buckets[i];
      timeNeeded -= bucketTime[i];
    }
    return total;
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
