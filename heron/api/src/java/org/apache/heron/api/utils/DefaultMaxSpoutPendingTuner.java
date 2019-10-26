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

package org.apache.heron.api.utils;

/**
 * This is a class that helps to auto tune the max spout pending value
 */
public class DefaultMaxSpoutPendingTuner {
  static final long CONSERVATIVE_MAX_SPOUT_PENDING = 10;
  static final int NOOP_THRESHOLD = 5;

  private Long adjustedMaxSpoutPending;
  private Long initialMaxSpoutPending;
  private Long prevProgress;
  private Long restoreAdjustedMaxSpoutPending;
  private Long restoreProgress;
  private int callsInNoop;
  private ACTION lastAction;
  private float autoTuneFactor;
  private double progressBound;
  private ACTION speculativeAction;

  /**
   * Conv constructor when initing from a non-set initial value
   */
  public DefaultMaxSpoutPendingTuner(float autoTuneFactor, double progressBound) {
    this(null, autoTuneFactor, progressBound);
  }

  public DefaultMaxSpoutPendingTuner(Long maxSpoutPending, float autoTuneFactor,
                                     double progressBound) {
    if (maxSpoutPending == null) {
      adjustedMaxSpoutPending = CONSERVATIVE_MAX_SPOUT_PENDING;
    } else {
      adjustedMaxSpoutPending = maxSpoutPending;
    }
    initialMaxSpoutPending = adjustedMaxSpoutPending;
    prevProgress = new Long(-1);
    restoreAdjustedMaxSpoutPending = adjustedMaxSpoutPending;
    restoreProgress = new Long(-1);
    lastAction = ACTION.NOOP;
    callsInNoop = 0;
    this.autoTuneFactor = autoTuneFactor;
    this.progressBound = progressBound;
    speculativeAction = ACTION.INCREASE;
  }

  public static boolean similarToNum(Long val, Long prevVal, double bound) {
    if (((double) (Math.abs(val - prevVal))) / prevVal < bound) {
      return true;
    }
    return false;
  }

  public static boolean moreThanNum(Long val, Long prevVal, double bound) {
    if (!similarToNum(val, prevVal, bound)) {
      return val > prevVal;
    }
    return false;
  }

  public static boolean lessThanNum(Long val, Long prevVal, double bound) {
    if (!similarToNum(val, prevVal, bound)) {
      return val < prevVal;
    }
    return false;
  }

  public Long get() {
    return adjustedMaxSpoutPending;
  }

  /**
   * Tune max default max spout pending based on progress
   */
  public void autoTune(Long progress) {
    // LOG.info ("Called auto tune with progress " + progress);

    if (lastAction == ACTION.NOOP) {
      // We did not take any action last time
      if (prevProgress == -1) {
        // The first time around when we are called
        doAction(ACTION.INCREASE, autoTuneFactor, progress);
      } else if (moreThanNum(progress, prevProgress, progressBound)) {
        // We have seen a sudden increase in progress in the steady state (NOOP) try to push
        // the max spout pending limit even more
        doAction(ACTION.INCREASE, autoTuneFactor, progress);
      } else if (lessThanNum(progress, prevProgress, progressBound)) {
        // We have seen a sudden drop in progress in the steady state try to decrease
        // the max spout pending proportionately to see if we can introduce an improvement
        // in progress;
        doAction(ACTION.DECREASE,
            Math.max((prevProgress - progress) / (float) prevProgress, autoTuneFactor), progress);
      } else {
        ++callsInNoop;
        // If the progress remains the same then once in a while try increasing the
        // max spout pending.
        if (callsInNoop >= NOOP_THRESHOLD) {
          doAction(speculativeAction, autoTuneFactor, progress);
          speculativeAction =
              speculativeAction == ACTION.INCREASE ? ACTION.DECREASE : ACTION.INCREASE;
        }
      }
    } else if (lastAction == ACTION.INCREASE) {
      if (moreThanNum(progress, prevProgress, autoTuneFactor - progressBound)) {
        // Our increase last time did result in a commisurate increase. Increase even more
        doAction(ACTION.INCREASE, autoTuneFactor, progress);
      } else if (lessThanNum(progress, prevProgress, progressBound)) {
        // Our increase last time actually resulted in a decrease.
        // Check how much we decreased. If our decrease was disproportionate
        // decrease accordingly.
        float drop = Math.max((prevProgress - progress) / (float) prevProgress, autoTuneFactor);
        if (drop > autoTuneFactor) {
          doAction(ACTION.DECREASE, drop, progress);
        } else {
          doAction(ACTION.RESTORE, autoTuneFactor, progress);
        }
      } else {
        // Our increase last time did not really change anything. So restore.
        doAction(ACTION.RESTORE, autoTuneFactor, progress);
      }
    } else if (lastAction == ACTION.DECREASE) {
      if (moreThanNum(progress, prevProgress, progressBound)) {
        // We actually saw an increase in progress. So decrease more
        doAction(ACTION.DECREASE, autoTuneFactor, progress);
      } else {
        // Hold off any decision. We will take any decision next time around.
        doAction(ACTION.NOOP, autoTuneFactor, progress);
      }
    } else if (lastAction == ACTION.RESTORE) {
      // Lets ignore the effects of the restore and wait out for the next cycle
      doAction(ACTION.NOOP, autoTuneFactor, progress);
    }
  }

  void doAction(ACTION action, float factor, Long currentProgress) {
    if (action == ACTION.INCREASE) {
      restoreAdjustedMaxSpoutPending = adjustedMaxSpoutPending;
      restoreProgress = prevProgress;
      adjustedMaxSpoutPending += Math.max(1,
          Math.round((Float) (factor * adjustedMaxSpoutPending)));
      callsInNoop = 0;
      prevProgress = currentProgress;
      // LOG.info("Increased max pending to " + adjustedMaxSpoutPending);
    } else if (action == ACTION.DECREASE) {
      restoreAdjustedMaxSpoutPending = adjustedMaxSpoutPending;
      restoreProgress = prevProgress;
      adjustedMaxSpoutPending -= Math.max(1,
          Math.round((Float) (factor * adjustedMaxSpoutPending)));

      // keep a min of CONSERVATIVE_MAX_SPOUT_PENDING or initialMaxSpoutPending tuple outstanding
      adjustedMaxSpoutPending = Math.max(
          Math.min(CONSERVATIVE_MAX_SPOUT_PENDING, initialMaxSpoutPending),
          adjustedMaxSpoutPending);
      callsInNoop = 0;
      prevProgress = currentProgress;
      // LOG.info("Decreased max pending to " + adjustedMaxSpoutPending);
    } else if (action == ACTION.RESTORE) {
      adjustedMaxSpoutPending = restoreAdjustedMaxSpoutPending;
      prevProgress = restoreProgress;
      callsInNoop = 0;
      // LOG.info("Restoring max pending to " + adjustedMaxSpoutPending);
    } else if (action == ACTION.NOOP) {
      prevProgress = currentProgress;
    }
    lastAction = action;
  }

  // public static Logger LOG = LoggerFactory.getLogger(DefaultMaxSpoutPendingTuner.class);
  enum ACTION {
    NOOP, INCREASE, DECREASE, RESTORE
  }
}
