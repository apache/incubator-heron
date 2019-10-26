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

package org.apache.heron.api.bolt;

import java.time.Duration;

import org.junit.Test;

import org.apache.heron.api.topology.TopologyBuilder;
import org.apache.heron.api.windowing.TupleWindow;

import static org.junit.Assert.fail;

/**
 * Unit tests for {@link BaseWindowedBolt}
 */
public class BaseWindowedBoltTest {

  public static class TestBolt extends BaseWindowedBolt {

    private static final long serialVersionUID = -7224073487836212922L;

    @Override
    public void execute(TupleWindow inputWindow) {

    }
  }

  @Test
  public void testSettingSlidingCountWindow() {
    final Object[][] args = new Object[][]{
        {-1, 10},
        {10, -1},
        {0, 10},
        {10, 0},
        {0, 0},
        {-1, -1},
        {5, 10},
        {1, 1},
        {10, 5},
        {100, 10},
        {100, 100},
        {200, 100},
        {500, 100},
        {null, null},
        {null, 1},
        {1, null},
        {null, -1},
        {-1, null}
    };

    for (Object[] arg : args) {
      TopologyBuilder builder = new TopologyBuilder();
      Object arg0 = arg[0];
      Object arg1 = arg[1];
      try {

        BaseWindowedBolt.Count windowLengthCount = null;
        if (arg0 != null) {
          windowLengthCount = BaseWindowedBolt.Count.of((Integer) arg0);
        }
        BaseWindowedBolt.Count slidingIntervalCount = null;

        if (arg1 != null) {
          slidingIntervalCount = BaseWindowedBolt.Count.of((Integer) arg1);
        }

        builder.setBolt("testBolt", new TestBolt().withWindow(windowLengthCount,
            slidingIntervalCount));
        if (arg0 == null || arg1 == null) {
          fail(String.format("Window length or sliding window length cannot be null -- "
              + "windowLengthCount: %s slidingIntervalCount: %s", arg0, arg1));
        }
        if ((Integer) arg0 <= 0 || (Integer) arg1 <= 0) {
          fail(String.format("Window length or sliding window length cannot be zero or less -- "
              + "windowLengthCount: %s slidingIntervalCount: %s", arg0, arg1));
        }
      } catch (IllegalArgumentException e) {
        if (arg0 != null && arg1 != null && (Integer) arg0 > 0 && (Integer) arg1 > 0) {
          fail(String.format("Exception: %s thrown on valid input -- windowLengthCount: %s "
              + "slidingIntervalCount: %s", e.getMessage(), arg0, arg1));
        }
      }
    }
  }

  @Test
  public void testSettingSlidingTimeWindow() {
    final Object[][] args = new Object[][]{
        {-1L, 10L},
        {10L, -1L},
        {0L, 10L},
        {10L, 0L},
        {0L, 0L},
        {-1L, -1L},
        {5L, 10L},
        {1L, 1L},
        {10L, 5L},
        {100L, 10L},
        {100L, 100L},
        {200L, 100L},
        {500L, 100L},
        {null, null},
        {null, 1L},
        {1L, null},
        {null, -1L},
        {-1L, null}
    };

    for (Object[] arg : args) {
      TopologyBuilder builder = new TopologyBuilder();
      Object arg0 = arg[0];
      Object arg1 = arg[1];
      try {

        Duration windowLengthDuration = null;
        if (arg0 != null) {
          windowLengthDuration = Duration.ofMillis((Long) arg0);
        }
        Duration slidingIntervalDuration = null;

        if (arg1 != null) {
          slidingIntervalDuration = Duration.ofMillis((Long) arg1);
        }

        builder.setBolt("testBolt", new TestBolt().withWindow(windowLengthDuration,
            slidingIntervalDuration));
        if (arg0 == null || arg1 == null) {
          fail(String.format("Window length or sliding window length cannot be null -- "
              + "windowLengthDuration: %s slidingIntervalDuration: %s", arg0, arg1));
        }
        if ((Long) arg0 <= 0 || (Long) arg1 <= 0) {
          fail(String.format("Window length or sliding window length cannot be zero or less -- "
              + "windowLengthDuration: %s slidingIntervalDuration: %s", arg0, arg1));
        }
      } catch (IllegalArgumentException e) {
        if (arg0 != null && arg1 != null && (Long) arg0 > 0 && (Long) arg1 > 0) {
          fail(String.format("Exception: %s thrown on valid input -- windowLengthDuration: %s "
              + "slidingIntervalDuration: %s", e.getMessage(), arg0, arg1));
        }
      }
    }
  }

  @Test
  public void testSettingTumblingCountWindow() {
    final Object[] args = new Object[] {-1, 0, 1, 2, 5, 10, null};

    for (Object arg : args) {
      TopologyBuilder builder = new TopologyBuilder();
      Object arg0 = arg;
      try {

        BaseWindowedBolt.Count windowLengthCount = null;
        if (arg0 != null) {
          windowLengthCount = BaseWindowedBolt.Count.of((Integer) arg0);
        }

        builder.setBolt("testBolt", new TestBolt().withTumblingWindow(windowLengthCount));
        if (arg0 == null) {
          fail(String.format("Window length cannot be null -- windowLengthCount: %s", arg0));
        }
        if ((Integer) arg0 <= 0) {
          fail(String.format("Window length cannot be zero or less -- windowLengthCount: %s",
              arg0));
        }
      } catch (IllegalArgumentException e) {
        if (arg0 != null && (Integer) arg0 > 0) {
          fail(String.format("Exception: %s thrown on valid input -- windowLengthCount: %s", e
              .getMessage(), arg0));
        }
      }
    }
  }

  @Test
  public void testSettingTumblingTimeWindow() {
    final Object[] args = new Object[]{-1L, 0L, 1L, 2L, 5L, 10L, null};
    for (Object arg : args) {
      TopologyBuilder builder = new TopologyBuilder();
      Object arg0 = arg;
      try {

        Duration windowLengthDuration = null;
        if (arg0 != null) {
          windowLengthDuration = Duration.ofMillis((Long) arg0);
        }

        builder.setBolt("testBolt", new TestBolt().withTumblingWindow(windowLengthDuration));
        if (arg0 == null) {
          fail(String.format("Window count duration cannot be null -- windowLengthDuration: %s",
              arg0));
        }
        if ((Long) arg0 <= 0) {
          fail(String.format("Window length cannot be zero or less -- windowLengthDuration: %s",
              arg0));
        }
      } catch (IllegalArgumentException e) {
        if (arg0 != null && (Long) arg0 > 0) {
          fail(String.format("Exception: %s thrown on valid input -- windowLengthDuration: %s", e
              .getMessage(), arg0));
        }
      }
    }
  }

  @Test
  public void testSettingLagTime() {
    final Object[] args = new Object[]{-1L, 0L, 1L, 2L, 5L, 10L, null};
    for (Object arg : args) {
      TopologyBuilder builder = new TopologyBuilder();
      Object arg0 = arg;
      try {

        Duration lagTime = null;
        if (arg0 != null) {
          lagTime = Duration.ofMillis((Long) arg0);
        }

        builder.setBolt("testBolt", new TestBolt().withLag(lagTime));
        if (arg0 == null) {
          fail(String.format("Window lag duration cannot be null -- lagTime: %s", arg0));
        }
        if ((Long) arg0 <= 0) {
          fail(String.format("Window lag cannot be zero or less -- lagTime: %s", arg0));
        }
      } catch (IllegalArgumentException e) {
        if (arg0 != null && (Long) arg0 > 0) {
          fail(String.format("Exception: %s thrown on valid input -- lagTime: %s",
              e.getMessage(), arg0));
        }
      }
    }
  }

  @Test
  public void testSettingWaterMarkInterval() {
    final Object[] args = new Object[]{-1L, 0L, 1L, 2L, 5L, 10L, null};
    for (Object arg : args) {
      TopologyBuilder builder = new TopologyBuilder();
      Object arg0 = arg;
      try {

        Duration watermarkInterval = null;
        if (arg0 != null) {
          watermarkInterval = Duration.ofMillis((Long) arg0);
        }

        builder.setBolt("testBolt", new TestBolt().withWatermarkInterval(watermarkInterval));
        if (arg0 == null) {
          fail(String.format("Watermark interval cannot be null -- watermarkInterval: %s", arg0));
        }
        if ((Long) arg0 <= 0) {
          fail(String.format("Watermark interval cannot be zero or less -- watermarkInterval: "
              + "%s", arg0));
        }
      } catch (IllegalArgumentException e) {
        if (arg0 != null && (Long) arg0 > 0) {
          fail(String.format("Exception: %s thrown on valid input -- watermarkInterval: %s", e
              .getMessage(), arg0));
        }
      }
    }
  }
}
