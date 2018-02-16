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

package com.twitter.heron.healthmgr.detectors;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;

import com.microsoft.dhalion.core.Measurement;
import com.microsoft.dhalion.core.Symptom;

import org.junit.Test;

import com.twitter.heron.healthmgr.HealthPolicyConfig;

import static com.twitter.heron.healthmgr.detectors.WaitQueueSkewDetector.CONF_SKEW_RATIO;
import static com.twitter.heron.healthmgr.sensors.BaseSensor.MetricName.METRIC_WAIT_Q_SIZE;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WaitQueueSkewDetectorTest {
  @Test
  public void testConfigAndFilter() {
    HealthPolicyConfig config = mock(HealthPolicyConfig.class);
    when(config.getConfig(CONF_SKEW_RATIO, 20.0)).thenReturn(15.0);

    Measurement measurement1
        = new Measurement("bolt", "i1", METRIC_WAIT_Q_SIZE.text(), Instant.ofEpochSecond
        (1497892222), 1501);
    Measurement measurement2
        = new Measurement("bolt", "i2", METRIC_WAIT_Q_SIZE.text(), Instant.ofEpochSecond
        (1497892222), 100.0);

    Collection<Measurement> metrics = new ArrayList<>();
    metrics.add(measurement1);
    metrics.add(measurement2);
    WaitQueueSkewDetector detector = new WaitQueueSkewDetector(config);
    Collection<Symptom> symptoms = detector.detect(metrics);

    assertEquals(1, symptoms.size());

     measurement1
        = new Measurement("bolt", "i1", METRIC_WAIT_Q_SIZE.text(), Instant.ofEpochSecond
        (1497892222), 1500);
     measurement2
        = new Measurement("bolt", "i2", METRIC_WAIT_Q_SIZE.text(), Instant.ofEpochSecond
        (1497892222), 110.0);

    metrics = new ArrayList<>();
    metrics.add(measurement1);
    metrics.add(measurement2);
    detector = new WaitQueueSkewDetector(config);
    symptoms = detector.detect(metrics);

    assertEquals(0, symptoms.size());
  }
}
