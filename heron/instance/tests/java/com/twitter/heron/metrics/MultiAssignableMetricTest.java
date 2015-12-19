package com.twitter.heron.metrics;

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.twitter.heron.api.metric.MultiAssignableMetric;

/**
 * Test for MultiAssignableMetric
 */
public class MultiAssignableMetricTest {
  @Test
  public void testMultiAssignableMetrics() {
    MultiAssignableMetric multiAssignableMetric = new MultiAssignableMetric();
    multiAssignableMetric.scope("metric_a").setValue(100);
    multiAssignableMetric.scope("metric_b").setValue(200);

    Map ret = (Map) multiAssignableMetric.getValueAndReset();
    Assert.assertEquals(ret.get("metric_a"), 100);
    Assert.assertEquals(ret.get("metric_b"), 200);

    // Re-assign by synchronized safeScope
    multiAssignableMetric.safeScope("metric_a").setValue(300);
    multiAssignableMetric.safeScope("metric_b").setValue(400);

    ret = (Map) multiAssignableMetric.getValueAndReset();
    Assert.assertEquals(ret.get("metric_a"), 300);
    Assert.assertEquals(ret.get("metric_b"), 400);
  }


}
