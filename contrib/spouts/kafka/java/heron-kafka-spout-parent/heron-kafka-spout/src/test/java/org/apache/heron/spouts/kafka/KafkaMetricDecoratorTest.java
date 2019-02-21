package org.apache.heron.spouts.kafka;

import org.apache.kafka.common.Metric;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaMetricDecoratorTest {
    @Mock
    private Metric metric;

    @BeforeEach
    void setUp() {
        when(metric.metricValue()).thenReturn("dummy value");
    }

    @Test
    void getValueAndReset() {
        assertEquals("dummy value", new KafkaMetricDecorator<>(metric).getValueAndReset());
    }
}