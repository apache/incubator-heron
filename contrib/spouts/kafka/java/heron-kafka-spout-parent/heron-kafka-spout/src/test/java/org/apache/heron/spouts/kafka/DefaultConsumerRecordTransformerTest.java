/*
 * Copyright 2019
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.heron.spouts.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DefaultConsumerRecordTransformerTest {
    private static final String DEFAULT_STREAM = "default";
    private ConsumerRecordTransformer<String, byte[]> consumerRecordTransformer;

    @BeforeEach
    void setUp() {
        consumerRecordTransformer = new DefaultConsumerRecordTransformer<>();
    }

    @Test
    void getOutputStreams() {
        assertEquals(Collections.singletonList(DEFAULT_STREAM), consumerRecordTransformer.getOutputStreams());
    }

    @Test
    void getFieldNames() {
        assertEquals(Arrays.asList("key", "value"), consumerRecordTransformer.getFieldNames(DEFAULT_STREAM));
    }

    @Test
    void transform() {
        ConsumerRecord<String, byte[]> consumerRecord = new ConsumerRecord<>("partition", 0, 0, "key", new byte[]{0x1, 0x2, 0x3});
        Map<String, List<Object>> expected = Collections.singletonMap(DEFAULT_STREAM, Arrays.asList(consumerRecord.key(), consumerRecord.value()));
        assertEquals(expected, consumerRecordTransformer.transform(consumerRecord));
    }
}