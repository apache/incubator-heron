package org.apache.heron.spouts.kafka;

import org.junit.jupiter.api.Test;

import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DefaultTopicPatternProviderTest {

    @Test
    void create() {
        assertEquals(Pattern.compile("a").pattern(), new DefaultTopicPatternProvider("a").create().pattern());
    }
}