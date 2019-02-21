package org.apache.heron.spouts.kafka;

import java.util.regex.Pattern;

public class DefaultTopicPatternProvider implements TopicPatternProvider {
    private static final long serialVersionUID = 5534026856505613199L;
    private String regex;

    @SuppressWarnings("WeakerAccess")
    public DefaultTopicPatternProvider(String regex) {
        this.regex = regex;
    }

    @Override
    public Pattern create() {
        if (regex == null) {
            throw new IllegalArgumentException("regex can not be null");
        }
        return Pattern.compile(regex);
    }
}
