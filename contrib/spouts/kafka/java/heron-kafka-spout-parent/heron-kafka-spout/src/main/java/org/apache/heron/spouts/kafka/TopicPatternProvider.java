package org.apache.heron.spouts.kafka;

import java.io.Serializable;
import java.util.regex.Pattern;

interface TopicPatternProvider extends Serializable {
    Pattern create();
}
