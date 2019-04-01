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

import java.util.regex.Pattern;

/**
 * the built-in default pattern provider to create a topic pattern out of a regex string
 */
public class DefaultTopicPatternProvider implements TopicPatternProvider {
    private static final long serialVersionUID = 5534026856505613199L;
    private String regex;

    /**
     * create a provider out of a regular expression string
     *
     * @param regex topic name regular expression
     */
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
