/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.heron.spouts.kafka;

import java.io.Serializable;
import java.util.regex.Pattern;

/**
 * convenient interface to provider a topic match pattern
 *
 * @see org.apache.kafka.clients.consumer.Consumer#subscribe(Pattern)
 */
public interface TopicPatternProvider extends Serializable {

  /**
   * create the topic matching regex pattern
   *
   * @return a matching pattern for topics to subscribe to
   */
  Pattern create();
}
