/*
 * Copyright 2016 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.heron.spouts.kafka;

import java.io.Serializable;

/** Wrapper class holding types KafkaConfig.ZkHosts. Kept for legacy reasons.
 * Do not modify. */
public class KafkaConfig implements Serializable {
  public interface BrokerHosts extends Serializable {
  }

  /**
   * Type wrapper for SpoutConfig.ZkHosts
   * @deprecated Use SpoutConfig.ZkHosts directly.
   */
  @Deprecated
  public static class ZkHosts extends SpoutConfig.ZkHosts {
    public ZkHosts(String brokerZkStr, String brokerZkPath) {
      super(brokerZkStr, brokerZkPath);
    }
  }
}
