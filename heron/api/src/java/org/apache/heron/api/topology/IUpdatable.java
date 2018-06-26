/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.heron.api.topology;

import org.apache.heron.classification.InterfaceAudience;
import org.apache.heron.classification.InterfaceStability;

/**
 * Bolt or spout instances should implement this method if they wish to be informed of changed to
 * the topology context during the lifecycle of the instance. The TopologyContext is backed by
 * elements of the physical plan, which can change during a scaling event. Instances that need to
 * adapt to such changes should implement this interface to receive a callback when a change occurs.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface IUpdatable {
  void update(TopologyContext topologyContext);
}
