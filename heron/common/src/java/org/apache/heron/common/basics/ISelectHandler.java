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

package org.apache.heron.common.basics;

import java.nio.channels.SelectableChannel;

/**
 * Implementing this interface allows an object to be the callback of SelectServer
 */

public interface ISelectHandler {
  /**
   * Handle a SelectableChannel when it is readable
   *
   * @param channel the channel ISelectHandler with handle with
   */
  void handleRead(SelectableChannel channel);

  /**
   * Handle a SelectableChannel when it is writable
   *
   * @param channel the channel ISelectHandler with handle with
   */
  void handleWrite(SelectableChannel channel);

  /**
   * Handle a SelectableChannel when it is acceptable
   *
   * @param channel the channel ISelectHandler with handle with
   */
  void handleAccept(SelectableChannel channel);

  /**
   * Handle a SelectableChannel when it is connectible
   *
   * @param channel the channel ISelectHandler with handle with
   */
  void handleConnect(SelectableChannel channel);

  /**
   * Handle a SelectableChannel when it meets some errors
   *
   * @param channel the channel ISelectHandler with handle with
   */
  void handleError(SelectableChannel channel);
}
