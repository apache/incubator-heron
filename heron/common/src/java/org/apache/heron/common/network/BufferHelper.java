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

package org.apache.heron.common.network;

import java.nio.Buffer;
import java.nio.ByteBuffer;

/**
 * Helper methods for working with {@link Buffer} objects.
 */
final class BufferHelper {

  private BufferHelper() {
  }

  /**
   * Flip the provided buffer.
   * <p>
   * This wrapper around {@link Buffer#flip()} is required because of
   * incompatible ABI changes between Java 8 and 11. In Java 8, {@link ByteBuffer#flip()} returns
   * a {@link Buffer}, whereas in Java 11, this method returns a {@link ByteBuffer}.
   * <p>
   * If this function is used, any object of {@link Buffer} (and subclasses) are first cast to
   * {@link Buffer} objects, then flipped, thus avoiding the binary incompatibility.
   *
   * @param buffer The buffer to flip
   */
  static void flip(Buffer buffer) {
    buffer.flip();
  }
}
