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

package org.apache.heron.streamlet.impl.utils;

import org.apache.commons.lang3.StringUtils;

public final class StreamletUtils {

  private StreamletUtils() {
  }

  /**
   * Verifies the requirement as the utility function.
   * @param requirement The requirement to verify
   * @param errorMessage The error message
   * @throws IllegalArgumentException if the requirement fails
   */
  public static void require(Boolean requirement, String errorMessage) {
    if (!requirement) {
      throw new IllegalArgumentException(errorMessage);
    }
  }

  /**
   * Verifies not blank text as the utility function.
   * @param text The text to verify
   * @param errorMessage The error message
   * @throws IllegalArgumentException if the requirement fails
   */
  public static String checkNotBlank(String text, String errorMessage) {
    if (StringUtils.isBlank(text)) {
      throw new IllegalArgumentException(errorMessage);
    } else {
      return text;
    }
  }

  /**
   * Verifies not null reference as the utility function.
   * @param reference The reference to verify
   * @param errorMessage The error message
   * @throws NullPointerException if the requirement fails
   */
  public static <T> T checkNotNull(T reference, String errorMessage) {
    if (reference == null) {
      throw new NullPointerException(errorMessage);
    }
    return reference;
  }

}
