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

package org.apache.heron.examples.api;

import org.apache.heron.common.basics.ByteAmount;

public final class ExampleResources {

  static final long DEFAULT_RAM_PADDING_PER_CONTAINER = 2 * 1024;
  static final long COMPONENT_RAM_MB = 1024;
  static final double DEFAULT_CPU_PADDING_PER_CONTAINER = 1.0;

  static ByteAmount getComponentRam() {
    return ByteAmount.fromMegabytes(COMPONENT_RAM_MB);
  }

  static ByteAmount getContainerDisk(int components, int containers) {
    return ByteAmount.fromGigabytes(Math.max(components / containers, 1));
  }

  static ByteAmount getContainerRam(int components, int containers) {
    final int componentsPerContainer = Math.max(components / containers, 1);
    return ByteAmount.fromMegabytes(COMPONENT_RAM_MB * componentsPerContainer
        + DEFAULT_RAM_PADDING_PER_CONTAINER);
  }

  static double getContainerCpu(int components, int containers) {
    final int componentsPerContainer = Math.max(components / containers, 1);
    return componentsPerContainer + DEFAULT_CPU_PADDING_PER_CONTAINER;
  }

  private ExampleResources() {
  }
}
