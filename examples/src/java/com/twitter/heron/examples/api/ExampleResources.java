//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package com.twitter.heron.examples.api;

import com.twitter.heron.common.basics.ByteAmount;

public final class ExampleResources {

  static final long COMPONENT_RAM_MB = 256;

  static ByteAmount getComponentRam() {
    return ByteAmount.fromMegabytes(COMPONENT_RAM_MB);
  }

  static ByteAmount getContainerDisk(int components, int containers) {
    return ByteAmount.fromGigabytes(Math.max(components / containers, 1));
  }

  static ByteAmount getContainerRam(int components, int containers) {
    final int componentsPerContainer = Math.max(components / containers, 1);
    return ByteAmount.fromMegabytes(COMPONENT_RAM_MB * componentsPerContainer);
  }

  private ExampleResources() {
  }
}
