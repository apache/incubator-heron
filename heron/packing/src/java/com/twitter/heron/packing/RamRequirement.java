// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.twitter.heron.packing;

import com.twitter.heron.common.basics.ByteAmount;

/**
 * Helper class that captures the RAM requirements of each component
 */
public class RamRequirement implements Comparable<RamRequirement> {

  private String componentName;
  private ByteAmount ramRequirement;

  public RamRequirement(String componentName, ByteAmount ram) {
    this.componentName = componentName;
    this.ramRequirement = ram;
  }

  public String getComponentName() {
    return componentName;
  }

  @Override
  public int compareTo(RamRequirement other) {
    return this.ramRequirement.compareTo(other.ramRequirement);
  }

  @Override
  public boolean equals(Object o) {

    if (o == this) {
      return true;
    }
    if (!(o instanceof RamRequirement)) {
      return false;
    }
    RamRequirement c = (RamRequirement) o;

    // Compare the ramRequirement values and return accordingly
    return ramRequirement.equals(c.ramRequirement);
  }

  @Override
  public int hashCode() {
    return ramRequirement.hashCode();
  }
}
