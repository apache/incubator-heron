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
package org.apache.dhalion.heron.common;

import java.util.Collection;

import org.apache.dhalion.symptom.InstanceInfo;

/**
 * An {@link HeronComponentInfo} holds information pertinent to a Heron topology component. For
 * e.g. has aggregate count of tuples emitted by all the spout intances, {@link InstanceInfo}.
 */
public class HeronComponentInfo {
  private Collection<InstanceInfo> instances;

  public HeronComponentInfo(Collection<InstanceInfo> instances) {
    this.instances = instances;
  }
}
