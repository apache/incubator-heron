// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.twitter.heron.healthmgr.common;

import com.microsoft.dhalion.resolver.Action;

public class HealthManagerEvents {

  /**
   * This event is created when a resolver executes topology update action
   */
  public static class TopologyUpdate extends Action {
    public TopologyUpdate() {
      super(TopologyUpdate.class.getSimpleName());
    }
  }

  /**
   * This event is created when a resolver executes restart container action
   */
  public static class ContainerRestart extends Action {
    public ContainerRestart() {
      super(ContainerRestart.class.getSimpleName());
    }
  }
}
