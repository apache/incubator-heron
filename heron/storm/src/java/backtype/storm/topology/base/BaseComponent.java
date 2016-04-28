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

package backtype.storm.topology.base;

import java.util.Map;

import backtype.storm.topology.IComponent;

public abstract class BaseComponent implements IComponent {
  private static final long serialVersionUID = 3451500233699261898L;

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
