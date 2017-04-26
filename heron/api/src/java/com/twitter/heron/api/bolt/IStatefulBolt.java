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

package com.twitter.heron.api.bolt;

import com.twitter.heron.api.topology.IStatefulComponent;

/**
 * An IStatefulBolt represents a bolt that implements stateful functionality.
 * In addition to the regular prepare/execute methods, it has initialization
 * methods with state as defined in IStatefulComponent. An implementation
 * of the stateful bolt saves state in the State interface and is able
 * to restore its state from the State during init call.
 */
public interface IStatefulBolt extends IStatefulComponent, IRichBolt {
}
