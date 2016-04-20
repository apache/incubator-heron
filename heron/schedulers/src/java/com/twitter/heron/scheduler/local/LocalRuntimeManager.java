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

package com.twitter.heron.scheduler.local;

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.scheduler.IRuntimeManager;

/**
 * Handles runtime tasks like kill/restart/activate/deactivate for
 * heron topology launched in the local scheduler.
 */
public class LocalRuntimeManager implements IRuntimeManager {

    private Config config;
    private Config runtime;

    @Override
    public void initialize(Config config, Config runtime) {
        this.config = config;
        this.runtime = runtime;
    }

    @Override
    public void close() {
    }

    @Override
    public boolean prepareRestart(Integer containerId) {
        return true;
    }

    @Override
    public boolean postRestart(Integer containerId) {
        return true;
    }

    @Override
    public boolean prepareDeactivate() {
        return true;
    }

    @Override
    public boolean postDeactivate() {
        return true;
    }

    /**
     * Check for preconditions for activate such as
     * If the topology is running?
     * If the topology is already in active state?
     *
     * @return true if the conditions are met
     */
    @Override
    public boolean prepareActivate() {
        return true;
    }

    @Override
    public boolean postActivate() {
        return true;
    }

    /**
     * Check for preconditions for kill such as if the topology running?
     *
     * @return true if the conditions are met
     */
    @Override
    public boolean prepareKill() {
        return true;
    }

    @Override
    public boolean postKill() {
        return true;
    }
}
